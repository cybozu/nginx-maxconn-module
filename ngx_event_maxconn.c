/* (C) 2015 Cybozu.  All rights reserved. */

#include <assert.h>
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_event.h>
#include <ngx_event_connect.h>
#include <ngx_event_maxconn.h>


#define NGX_EVENT_MAXCONN_BUCKET_SIZE 257


typedef struct ngx_event_maxconn_ctx_s ngx_event_maxconn_ctx_t;
typedef struct ngx_event_maxconn_cache_s ngx_event_maxconn_cache_t;


struct ngx_event_maxconn_cache_s {
    ngx_peer_connection_t   peer;
    ngx_event_maxconn_t    *maxconn;

    ngx_queue_t             queue;
};


struct ngx_event_maxconn_s {
    ngx_addr_t             *addrs;
    ngx_str_t               host;
    in_port_t               port;

    ngx_msec_t              timeout;

    size_t                  max_cached;
    ngx_queue_t             cached_connections;
    ngx_queue_t             free_connections;

    /* Mapping from ngx_connection_t* to ngx_event_maxconn_ctx_t* */
    ngx_queue_t             contexts[NGX_EVENT_MAXCONN_BUCKET_SIZE];
};


typedef enum {
    NGX_EVENT_MAXCONN_STATE_INITIAL,
    NGX_EVENT_MAXCONN_STATE_ACQUIRING,
    NGX_EVENT_MAXCONN_STATE_RELEASING,
} ngx_event_maxconn_state_t;


struct ngx_event_maxconn_ctx_s {
    ngx_peer_connection_t           peer;
    ngx_connection_t               *client;
    size_t                          n_acquired;
    ngx_str_t                       key;
    ngx_event_maxconn_state_t       state;

    ngx_event_maxconn_callback_t    callback;
    ngx_buf_t                      *request;
    ngx_buf_t                      *response;

    ngx_event_maxconn_t            *maxconn;
    ngx_pool_t                     *pool;
    ngx_log_t                      *log;

    ngx_queue_t                     queue;
};


typedef enum {
    NGX_EVENT_MAXCONN_UNKNOWN,
    NGX_EVENT_MAXCONN_ACQUIRE = 0x02,
    NGX_EVENT_MAXCONN_RELEASE = 0x03,
} ngx_event_maxconn_operation_t;


static u_char *
ngx_event_maxconn_log_error(ngx_log_t *log, u_char *buf, size_t len);
static void
ngx_event_maxconn_release_internal(ngx_event_maxconn_t *maxconn,
                                   ngx_event_maxconn_ctx_t *ctx,
                                   ngx_connection_t *client);
static ngx_event_maxconn_ctx_t *
ngx_event_maxconn_get_context(ngx_event_maxconn_t *maxconn,
                              ngx_connection_t *client);
static ngx_event_maxconn_ctx_t *
ngx_event_maxconn_create_context(void);
static ngx_buf_t *
ngx_event_maxconn_create_request(ngx_event_maxconn_ctx_t *ctx,
                                 ngx_event_maxconn_operation_t op,
                                 ngx_int_t limit,
                                 ngx_str_t key);
static ngx_int_t
ngx_event_maxconn_get_peer_connection(ngx_event_maxconn_t *maxconn,
                                      ngx_event_maxconn_ctx_t *ctx,
                                      ngx_peer_connection_t *pc);
static void
ngx_event_maxconn_setup_connection(ngx_connection_t* c,
                                   ngx_event_maxconn_ctx_t *ctx);
static ngx_int_t
ngx_event_maxconn_connect(ngx_event_maxconn_ctx_t *ctx);
static void
ngx_event_maxconn_write_handler(ngx_event_t *wev);
static void
ngx_event_maxconn_read_handler(ngx_event_t *rev);
static ngx_int_t
ngx_event_maxconn_process_response(ngx_event_maxconn_ctx_t *ctx,
                                   /* OUT */ ngx_int_t *status);
static void
ngx_event_maxconn_keep_connection(ngx_event_maxconn_ctx_t *ctx, int return_to_cache);
static void
ngx_event_maxconn_dummy_handler(ngx_event_t *ev);
static int
ngx_event_maxconn_is_closed(ngx_connection_t *c);
static void
ngx_event_maxconn_close_check_handler_for_cache(ngx_event_t *ev);
static void
ngx_event_maxconn_close_check_handler_for_ctx(ngx_event_t *ev);
static void
ngx_event_maxconn_error(ngx_event_maxconn_ctx_t *ctx);
static void
ngx_event_maxconn_destroy_context(ngx_event_maxconn_ctx_t *ctx);
static void
ngx_event_maxconn_close_connection(ngx_connection_t *c);


static size_t
ngx_event_maxconn_get_hash_index(ngx_connection_t *c)
{
    return (uintptr_t)c % NGX_EVENT_MAXCONN_BUCKET_SIZE;
}


ngx_event_maxconn_t *
ngx_event_maxconn_init(ngx_conf_t *cf, ngx_str_t server_name, size_t max_cached)
{
    ngx_event_maxconn_t        *maxconn;
    ngx_event_maxconn_cache_t  *cached;
    ngx_url_t                   url;
    size_t                      i;

    ngx_log_debug2(NGX_LOG_DEBUG_EVENT, cf->log, 0,
                   "ngx_event_maxconn_init: server_name=%V, max_cached=%d",
                   &server_name, (int)max_cached);

    maxconn = ngx_pcalloc(cf->pool, sizeof(ngx_event_maxconn_t));
    if (maxconn == NULL)
        return NULL;

    ngx_memzero(&url, sizeof(ngx_url_t));

    url.url = server_name;
    url.default_port = 11215;
    url.uri_part = 1;

    if (ngx_parse_url(cf->pool, &url) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, cf->log, 0,
            "[%s] failed to parse server name: %V",
            url.err, &url.url);
        return NULL;
    }

    maxconn->addrs = url.addrs;
    maxconn->host = url.host;
    maxconn->port = url.port;

    maxconn->timeout = 1000;

    maxconn->max_cached = max_cached;

    cached = ngx_pcalloc(cf->pool, sizeof(ngx_event_maxconn_cache_t) * max_cached);
    if (cached == NULL)
        return NULL;

    ngx_queue_init(&maxconn->cached_connections);
    ngx_queue_init(&maxconn->free_connections);

    for (i = 0; i < max_cached; i++) {
        ngx_queue_insert_head(&maxconn->free_connections, &cached[i].queue);
    }

    for (i = 0; i < NGX_EVENT_MAXCONN_BUCKET_SIZE; ++i) {
        ngx_queue_init(&maxconn->contexts[i]);
    }

    return maxconn;
}


static u_char *
ngx_event_maxconn_log_error(ngx_log_t *log, u_char *buf, size_t len)
{
    u_char *p;
    ngx_event_maxconn_ctx_t *ctx;

    p = buf;

    if (log->action) {
        p = ngx_snprintf(buf, len, " while %s", log->action);
        len -= p - buf;
    }

    ctx = log->data;

    if (ctx) {
        p = ngx_snprintf(p, len, ", maxconn: %V", &ctx->key);
    }

    return p;
}


ngx_int_t
ngx_event_maxconn_acquire(ngx_event_maxconn_t *maxconn, ngx_int_t limit, ngx_str_t key,
                          ngx_connection_t *client, ngx_event_maxconn_callback_t callback)
{
    ngx_int_t                   rc;
    ngx_event_maxconn_ctx_t    *ctx;
    size_t                      hash;

    ctx = ngx_event_maxconn_get_context(maxconn, client);
    if (ctx != NULL) {
        ngx_log_error(NGX_LOG_ERR, client->log, 0,
                      "acquiring resources multiple times are not permitted");
        return NGX_ERROR;
    }

    ctx = ngx_event_maxconn_create_context();
    if (ctx == NULL) {
        ngx_log_error(NGX_LOG_ERR, client->log, 0,
                      "failed to create maxconn context");
        return NGX_ERROR;
    }

    ctx->client = client;
    ctx->maxconn = maxconn;
    ctx->callback = callback;
    ctx->n_acquired = 0;
    ctx->key.len = key.len;
    ctx->key.data = ngx_palloc(ctx->pool, key.len);
    ngx_memcpy(ctx->key.data, key.data, key.len);

    hash = ngx_event_maxconn_get_hash_index(client);
    ngx_queue_insert_head(&maxconn->contexts[hash], &ctx->queue);

    ctx->request = ngx_event_maxconn_create_request(
            ctx, NGX_EVENT_MAXCONN_ACQUIRE, limit, key);
    if (ctx->request == NULL)
        goto error;

    rc = ngx_event_maxconn_get_peer_connection(maxconn, ctx, &ctx->peer);
    if (rc != NGX_DECLINED && rc != NGX_DONE)
        goto error;

    ctx->state = NGX_EVENT_MAXCONN_STATE_ACQUIRING;

    if (rc == NGX_DECLINED) {
        /* NGX_DECLINED means that we need connect to backend server */
        rc = ngx_event_maxconn_connect(ctx);
        if (rc != NGX_OK)
            goto error;
        /* Do not touch ctx after ngx_event_maxconn_connect returns NGX_OK
         * because ctx may be already destructed. */
        return NGX_OK;
    }

    /* NGX_DONE means that we have available connection.
     * Therefore, we go ahead to sending a request. */
    ngx_event_maxconn_write_handler(ctx->peer.connection->write);
    /* Do not touch ctx after ngx_event_write_handler
     * because ctx may be already destructed. */
    return NGX_OK;

error:
    ngx_event_maxconn_destroy_context(ctx);
    return NGX_ERROR;
}


void
ngx_event_maxconn_release(ngx_event_maxconn_t *maxconn,
                          ngx_connection_t *client)
{
    ngx_event_maxconn_ctx_t *ctx;

    ctx = ngx_event_maxconn_get_context(maxconn, client);
    if (ctx == NULL) {
        ngx_log_debug1(NGX_LOG_DEBUG_EVENT, ngx_cycle->log, 0,
                       "ngx_event_maxconn_release: no ctx found: client_fd=%d",
                       client->fd);
        /* No context found. Nothing to do. */
        return;
    }

    ngx_event_maxconn_release_internal(maxconn, ctx, client);
}


static void
ngx_event_maxconn_release_internal(ngx_event_maxconn_t *maxconn,
                                   ngx_event_maxconn_ctx_t *ctx,
                                   ngx_connection_t *client)
{
    ngx_log_debug3(NGX_LOG_DEBUG_EVENT, ngx_cycle->log, 0,
                   "ngx_event_maxconn_release_internal: client_fd=%d, peer_fd=%d, ctx=%p",
                   client->fd, ctx->peer.connection->fd, ctx);

    ctx->callback.handler = NULL;
    ctx->callback.data = NULL;
    ctx->response = NULL;
    ctx->request = ngx_event_maxconn_create_request(
            ctx, NGX_EVENT_MAXCONN_RELEASE, 0, ctx->key);
    if (ctx->request == NULL) {
        ngx_event_maxconn_error(ctx);
        return;
    }

    ctx->state = NGX_EVENT_MAXCONN_STATE_RELEASING;
    ngx_event_maxconn_setup_connection(ctx->peer.connection, ctx);
    ngx_event_maxconn_write_handler(ctx->peer.connection->write);
    /* Do not touch ctx after ngx_event_write_handler
     * because ctx may be already destructed. */
}


static ngx_event_maxconn_ctx_t *
ngx_event_maxconn_get_context(ngx_event_maxconn_t *maxconn,
                              ngx_connection_t *client)
{
    ngx_queue_t               *q;
    ngx_event_maxconn_ctx_t   *ctx;
    size_t                     hash;

    hash = ngx_event_maxconn_get_hash_index(client);
    for (q = ngx_queue_head(&maxconn->contexts[hash]);
         q != ngx_queue_sentinel(&maxconn->contexts[hash]);
         q = ngx_queue_next(q))
    {
        ctx = ngx_queue_data(q, ngx_event_maxconn_ctx_t, queue);
        if (ctx->client == client)
            return ctx;
    }

    return NULL;
}


static ngx_event_maxconn_ctx_t *
ngx_event_maxconn_create_context(void)
{
    ngx_log_t                  *log;
    ngx_pool_t                 *pool;
    ngx_event_maxconn_ctx_t    *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_EVENT, ngx_cycle->log, 0,
                   "ngx_event_maxconn_create_context");

    pool = ngx_create_pool(512, ngx_cycle->log);
    if (pool == NULL)
        return NULL;

    ctx = ngx_pcalloc(pool, sizeof(ngx_event_maxconn_ctx_t));
    if (ctx == NULL) {
        ngx_destroy_pool(pool);
        return NULL;
    }

    log = ngx_pcalloc(pool, sizeof(ngx_log_t));
    if (log == NULL) {
        ngx_destroy_pool(pool);
        return NULL;
    }

    ctx->pool = pool;
    *log = *ctx->pool->log;
    ctx->pool->log = log;
    ctx->log = log;

    log->handler = ngx_event_maxconn_log_error;
    log->data = ctx;
    log->action = "processing maxconn";

    return ctx;
}


static ngx_buf_t *
ngx_event_maxconn_create_request(ngx_event_maxconn_ctx_t *ctx,
                                 ngx_event_maxconn_operation_t op,
                                 ngx_int_t limit,
                                 ngx_str_t key)
{
    static const size_t HEADER_SIZE = 12;
    size_t          body_len;
    ngx_buf_t      *buf;
    uint32_t        n;
    uint16_t        m;

    ngx_log_debug3(NGX_LOG_DEBUG_EVENT, ctx->log, 0,
                   "ngx_event_maxconn_create_request: op=%d, limit=%d, key=\"%V\"",
                   (int)op, (int)limit, &key);

    /* Length calculation */
    if (op == NGX_EVENT_MAXCONN_ACQUIRE) {
        body_len = 10 + key.len;
    } else if (op == NGX_EVENT_MAXCONN_RELEASE) {
        body_len = 6 + key.len;
    } else {
        ngx_log_error(NGX_LOG_ERR, ctx->log, 0, "bug?");
        return NULL;
    }

    /* Create request */
    buf = ngx_create_temp_buf(ctx->pool, HEADER_SIZE + body_len);
    if (buf == NULL)
        return NULL;

    *buf->last++ = 0x90;  /* Magic */
    *buf->last++ = op;    /* Opcode */
    *buf->last++ = 0;     /* Flags */
    *buf->last++ = 0;     /* Reserved */

    /* Body length */
    n = htonl((uint32_t)body_len);
    memcpy(buf->last, &n, sizeof(n));
    buf->last += sizeof(n);

    /* Opaque */
    n = 0;
    memcpy(buf->last, &n, sizeof(n));
    buf->last += sizeof(n);

    /* Resources */
    n = htonl((uint32_t)1);
    memcpy(buf->last, &n, sizeof(n));
    buf->last += sizeof(n);

    if (op == NGX_EVENT_MAXCONN_ACQUIRE) {
        /* Maximum */
        n = htonl((uint32_t)limit);
        memcpy(buf->last, &n, sizeof(n));
        buf->last += sizeof(n);
    }

    /* Name length */
    m = htons((uint16_t)key.len);
    memcpy(buf->last, &m, sizeof(m));
    buf->last += sizeof(m);

    /* Name data */
    memcpy(buf->last, key.data, key.len);
    buf->last += key.len;

    return buf;
}


static ngx_int_t
ngx_event_maxconn_get_peer_connection(ngx_event_maxconn_t *maxconn,
                                      ngx_event_maxconn_ctx_t *ctx,
                                      ngx_peer_connection_t *pc)
{
    ngx_queue_t                   *q;
    ngx_event_maxconn_cache_t     *item;

    for (q = ngx_queue_head(&maxconn->cached_connections);
         q != ngx_queue_sentinel(&maxconn->cached_connections);
         q = ngx_queue_next(q))
    {
        item = ngx_queue_data(q, ngx_event_maxconn_cache_t, queue);
        *pc = item->peer;

        ngx_log_debug2(NGX_LOG_DEBUG_EVENT, ctx->log, 0,
                       "ngx_event_maxconn_get_peer_connection: "
                       "using connection %p for ctx %p",
                       pc->connection, ctx);

        /* remove item from cached_connections */
        ngx_queue_remove(q);

        /* add memory to free_connections */
        ngx_memzero(item, sizeof(ngx_event_maxconn_cache_t));
        ngx_queue_insert_head(&maxconn->free_connections, q);

        ngx_event_maxconn_setup_connection(pc->connection, ctx);
        return NGX_DONE;
    }

    /* No available connection in cache */

    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, ctx->log, 0,
                   "ngx_event_maxconn_get_peer_connection: "
                   "no cached connections for ctx %p",
                   ctx);
    pc->connection = NULL;
    return NGX_DECLINED;
}


static void
ngx_event_maxconn_setup_connection(ngx_connection_t* c,
                                   ngx_event_maxconn_ctx_t *ctx)
{
    c->data = ctx;
    c->read->handler = ngx_event_maxconn_read_handler;
    c->write->handler = ngx_event_maxconn_write_handler;

    ngx_add_timer(c->read, ctx->maxconn->timeout);
    ngx_add_timer(c->write, ctx->maxconn->timeout);
    c->idle = 0;

    c->log = ctx->log;
    c->pool->log = ctx->log;
    c->read->log = ctx->log;
    c->write->log = ctx->log;
}


static ngx_int_t
ngx_event_maxconn_connect(ngx_event_maxconn_ctx_t *ctx)
{
    ngx_event_maxconn_t    *maxconn;
    ngx_pool_t             *pool;
    ngx_int_t               rc;

    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, ctx->log, 0,
                   "ngx_event_maxconn_connect: ctx=%p",
                   ctx);

    maxconn = ctx->maxconn;

    /* create new pool for the new connection */
    pool = ngx_create_pool(512, ngx_cycle->log);
    if (pool == NULL)
        return NGX_ERROR;

    /* set peer information */
    ngx_memzero(&ctx->peer, sizeof(ctx->peer));
    ctx->peer.sockaddr = maxconn->addrs[0].sockaddr;
    ctx->peer.socklen = maxconn->addrs[0].socklen;
    ctx->peer.name = &maxconn->addrs[0].name;
    ctx->peer.get = ngx_event_get_peer;
    ctx->peer.log = ctx->log;
    ctx->peer.log_error = NGX_ERROR_ERR;

    /* connect */
    rc = ngx_event_connect_peer(&ctx->peer);
    if (rc == NGX_ERROR || rc == NGX_BUSY || rc == NGX_DECLINED) {
        ngx_destroy_pool(pool);
        return NGX_ERROR;
    }

    /* set data and handlers */
    ctx->peer.connection->pool = pool;
    ngx_event_maxconn_setup_connection(ctx->peer.connection, ctx);

    if (rc != NGX_OK) {
        /* rc must be NGX_AGAIN here */
        return NGX_OK;
    }

    ngx_event_maxconn_write_handler(ctx->peer.connection->write);
    return NGX_OK;
}


static void
ngx_event_maxconn_write_handler(ngx_event_t *wev)
{
    ssize_t                     n, size;
    ngx_connection_t           *c;
    ngx_event_maxconn_ctx_t    *ctx;

    c = wev->data;
    ctx = c->data;

    ngx_log_debug0(NGX_LOG_DEBUG_EVENT, wev->log, 0,
                  "ngx_event_maxconn_write_handler");

    if (wev->timedout) {
        ngx_log_error(NGX_LOG_ERR, wev->log, NGX_ETIMEDOUT,
                      "yrmcds server timed out");
        ngx_event_maxconn_error(ctx);
        return;
    }

    size = ctx->request->last - ctx->request->pos;

    n = ngx_send(c, ctx->request->pos, size);

    if (n == NGX_ERROR) {
        ngx_event_maxconn_error(ctx);
        return;
    }

    if (n > 0) {
        ctx->request->pos += n;

        if (n == size) {
            wev->handler = ngx_event_maxconn_dummy_handler;

            if (wev->timer_set)
                ngx_del_timer(wev);

            if (ngx_handle_write_event(wev, 0) != NGX_OK) {
                ngx_event_maxconn_error(ctx);
                return;
            }

            return;
        }
    }

    if (!wev->timer_set)
        ngx_add_timer(wev, ctx->maxconn->timeout);
}


static void
ngx_event_maxconn_read_handler(ngx_event_t *rev)
{
    ssize_t                     n, size;
    ngx_event_maxconn_ctx_t    *ctx;
    ngx_connection_t           *c;

    c = rev->data;
    ctx = c->data;

    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, rev->log, 0,
                   "ngx_event_maxconn_read_handler: peer_fd=%d",
                   c->fd);

    if (rev->timedout) {
        ngx_log_error(NGX_LOG_ERR, rev->log, NGX_ETIMEDOUT,
                "ngx_event_maxconn_read_handler timed out");
        ngx_event_maxconn_error(ctx);
        return;
    }

    if (ctx->response == NULL) {
        ctx->response = ngx_create_temp_buf(ctx->pool, 64);
        if (ctx->response == NULL) {
            ngx_event_maxconn_error(ctx);
            return;
        }
    }

    for (;;) {
        size = ctx->response->end - ctx->response->last;
        n = ngx_recv(c, ctx->response->last, size);

        if (n == 0) {
            /* connection close or buffer full.
             * yrmcds does not close connection in this timing, and buffer-full
             * does not occurr because the size of a response of yrmcds is
             * constant.
             * Threfore, we treat n == 0 as an error.
             */
            break;
        }
        if (n > 0) {
            ngx_int_t                     rc, status;
            ngx_event_maxconn_callback_t  callback;

            /* Process the byte seqeunce received. */
            ctx->response->last += n;
            rc = ngx_event_maxconn_process_response(ctx, &status);
            if (rc == NGX_AGAIN) {
                /* If rc is NGX_AGAIN, then there are bytes yet to be received. */
                continue;
            }
            if (rc == NGX_ERROR) {
                /* If rc is NGX_ERROR, then close this connection because it is
                 * protocol error. */
                ngx_event_maxconn_error(ctx);
                return;
            }
            /* rc == NGX_OK */
            if (ctx->state == NGX_EVENT_MAXCONN_STATE_ACQUIRING) {
                /* If rc is NGX_OK and the operation is Acquire, then keep this
                 * connection and call the callback function. */
                ctx->state = NGX_EVENT_MAXCONN_STATE_INITIAL;
                ngx_event_maxconn_keep_connection(ctx, 0);
                callback = ctx->callback;
                ctx->callback.handler = NULL;
                ctx->callback.data = NULL;
                callback.handler(status, callback.data);
                return;
            }
            if (ctx->state == NGX_EVENT_MAXCONN_STATE_RELEASING) {
                /* If rc is NGX_OK and the operation is Release, then return this
                 * connection to cache and destroy the context. */
                ctx->state = NGX_EVENT_MAXCONN_STATE_INITIAL;
                ngx_event_maxconn_keep_connection(ctx, 1);
                ngx_event_maxconn_destroy_context(ctx);
                return;
            }
            assert(0 && "BUG!!");
        }
        if (n == NGX_AGAIN) {
            if (ngx_handle_read_event(rev, 0) != NGX_OK) {
                ngx_event_maxconn_error(ctx);
                return;
            }
            return;
        }
        break;
    }

    // An error ocurred before the response is processed.
    ngx_log_error(NGX_LOG_ERR, ctx->log, 0, "peer prematurely closed connection");
    ngx_event_maxconn_error(ctx);
}


static ngx_int_t
ngx_event_maxconn_process_response(ngx_event_maxconn_ctx_t *ctx,
                                   /* OUT */ ngx_int_t *status)
{
    static const size_t HEADER_SIZE = 12;
    uint32_t            n;
    uint32_t            body_len;
    uint8_t             statuscode;
    uint8_t             opcode;
    ngx_int_t           rc;

    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, ctx->log, 0,
                   "ngx_event_maxconn_process_response: peer_fd=%d",
                   ctx->peer.connection != NULL ?
                   ctx->peer.connection->fd : -1);

    /* Parse the response of yrmcds counter extension.
     * See https://github.com/cybozu/yrmcds/blob/v1.1.0-rc2/docs/counter.md
     * for the counter protocol specification. */

    if (ctx->response->last - ctx->response->pos < (int)HEADER_SIZE)
        return NGX_AGAIN;

    if (ctx->response->pos[0] != 0x91) {
        ngx_log_error(NGX_LOG_ERR, ctx->log, 0,
                      "yrmcds server sent invalid response");
        return NGX_ERROR;
    }

    opcode = ctx->response->pos[1];
    statuscode = ctx->response->pos[2];

    memcpy(&n, &ctx->response->pos[4], sizeof(n));
    body_len = ntohl(n);

    if (ctx->response->last - ctx->response->pos < (int)(HEADER_SIZE + body_len))
        return NGX_AGAIN;

    ctx->response->pos += HEADER_SIZE + body_len;

    switch (statuscode) {
    case 0x00:  /* No error */
        rc = 200;
        if (opcode == NGX_EVENT_MAXCONN_ACQUIRE)
            ctx->n_acquired++;
        else if (opcode == NGX_EVENT_MAXCONN_RELEASE)
            ctx->n_acquired = 0;
        break;
    case 0x01:  /* Not found */
        rc = 404;
        break;
    case 0x04:  /* Invalid arguments */
        rc = 400;
        break;
    case 0x21:  /* Resource not available */
        rc = 429;
        break;
    case 0x22:  /* Not acquired */
        rc = 409;
        break;
    case 0x81:  /* Unknown command */
        rc = 405;
        break;
    default:
        rc = 500;
        break;
    }

    *status = rc;

    ngx_log_debug3(NGX_LOG_DEBUG_EVENT, ctx->log, 0,
                   "ngx_event_maxconn_process_response: op=%d, status=%d, n_acquired=%d",
                   (int)opcode, (int)rc, (int)ctx->n_acquired);

    return NGX_OK;
}


static void
ngx_event_maxconn_keep_connection(ngx_event_maxconn_ctx_t *ctx, int return_to_cache)
{
    ngx_event_maxconn_t            *maxconn;
    ngx_connection_t               *c;

    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, ctx->log, 0,
                   "ngx_event_maxconn_keep_connection: peer_fd=%d",
                   ctx->peer.connection->fd);

    /* Assume that there are no remaining bytes to read in this connection,
     * that is, there are no bytes yet to consume returned by ngx_recv().
     * Actually, since maxconn module does not send next request until it
     * reads the whole response, all bytes sent by yrmcds are read by maxconn.
     */

    maxconn = ctx->maxconn;
    c = ctx->peer.connection;

    if (return_to_cache) {
        /* Return connection to maxconn->cached_connections */
        ngx_event_maxconn_cache_t      *item;
        ngx_queue_t                    *q;

        if (ngx_queue_empty(&maxconn->free_connections)) {
            /* No free memory for ngx_event_maxconn_cache_t */
            ngx_event_maxconn_close_connection(ctx->peer.connection);
            return;
        }

        /* Pop from free_connections */
        q = ngx_queue_head(&maxconn->free_connections);
        ngx_queue_remove(q);
        item = ngx_queue_data(q, ngx_event_maxconn_cache_t, queue);

        /* Set values */
        item->peer = ctx->peer;
        item->maxconn = maxconn;

        /* Push to cached_connections */
        ngx_queue_insert_head(&maxconn->cached_connections, q);

        /* Set connection's data */
        c->data = item;
    } else {
        c->data = ctx;
    }

    if (c->read->timer_set)
        ngx_del_timer(c->read);
    if (c->write->timer_set)
        ngx_del_timer(c->write);

    c->write->handler = ngx_event_maxconn_dummy_handler;
    c->read->handler = return_to_cache ?
                       ngx_event_maxconn_close_check_handler_for_cache :
                       ngx_event_maxconn_close_check_handler_for_ctx;

    c->log = ngx_cycle->log;
    c->read->log = ngx_cycle->log;
    c->write->log = ngx_cycle->log;
    c->pool->log = ngx_cycle->log;
    c->idle = 1;

    if (c->read->ready)
        c->read->handler(c->read);
}


static void
ngx_event_maxconn_dummy_handler(ngx_event_t *ev)
{
    ngx_log_debug0(NGX_LOG_DEBUG_EVENT, ev->log, 0,
                   "ngx_event_maxconn_dummy_handler");
}


static int
ngx_event_maxconn_is_closed(ngx_connection_t *c)
{
    ssize_t n;
    char    buf[1];

    if (c->close)
        return 1;

    n = recv(c->fd, buf, 1, MSG_PEEK);

    if (n == -1 && ngx_socket_errno == NGX_EAGAIN) {
        /* stale event */

        if (ngx_handle_read_event(c->read, 0) != NGX_OK)
            return 1;

        return 0;
    }

    return 1;
}


static void
ngx_event_maxconn_close_check_handler_for_cache(ngx_event_t *ev)
{
    ngx_event_maxconn_cache_t  *cache;
    ngx_event_maxconn_t        *maxconn;
    ngx_connection_t           *c;

    c = ev->data;

    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, ev->log, 0,
                   "ngx_event_maxconn_close_check_handler_for_cache: "
                   "peer_fd=%d",
                   c->fd);

    if (!ngx_event_maxconn_is_closed(c))
        return;

    ngx_log_debug0(NGX_LOG_DEBUG_EVENT, ev->log, 0,
                   "ngx_event_maxconn_close_check_handler_for_cache: "
                   "connection closed");

    cache = c->data;
    maxconn = cache->maxconn;

    ngx_event_maxconn_close_connection(c);

    /* remove from cached_connections */
    ngx_queue_remove(&cache->queue);

    /* push to free_connections */
    ngx_memzero(cache, sizeof(ngx_event_maxconn_cache_t));
    ngx_queue_insert_head(&maxconn->free_connections, &cache->queue);
}


static void
ngx_event_maxconn_close_check_handler_for_ctx(ngx_event_t *ev)
{
    ngx_connection_t           *c;
    ngx_event_maxconn_ctx_t    *ctx;

    c = ev->data;

    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, ev->log, 0,
                   "ngx_event_maxconn_close_check_handler_for_ctx: "
                   "peer_fd=%d",
                   c->fd);

    if (!ngx_event_maxconn_is_closed(c))
        return;

    ngx_log_debug0(NGX_LOG_DEBUG_EVENT, ev->log, 0,
                   "ngx_event_maxconn_close_check_handler_for_ctx: "
                   "connection closed");

    ctx = c->data;
    ngx_event_maxconn_close_connection(c);
    ngx_event_maxconn_destroy_context(ctx);
}


void
ngx_event_maxconn_cleanup(ngx_event_maxconn_t *maxconn,
                          ngx_connection_t *client)
{
    ngx_event_maxconn_ctx_t   *ctx;

    ctx = ngx_event_maxconn_get_context(maxconn, client);
    if (ctx == NULL) {
        /* Context not found. Maybe already destructed. */
        return;
    }

    /* Make sure that this context is not accessed by the client connection
     * after the client request is cleaned up. */
    ngx_queue_remove(&ctx->queue);
    ctx->queue.next = NULL;
    ctx->queue.prev = NULL;
    ctx->callback.handler = NULL;
    ctx->callback.data = NULL;

    switch (ctx->state) {
    case NGX_EVENT_MAXCONN_STATE_INITIAL:
        if (ctx->n_acquired > 0) {
            /* release aquired resources */
            ngx_event_maxconn_release_internal(maxconn, ctx, client);
        } else {
            /* release operation is not needed */
            ngx_event_maxconn_keep_connection(ctx, 1);
            ngx_event_maxconn_destroy_context(ctx);
        }
        break;

    case NGX_EVENT_MAXCONN_STATE_ACQUIRING:
        ngx_event_maxconn_error(ctx);       /* abort connection */
        break;

    case NGX_EVENT_MAXCONN_STATE_RELEASING:
        /* nothing to do */
        break;

    default:
        assert(0 && "BUG!!");
        break;
    }
}


static void
ngx_event_maxconn_error(ngx_event_maxconn_ctx_t *ctx)
{
    ngx_event_maxconn_callback_t callback = ctx->callback;

    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, ctx->log, 0,
                   "ngx_event_maxconn_error: peer_fd=%d",
                   ctx->peer.connection != NULL ?
                   ctx->peer.connection->fd : -1);

    if (ctx->peer.connection != NULL)
        ngx_event_maxconn_close_connection(ctx->peer.connection);
    ngx_event_maxconn_destroy_context(ctx);
    if (callback.handler != NULL)
        callback.handler(NGX_ERROR, callback.data);
}


static void
ngx_event_maxconn_destroy_context(ngx_event_maxconn_ctx_t *ctx)
{
    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, ctx->log, 0,
                   "ngx_event_maxconn_destroy_context: ctx=%p",
                   ctx);

    if (ctx->queue.next != NULL && ctx->queue.prev != NULL) {
        ngx_queue_remove(&ctx->queue);
    }
    ngx_destroy_pool(ctx->pool);
}


static void
ngx_event_maxconn_close_connection(ngx_connection_t *c)
{
    ngx_log_debug1(NGX_LOG_DEBUG_EVENT, c->log, 0,
                   "ngx_event_maxconn_close_connection: peer_fd=%d",
                   c->fd);

    ngx_destroy_pool(c->pool);
    ngx_close_connection(c);
}
