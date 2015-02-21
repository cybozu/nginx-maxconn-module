/* (C) 2015 Cybozu.  All rights reserved. */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_event_maxconn.h>


static ngx_int_t ngx_http_maxconn_precofig(ngx_conf_t *cf);
static ngx_int_t ngx_http_maxconn_postconfig(ngx_conf_t *cf);
static void *ngx_http_maxconn_create_srv_conf(ngx_conf_t *cf);
static char *ngx_http_maxconn_merge_srv_conf(ngx_conf_t *cf, void *parent, void *child);
static void *ngx_http_maxconn_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_maxconn_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);
static char *ngx_http_maxconn_acquire(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_maxconn_release(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);


typedef enum acquire_status_e {
    NGX_HTTP_MAXCONN_AS_INITIAL,
    NGX_HTTP_MAXCONN_AS_ACQUIRING,
    NGX_HTTP_MAXCONN_AS_DONE,
} ngx_http_maxconn_acquire_status_t;


typedef struct {
    ngx_http_maxconn_acquire_status_t   acquire_status;
} ngx_http_maxconn_ctx_t;


typedef struct {
    ngx_str_t               server;
    ngx_event_maxconn_t    *maxconn;
} ngx_http_maxconn_srv_conf_t;


typedef struct {
    ngx_flag_t          acquire;
    ngx_flag_t          release;

    /* index for variables */
    ngx_int_t           limit_index;
    ngx_int_t           key_index;
} ngx_http_maxconn_loc_conf_t;


static ngx_command_t  ngx_http_maxconn_commands[] = {

    { ngx_string("maxconn_server"),
      NGX_HTTP_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot,
      NGX_HTTP_SRV_CONF_OFFSET,
      offsetof(ngx_http_maxconn_srv_conf_t, server),
      NULL },

    { ngx_string("maxconn_acquire"),
      NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_maxconn_acquire,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("maxconn_release"),
      NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_maxconn_release,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_maxconn_loc_conf_t, release),
      NULL },

    ngx_null_command
};


static ngx_http_module_t  ngx_http_maxconn_module_ctx = {
    ngx_http_maxconn_precofig,             /* preconfiguration */
    ngx_http_maxconn_postconfig,           /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    ngx_http_maxconn_create_srv_conf,      /* create server configuration */
    ngx_http_maxconn_merge_srv_conf,       /* merge server configuration */

    ngx_http_maxconn_create_loc_conf,      /* create location configuration */
    ngx_http_maxconn_merge_loc_conf        /* merge location configuration */
};


ngx_module_t  ngx_http_maxconn_module = {
    NGX_MODULE_V1,
    &ngx_http_maxconn_module_ctx,          /* module context */
    ngx_http_maxconn_commands,             /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_str_t  ngx_http_maxconn_limit = ngx_string("maxconn_limit");
static ngx_str_t  ngx_http_maxconn_key   = ngx_string("maxconn_key");


/* A callback function called when the response to an Acquire request is returned by yrmcds. */
static void
ngx_http_maxconn_post_acquire(ngx_int_t rc, void *data)
{
    ngx_http_request_t     *r = data;
    ngx_http_maxconn_ctx_t *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_maxconn_module);
    ctx->acquire_status = NGX_HTTP_MAXCONN_AS_DONE;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "ngx_http_maxconn_post_acquire: rc=%d, uri=\"%V\"", (int)rc, &r->uri);

    /* finalize request when "Too Many Requests" is returned. */
    if (rc == 429) {
        ngx_http_finalize_request(r, rc);
        return;
    }

    /* If errorcode is other than "Too Many Requests", then continue request processing. */
    if (rc != NGX_HTTP_OK) {
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "maxconn server returned error code %d for \"%V\"", (int)rc, &r->uri);
    }
    ngx_http_core_run_phases(r);
}


/* A callback function called when the request from client is closed. */
static void
ngx_http_maxconn_cleanup(void *data)
{
    ngx_http_request_t *r = data;
    ngx_http_maxconn_srv_conf_t *mscf;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "ngx_http_maxconn_cleanup: uri=\"%V\"", &r->uri);

    mscf = ngx_http_get_module_srv_conf(r, ngx_http_maxconn_module);
    ngx_event_maxconn_cleanup(mscf->maxconn, r->connection);
}


static ngx_int_t
ngx_http_maxconn_process_acquire(ngx_http_request_t *r,
                                 ngx_http_maxconn_loc_conf_t *mcf,
                                 ngx_http_maxconn_srv_conf_t *mscf)
{
    ngx_http_maxconn_ctx_t      *ctx;
    ngx_http_variable_value_t   *vv;
    ngx_int_t                    rc;
    ngx_http_cleanup_t          *cln;
    ngx_int_t                    limit;
    ngx_str_t                    key;

    ctx = ngx_http_get_module_ctx(r, ngx_http_maxconn_module);
    if (ctx == NULL || ctx->acquire_status == NGX_HTTP_MAXCONN_AS_INITIAL) {
        /* limit */
        vv = ngx_http_get_indexed_variable(r, mcf->limit_index);
        if (vv == NULL || vv->not_found || vv->len == 0) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "maxconn: the 'maxconn_limit' variable is not set");
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        limit = ngx_atoi(vv->data, vv->len);
        if (limit == NGX_ERROR) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "counter: invalid 'maxconn_limit' value: '%v'", vv);
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        /* key */
        vv = ngx_http_get_indexed_variable(r, mcf->key_index);
        if (vv == NULL || vv->not_found || vv->len == 0) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "counter: the 'maxconn_key' variable is not set");
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        if (vv->len == 0) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "counter: 'maxconn_key' cannot be empty");
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        key.len = vv->len;
        key.data = vv->data;

        ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_maxconn_ctx_t));
        if (ctx == NULL)
            return NGX_ERROR;
        ngx_http_set_ctx(r, ctx, ngx_http_maxconn_module);

        cln = ngx_http_cleanup_add(r, 0);
        if (cln == NULL)
            return NGX_ERROR;
        cln->handler = ngx_http_maxconn_cleanup;
        cln->data = r;

        ngx_event_maxconn_callback_t callback;
        callback.handler = ngx_http_maxconn_post_acquire;
        callback.data = r;

        ngx_log_debug4(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "ngx_http_maxconn_process_acquire: uri=\"%V\", limit=%d, key=\"%V\", fd=%d",
                       &r->uri, (int)limit, &key, r->connection->fd);

        ctx->acquire_status = NGX_HTTP_MAXCONN_AS_ACQUIRING;
        rc = ngx_event_maxconn_acquire(mscf->maxconn, limit, key, r->connection, callback);
        if (rc == NGX_OK) {
            /*
             * Case 1: callback is not called.
             *         In this case, we should block the main request processing.
             *         Therefore, we return NGX_AGAIN.
             * Case 2: callback is already called, and the phase already advanced.
             *         In this case, nothing to do.
             *         Therefore, we return NGX_AGAIN.
             */
            return NGX_AGAIN;
        }
        /* An error occurred. */
        ctx->acquire_status = NGX_HTTP_MAXCONN_AS_INITIAL;
        return NGX_DECLINED;
    }

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "ngx_http_maxconn_process_acquire: uri=\"%V\", acquire_status=%d",
                   &r->uri, (int)ctx->acquire_status);

    if (ctx->acquire_status != NGX_HTTP_MAXCONN_AS_DONE)
        return NGX_AGAIN;

    ctx->acquire_status = NGX_HTTP_MAXCONN_AS_INITIAL;
    return NGX_DECLINED;
}


static ngx_int_t
ngx_http_maxconn_process_release(ngx_http_request_t *r,
                                 ngx_http_maxconn_srv_conf_t *mscf)
{
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "ngx_http_maxconn_process_release: uri=\"%V\"",
                   &r->uri);

    ngx_event_maxconn_release(mscf->maxconn, r->connection);
    return NGX_DECLINED;
}


static ngx_int_t
ngx_http_maxconn_preaccess_handler(ngx_http_request_t *r)
{
    ngx_http_maxconn_loc_conf_t *mcf;
    ngx_http_maxconn_srv_conf_t *mscf;

    mscf = ngx_http_get_module_srv_conf(r, ngx_http_maxconn_module);

    if (mscf->maxconn == NULL)
        return NGX_DECLINED;

    mcf = ngx_http_get_module_loc_conf(r, ngx_http_maxconn_module);

    if (mcf->acquire == 1)
        return ngx_http_maxconn_process_acquire(r, mcf, mscf);

    if (mcf->release == 1)
        return ngx_http_maxconn_process_release(r, mscf);

    return NGX_DECLINED;
}


static char *
ngx_http_maxconn_acquire(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_maxconn_loc_conf_t    *mcf;

    mcf = conf;
    if (mcf->acquire == 1)
        return "is duplicate";
    if (mcf->release == 1)
        return "cannot be placed on the same location to 'maxconn_release'";
    mcf->acquire = 1;

    /* store variable indices */

    mcf->limit_index = ngx_http_get_variable_index(cf, &ngx_http_maxconn_limit);
    if (mcf->limit_index == NGX_ERROR)
        return NGX_CONF_ERROR;

    mcf->key_index = ngx_http_get_variable_index(cf, &ngx_http_maxconn_key);
    if (mcf->key_index == NGX_ERROR)
        return NGX_CONF_ERROR;

    return NGX_CONF_OK;
}


static char *
ngx_http_maxconn_release(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_maxconn_loc_conf_t    *mcf;

    mcf = conf;
    if (mcf->release == 1)
        return "is duplicate";
    if (mcf->acquire == 1)
        return "cannot be placed on the same location to 'maxconn_acquire'";
    mcf->release = 1;

    /* store variable indices */

    mcf->limit_index = ngx_http_get_variable_index(cf, &ngx_http_maxconn_limit);
    if (mcf->limit_index == NGX_ERROR)
        return NGX_CONF_ERROR;

    mcf->key_index = ngx_http_get_variable_index(cf, &ngx_http_maxconn_key);
    if (mcf->key_index == NGX_ERROR)
        return NGX_CONF_ERROR;

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_maxconn_variable_not_found(ngx_http_request_t *r, ngx_http_variable_value_t *v,
                                    uintptr_t data)
{
    v->not_found = 1;
    return NGX_OK;
}


static ngx_int_t
ngx_http_maxconn_precofig(ngx_conf_t *cf)
{
    ngx_http_variable_t        *v;

    /* add variables */

    v = ngx_http_add_variable(cf, &ngx_http_maxconn_limit, NGX_HTTP_VAR_CHANGEABLE);
    if (v == NULL)
        return NGX_ERROR;
    v->get_handler = ngx_http_maxconn_variable_not_found;

    v = ngx_http_add_variable(cf, &ngx_http_maxconn_key, NGX_HTTP_VAR_CHANGEABLE);
    if (v == NULL)
        return NGX_ERROR;
    v->get_handler = ngx_http_maxconn_variable_not_found;

    return NGX_OK;
}


static ngx_int_t
ngx_http_maxconn_postconfig(ngx_conf_t *cf)
{
    ngx_http_handler_pt        *h;
    ngx_http_core_main_conf_t  *cmcf;

    /* add phase handers */

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    h = ngx_array_push(&cmcf->phases[NGX_HTTP_PREACCESS_PHASE].handlers);
    if (h == NULL)
        return NGX_ERROR;
    *h = ngx_http_maxconn_preaccess_handler;

    return NGX_OK;
}


static void *
ngx_http_maxconn_create_srv_conf(ngx_conf_t *cf)
{
    ngx_http_maxconn_srv_conf_t    *mscf;

    mscf = ngx_pcalloc(cf->pool, sizeof(ngx_http_maxconn_srv_conf_t));
    if (mscf == NULL)
        return NULL;

    return mscf;
}


static char *
ngx_http_maxconn_merge_srv_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_maxconn_srv_conf_t *prev = parent;
    ngx_http_maxconn_srv_conf_t *conf = child;

    ngx_conf_merge_str_value(conf->server, prev->server, "");

    if (conf->server.len > 0) {
        conf->maxconn = ngx_event_maxconn_init(cf, conf->server, 100);
        if (conf->maxconn == NULL)
            return NGX_CONF_ERROR;
    } else {
        conf->maxconn = NULL;
    }

    return NGX_CONF_OK;
}


static void *
ngx_http_maxconn_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_maxconn_loc_conf_t    *mcf;

    mcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_maxconn_loc_conf_t));
    if (mcf == NULL)
        return NULL;

    mcf->limit_index = NGX_CONF_UNSET;
    mcf->key_index = NGX_CONF_UNSET;
    mcf->acquire = NGX_CONF_UNSET;
    mcf->release = NGX_CONF_UNSET;

    return mcf;
}


static char *
ngx_http_maxconn_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_maxconn_loc_conf_t    *prev = parent;
    ngx_http_maxconn_loc_conf_t    *conf = child;
    ngx_conf_merge_value(conf->acquire, prev->acquire, 0);
    ngx_conf_merge_value(conf->release, prev->release, 0);

    if (conf->limit_index == NGX_CONF_UNSET)
        conf->limit_index = prev->limit_index;
    if (conf->key_index == NGX_CONF_UNSET)
        conf->key_index = prev->key_index;

    return NGX_CONF_OK;
}
