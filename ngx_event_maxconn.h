/* (C) 2015 Cybozu.  All rights reserved. */

#ifndef _NGX_EVENT_MAXCONN_H_INCLUDED_
#define _NGX_EVENT_MAXCONN_H_INCLUDED_


typedef struct ngx_event_maxconn_s ngx_event_maxconn_t;


typedef struct {
    void  (*handler)(ngx_int_t rc, void *data);
    void  *data;
} ngx_event_maxconn_callback_t;


ngx_event_maxconn_t *
ngx_event_maxconn_init(ngx_conf_t *cf,
                       ngx_str_t server,
                       size_t max_cached);


/*
 * Send an acquire request to the server specified by ngx_event_maxconn_init().
 * When the response is returned, the callback function is called with the
 * returncode from the server and callback->data.
 *
 * Returns:
 *   NGX_OK    - When no errors occurred.
 *   Others    - An error occurred before the request completes or blocked.
 *               In this case, the callback function is not called.
 *
 * Note:
 *   The callback function may be called before or after this function returns.
 *   If you call ngx_event_maxconn_cleanup before the callback function is called,
 *   the callback function is not called.
 */
ngx_int_t
ngx_event_maxconn_acquire(ngx_event_maxconn_t *maxconn,
                          ngx_int_t limit,
                          ngx_str_t key,
                          ngx_connection_t *client,
                          ngx_event_maxconn_callback_t callback);


/*
 * Send a Release operation to the server to release all acquired resources.
 */
void
ngx_event_maxconn_release(ngx_event_maxconn_t *maxconn,
                          ngx_connection_t *client);


/* Send a Release operation to the server to release all acquired resources
 * if there are acquired resources.
 * Terminate running acquire requests.
 * Note that running release requests are not terminated.
 */
void
ngx_event_maxconn_cleanup(ngx_event_maxconn_t *maxconn,
                          ngx_connection_t *client);


#endif
