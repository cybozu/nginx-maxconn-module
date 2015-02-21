nginx-maxconn-module
====================

The `nginx-maxconn-module` is used to limit the number of simultaneous requests per defined key.

This module uses [counter extension](https://github.com/cybozu/yrmcds/blob/master/docs/counter.md) of [yrmcds](https://github.com/cybozu/yrmcds) to count requests.  External counting server enables us to count the requests consistently even if there are multiple nginx servers across multiple hosts.


Example Configuration
---------------------

```
server {
    # The address of yrmcds server to count requests.
    maxconn_server 127.0.0.1:11215;

    location / {
        # Limit requests up to 10 by their remote IP addresses.
        set $maxconn_key   $remote_addr;
        set $maxconn_limit 10;
        maxconn_acquire;

        # Specify the error page of 429 (Too Many Requests).
        error_page 429 /path/to/429.html;

        ...
    }

    location /foobar/ {
        # Limit requests by the pairs of the remote addrs and the request paths.
        set $maxconn_key   $remote_addr:$uri;
        set $maxconn_limit 6;
        maxconn_acquire;

        ...
    }
}
```


Directives
----------

### maxconn_server

Syntax: `maxconn_server ADDR`  
Context: server

Specify the address of yrmcds server to count requests.


### maxconn_acquire

Syntax: `maxconn_acquire`  
Context: location

Send an `Acquire` command to increment the counter at yrmcds.  If `Acquire` fails on `Not acquired` error, nginx returns "429 Too Many Client" to a client.

You need to define following variables before `maxconn_acquire`:

- `$maxconn_key` - the name of the counter to acquire.
- `$maxconn_limit` - the maximum value of the counter.

The incremented counter is automatically decremented at the end of the request.


### maxconn_release

Syntax: `maxconn_release`  
Context: location

Send a `Release` command to decrement the counter at yrmcds.

If `Acquire` command is not succeeded in the same request processing, this directive does nothing.  Therefore, it is safe to execute `maxconn_release` without executing `maxconn_acquire`.


Install
-------

Specify `--add-module=/path/to/nginx-maxconn-module` when you run `./configure`.

Example:

```
./configure --add-module=/path/to/nginx-maxconn-module
make
make install
```


License
-------

2-clause BSD-like license.
