# We need to do SSL tunnelling, but NGINX doesn't support this out of the box - there's a module for this, but the only
#  way to install it is to compile NGINX from source, so here we go
#
# OPTIMIZED: Using multi-stage build with Alpine to reduce image size from ~950MB to ~15MB

#######################
# Build stage
#######################
FROM nginx:1.27-alpine AS builder

ENV NGINX_VERSION=1.27.1

RUN set -x \
    && apk add --no-cache --virtual .build-deps \
       gcc libc-dev make openssl-dev pcre2-dev zlib-dev linux-headers wget git patch \
    && wget -q http://nginx.org/download/nginx-${NGINX_VERSION}.tar.gz \
    && git clone --depth 1 --branch v0.0.7 --single-branch https://github.com/chobits/ngx_http_proxy_connect_module.git \
    && tar -xzf nginx-${NGINX_VERSION}.tar.gz \
    && cd nginx-${NGINX_VERSION} \
    && patch -p1 < ../ngx_http_proxy_connect_module/patch/proxy_connect_rewrite_102101.patch \
    && ./configure \
       --prefix=/etc/nginx \
       --sbin-path=/usr/sbin/nginx \
       --modules-path=/usr/lib/nginx/modules \
       --conf-path=/etc/nginx/nginx.conf \
       --error-log-path=/var/log/nginx/error.log \
       --http-log-path=/var/log/nginx/access.log \
       --pid-path=/var/run/nginx.pid \
       --lock-path=/var/run/nginx.lock \
       --with-http_ssl_module \
       --with-http_realip_module \
       --with-http_stub_status_module \
       --with-threads \
       --with-stream \
       --with-stream_ssl_module \
       --add-module=../ngx_http_proxy_connect_module \
    && make -j$(nproc) \
    && make install


#######################
# Runtime stage - minimal image
#######################
FROM nginx:1.27-alpine

# Copy the custom-built nginx binary with the proxy_connect module
COPY --from=builder /usr/sbin/nginx /usr/sbin/nginx

# Copy custom config
COPY ./nginx.conf /etc/nginx/nginx.conf

# Create log symlinks to stdout/stderr (like official nginx image)
RUN ln -sf /dev/stdout /var/log/nginx/access.log \
    && ln -sf /dev/stderr /var/log/nginx/error.log

EXPOSE 8888
EXPOSE 8889

STOPSIGNAL SIGQUIT

CMD ["nginx", "-g", "daemon off;"]
