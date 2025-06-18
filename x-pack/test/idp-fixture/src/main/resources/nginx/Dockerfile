# We need to do SSL tunnelling, but NGINX doesn't support this out of the box - there's a module for this, but the only
#  way to install it is to compile NGINX from source, so here we go

FROM nginx:1.27.1

RUN set -x \
    && apt-get update \
    # libs and tools that we'll need to build NGINX
    && apt-get install --no-install-recommends --no-install-suggests -y  \
      zlib1g-dev libpcre2-dev libssl-dev wget git gcc patch make  \
    && wget http://nginx.org/download/nginx-1.27.1.tar.gz \
    && git clone --depth 1 --branch v0.0.7 --single-branch https://github.com/chobits/ngx_http_proxy_connect_module.git \
    && tar -xzvf nginx-1.27.1.tar.gz \
    && cd nginx-1.27.1 \
    # patch the CONNECT module in
    && patch -p1 < ../ngx_http_proxy_connect_module/patch/proxy_connect_rewrite_102101.patch \
    && ./configure \
      --sbin-path=/usr/local/bin/nginx \
      --conf-path=/etc/nginx/nginx.conf \
      --pid-path=/etc/nginx/nginx.pid \
      --with-http_ssl_module \
      --http-log-path=/dev/stdout \
      --error-log-path=/dev/stderr \
      --add-module=../ngx_http_proxy_connect_module \
    && make && make install \
    && apt-get remove --purge --auto-remove -y && rm -rf /var/lib/apt/lists/*

COPY ./nginx/nginx.conf /etc/nginx/nginx.conf

ENTRYPOINT ["/docker-entrypoint.sh"]

EXPOSE 8888
EXPOSE 8889

STOPSIGNAL SIGQUIT

CMD ["nginx", "-g", "daemon off;"]
