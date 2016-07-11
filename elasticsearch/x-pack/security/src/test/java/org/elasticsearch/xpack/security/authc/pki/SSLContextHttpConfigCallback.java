/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.elasticsearch.client.RestClient;

import javax.net.ssl.SSLContext;

class SSLContextHttpConfigCallback implements RestClient.HttpClientConfigCallback {

    private final SSLContext sslContext;

    SSLContextHttpConfigCallback(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    public void customizeDefaultRequestConfig(RequestConfig.Builder requestConfigBuilder) {

    }

    @Override
    public void customizeHttpClient(HttpClientBuilder httpClientBuilder) {
        httpClientBuilder.setSSLContext(sslContext);
    }
}
