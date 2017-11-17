/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.common.http.auth.basic;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.elasticsearch.xpack.watcher.common.http.auth.ApplicableHttpAuth;
import org.elasticsearch.xpack.watcher.crypto.CryptoService;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ApplicableBasicAuth extends ApplicableHttpAuth<BasicAuth> {

    private final String basicAuth;
    private final CryptoService cryptoService;

    public ApplicableBasicAuth(BasicAuth auth, CryptoService service) {
        super(auth);
        basicAuth = headerValue(auth.username, auth.password.text(service));
        this.cryptoService = service;
    }

    public static String headerValue(String username, char[] password) {
        return "Basic " + Base64.getEncoder().encodeToString((username + ":" + new String(password)).getBytes(StandardCharsets.UTF_8));
    }

    public void apply(HttpURLConnection connection) {
        connection.setRequestProperty("Authorization", basicAuth);
    }

    @Override
    public void apply(CredentialsProvider credsProvider, AuthScope authScope) {
        credsProvider.setCredentials(authScope,
                new UsernamePasswordCredentials(auth.username, new String(auth.password.text(cryptoService))));
    }

}
