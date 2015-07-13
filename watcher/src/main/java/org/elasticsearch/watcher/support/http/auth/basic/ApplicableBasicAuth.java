/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.http.auth.basic;

import org.elasticsearch.common.Base64;
import org.elasticsearch.watcher.support.http.auth.ApplicableHttpAuth;
import org.elasticsearch.watcher.support.secret.SecretService;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

/**
 */
public class ApplicableBasicAuth extends ApplicableHttpAuth<BasicAuth> {

    private final String basicAuth;

    public ApplicableBasicAuth(BasicAuth auth, SecretService service) {
        super(auth);
        basicAuth = headerValue(auth.username, auth.password.text(service));
    }

    public static String headerValue(String username, char[] password) {
        return "Basic " + Base64.encodeBytes((username + ":" + new String(password)).getBytes(StandardCharsets.UTF_8));
    }

    public void apply(HttpURLConnection connection) {
        connection.setRequestProperty("Authorization", basicAuth);
    }

}
