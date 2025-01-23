/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;

class VersionSpecificNetworkChecks {
    static void createInetAddressResolverProvider() {
        var x = new InetAddressResolverProvider() {
            @Override
            public InetAddressResolver get(Configuration configuration) {
                return null;
            }

            @Override
            public String name() {
                return "TEST";
            }
        };
    }

    static void httpClientSend() throws InterruptedException {
        HttpClient httpClient = HttpClient.newBuilder().build();
        try {
            httpClient.send(HttpRequest.newBuilder(URI.create("http://localhost")).build(), HttpResponse.BodyHandlers.discarding());
        } catch (IOException e) {
            // Expected, the send action may fail with these parameters (but after it run the entitlement check in the prologue)
        }
    }

    static void httpClientSendAsync() {
        HttpClient httpClient = HttpClient.newBuilder().build();
        httpClient.sendAsync(HttpRequest.newBuilder(URI.create("http://localhost")).build(), HttpResponse.BodyHandlers.discarding());
    }
}
