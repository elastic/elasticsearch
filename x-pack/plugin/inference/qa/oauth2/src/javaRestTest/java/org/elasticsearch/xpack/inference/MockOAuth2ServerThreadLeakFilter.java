/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Filters daemon threads left by the OkHttp {@code MockWebServer} embedded inside
 * {@code no.nav.security.mock.oauth2.MockOAuth2Server} (see the
 * <a href="https://github.com/navikt/mock-oauth2-server/tree/master">mock-oauth2-server</a> project).
 *
 * <p>The shutdown call chain — {@code MockOAuth2Server.shutdown()} &rarr;
 * {@code MockWebServerWrapper.stop()} &rarr; {@code mockWebServer.shutdown()} (see
 * <a href="https://github.com/navikt/mock-oauth2-server/blob/2.2.1/src/main/kotlin/no/nav/security/mock/oauth2/http/OAuth2HttpServer.kt#L100">
 * OAuth2HttpServer.kt:100</a>) &rarr; OkHttp {@link java.util.concurrent.ThreadPoolExecutor#shutdown()}
 * in {@code TaskRunner.RealBackend} (see
 * <a href="https://github.com/square/okhttp/blob/parent-4.12.0/okhttp/src/main/kotlin/okhttp3/internal/concurrent/TaskRunner.kt#L302">
 * TaskRunner.kt:302</a>) — is graceful but non-blocking: no {@code awaitTermination} follows, so the
 * backing daemon threads exit asynchronously after {@code shutdown()} returns, creating a race with
 * the randomized-testing thread-leak check that can flake under CI load.
 *
 * <p>All threads spawned by the embedded server carry the {@code "MockWebServer "} prefix, set in
 * the OkHttp {@code MockWebServer} constructor (see
 * <a href="https://github.com/square/okhttp/blob/parent-4.12.0/mockwebserver/src/main/kotlin/okhttp3/mockwebserver/MockWebServer.kt">
 * MockWebServer.kt</a>): {@code "MockWebServer TaskRunner"} (task executor),
 * {@code "MockWebServer <address>"} (accept loop), and {@code "MockWebServer WebSocket ..."}.
 */
public class MockOAuth2ServerThreadLeakFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("MockWebServer ");
    }
}
