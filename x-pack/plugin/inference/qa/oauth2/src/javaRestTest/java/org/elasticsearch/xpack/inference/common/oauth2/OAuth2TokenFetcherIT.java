/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.nimbusds.jose.JOSEObjectType;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.MockOAuth2ServerThreadLeakFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockOAuth2ClusterSettings;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Integration test that exercises {@link OAuth2TokenFetcher} against a real in-process OAuth2 identity
 * provider ({@link MockOAuth2Server}) to confirm Nimbus interoperability with a spec-compliant IdP.
 *
 * <p>Complements the unit-level {@link org.elasticsearch.xpack.inference.common.oauth2.OAuth2TokenFetcherTests}, which uses ES's hand-rolled
 * {@link org.elasticsearch.test.http.MockWebServer}; this test uses a proper RFC 6749 server so the full token-endpoint
 * protocol (well-known discovery, signed JWTs, etc.) is exercised.
 */
// GraalVMThreadsFilter, NettyGlobalThreadsFilter, and JnaCleanerThreadsFilter are declared on
// ESTestCase, but @ThreadLeakFilters is not merged across the class hierarchy — a subclass
// annotation wholly replaces the inherited one, so they must be re-listed here.
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        MockOAuth2ServerThreadLeakFilter.class }
)
public class OAuth2TokenFetcherIT extends ESTestCase {

    private static final String INFERENCE_ID = "inference-id";
    private static final String CLIENT_ID = "client";
    private static final String CLIENT_SECRET = "secret";
    private static final String ISSUER_ID = "default";
    private static final String SUBJECT = "subject"; // ignored for client_credentials (client id is used)
    private static final String SCOPE = "api";
    private static final List<String> SCOPES = List.of(SCOPE);
    private static final Duration TOKEN_EXPIRY = Duration.ofMinutes(30);

    private static MockOAuth2Server idp;
    private static URI tokenEndpoint;
    private ThreadPool threadPool;

    @BeforeClass
    public static void startIdp() throws Exception {
        idp = new MockOAuth2Server();
        idp.start();
        tokenEndpoint = idp.tokenEndpointUrl(ISSUER_ID).uri();
    }

    @AfterClass
    public static void stopIdp() throws Exception {
        idp.shutdown();
        idp = null;
    }

    @Before
    public void startThreadPool() {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testFetch_ReturnsTokenExpiringInThirtyMinutes() {
        idp.enqueueCallback(tokenCallback());
        var fetcher = new OAuth2TokenFetcher(
            INFERENCE_ID,
            tokenEndpoint,
            CLIENT_ID,
            CLIENT_SECRET,
            SCOPES,
            threadPool,
            mockOAuth2ClusterSettings()
        );

        var before = Instant.now();
        var future = new TestPlainActionFuture<CachedToken>();
        fetcher.fetch(future);
        var token = future.actionGet(TEST_REQUEST_TIMEOUT);
        var after = Instant.now();

        assertThat(token.bearer(), not(emptyString()));
        // expiresAt = fetch-time + 30m, so it lands between before+30m and after+30m (±1s slack).
        assertTrue(token.expiresAt().isAfter(before.plus(TOKEN_EXPIRY).minusSeconds(1)));
        assertFalse(token.expiresAt().isAfter(after.plus(TOKEN_EXPIRY).plusSeconds(1)));
    }

    public void testFetch_SendsClientCredentialsRequestToIdp() {
        idp.enqueueCallback(tokenCallback());
        var fetcher = new OAuth2TokenFetcher(
            INFERENCE_ID,
            tokenEndpoint,
            CLIENT_ID,
            CLIENT_SECRET,
            SCOPES,
            threadPool,
            mockOAuth2ClusterSettings()
        );

        var future = new TestPlainActionFuture<CachedToken>();
        fetcher.fetch(future);
        future.actionGet(TEST_REQUEST_TIMEOUT);

        var recorded = idp.takeRequest(); // RecordedRequest from the embedded okhttp MockWebServer
        assertThat(recorded.getMethod(), is("POST"));
        assertThat(recorded.getPath(), endsWith("/token"));
        assertThat(recorded.getHeader(HttpHeaders.AUTHORIZATION), startsWith("Basic "));
        var body = recorded.getBody().readUtf8();
        assertThat(body, containsString("grant_type=client_credentials"));
        assertThat(body, containsString("scope=" + SCOPE));
    }

    /**
     * Builds a token callback with an explicit 30-minute expiry.
     * {@code expiry} is the last {@code @JvmOverloads} parameter, so Java must pass every
     * preceding argument rather than relying on the library's default of 3600 seconds.
     */
    private static DefaultOAuth2TokenCallback tokenCallback() {
        return new DefaultOAuth2TokenCallback(
            ISSUER_ID,
            SUBJECT,
            JOSEObjectType.JWT.getType(), // typeHeader
            null,                         // audience (defaults to the requested scopes)
            Map.of(),                     // extra claims
            TOKEN_EXPIRY.toSeconds()      // explicit 30-minute lifetime
        );
    }
}
