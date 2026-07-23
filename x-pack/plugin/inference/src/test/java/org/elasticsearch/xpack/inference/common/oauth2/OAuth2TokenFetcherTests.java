/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterService;
import static org.elasticsearch.xpack.inference.Utils.mockOAuth2ClusterSettings;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class OAuth2TokenFetcherTests extends ESTestCase {

    private static final String INFERENCE_ID = "test-inference-id";
    private static final String CLIENT_ID = "test-client-id";
    private static final String CLIENT_SECRET = "test-client-secret";
    private static final String BEARER = "test-access-token";
    private static final int EXPIRES_IN = 3600;
    private static final String TOKEN_PATH = "/token";
    private static final String SCOPE_ITEM = "api";
    private static final List<String> SCOPES = List.of("api");
    private static final String SUCCESS_BODY = Strings.format("""
            {"access_token":"%s","token_type":"Bearer","expires_in":%d}
        """, BEARER, EXPIRES_IN);

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
        webServer.close();
    }

    public void testFetch_SuccessReturnsCachedToken() throws Exception {
        webServer.enqueue(successResponse());

        var clock = Clock.fixed(Instant.now(), Clock.systemUTC().getZone());
        var fetcher = createFetcher(SCOPES, clock);
        var future = new TestPlainActionFuture<CachedToken>();
        fetcher.fetch(future);
        var token = future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

        assertThat(token.bearer(), is(BEARER));
        assertThat(token.expiresAt(), is(clock.instant().plusSeconds(EXPIRES_IN)));
    }

    public void testFetch_SendsClientCredentialsRequest() throws Exception {
        webServer.enqueue(successResponse());

        var fetcher = createFetcher(SCOPES);
        var future = new TestPlainActionFuture<CachedToken>();
        fetcher.fetch(future);
        future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

        assertThat(webServer.requests(), hasSize(1));
        var request = webServer.requests().getFirst();
        assertThat(request.getMethod(), is(HttpPost.METHOD_NAME));
        assertThat(request.getUri().getPath(), is(TOKEN_PATH));
        assertThat(request.getHeader(HttpHeaders.AUTHORIZATION), containsString("Basic "));
        assertThat(request.getBody(), is(Strings.format("grant_type=client_credentials&scope=%s", SCOPE_ITEM)));
    }

    public void testFetch_EmptyScopesOmitsScopeParameter() throws Exception {
        webServer.enqueue(successResponse());

        var fetcher = createFetcher(List.of());
        var future = new TestPlainActionFuture<CachedToken>();
        fetcher.fetch(future);
        future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

        assertThat(webServer.requests(), hasSize(1));
        assertThat(webServer.requests().getFirst().getBody(), is("grant_type=client_credentials"));
    }

    public void testFetch_ErrorResponseFailsListener() throws Exception {
        var errorCode = "invalid_client";
        var errorDescription = "bad creds";
        var body = XContentHelper.stripWhitespace(Strings.format("""
                {
                    "error":"%s",
                    "error_description":"%s"
                }
            """, errorCode, errorDescription));

        webServer.enqueue(
            new MockResponse().setResponseCode(400)
                .setBody(body)
                .addHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters())
        );

        var fetcher = createFetcher(SCOPES);
        var future = new TestPlainActionFuture<CachedToken>();
        fetcher.fetch(future);
        var thrown = expectThrows(ElasticsearchException.class, () -> future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));

        assertThat(
            thrown.getMessage(),
            is(
                Strings.format(
                    "Failed to retrieve access token for request for inference id [%s]: [%s] %s",
                    INFERENCE_ID,
                    errorCode,
                    errorDescription
                )
            )
        );
    }

    public void testFetch_MalformedResponseFailsWithCause() throws Exception {
        // Valid Content-Type but non-JSON body triggers a ParseException inside the fetch,
        // which is caught by the generic catch block and wrapped in an ElasticsearchException.
        webServer.enqueue(
            new MockResponse().setResponseCode(200)
                .setBody("this is not valid json")
                .addHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters())
        );

        var fetcher = createFetcher(SCOPES);
        var future = new TestPlainActionFuture<CachedToken>();
        fetcher.fetch(future);
        var thrown = expectThrows(ElasticsearchException.class, () -> future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));

        assertThat(
            thrown.getMessage(),
            is(Strings.format("Failed to retrieve access token for request for inference id [%s]", INFERENCE_ID))
        );
        assertNotNull(thrown.getCause());
    }

    public void testFetch_ReadTimeout_FailsWhenServerDoesNotRespond() throws Exception {
        // A 1ms read timeout will fire before the server sends anything after the delay.
        var settings = Settings.builder().put(OAuth2ClusterSettings.READ_TIMEOUT.getKey(), TimeValue.timeValueMillis(1)).build();
        var oauth2ClusterSettings = new OAuth2ClusterSettings(settings, mockClusterService(settings));

        webServer.enqueue(
            new MockResponse().setResponseCode(200)
                .setBody(SUCCESS_BODY)
                .setBeforeReplyDelay(TimeValue.timeValueSeconds(5))
                .addHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters())
        );

        var fetcher = new OAuth2TokenFetcher(
            INFERENCE_ID,
            webServer.getUri(TOKEN_PATH),
            CLIENT_ID,
            CLIENT_SECRET,
            SCOPES,
            threadPool,
            oauth2ClusterSettings
        );
        var future = new TestPlainActionFuture<CachedToken>();
        fetcher.fetch(future);
        var thrown = expectThrows(ElasticsearchException.class, () -> future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));

        assertThat(
            thrown.getMessage(),
            containsString(Strings.format("Failed to retrieve access token for request for inference id [%s]", INFERENCE_ID))
        );
        assertThat(thrown.getCause().getMessage(), is("Read timed out"));
    }

    private OAuth2TokenFetcher createFetcher(List<String> scopes) throws Exception {
        return new OAuth2TokenFetcher(
            INFERENCE_ID,
            webServer.getUri(TOKEN_PATH),
            CLIENT_ID,
            CLIENT_SECRET,
            scopes,
            threadPool,
            mockOAuth2ClusterSettings()
        );
    }

    private OAuth2TokenFetcher createFetcher(List<String> scopes, Clock clock) throws Exception {
        return new OAuth2TokenFetcher(
            INFERENCE_ID,
            webServer.getUri(TOKEN_PATH),
            CLIENT_ID,
            CLIENT_SECRET,
            scopes,
            threadPool,
            mockOAuth2ClusterSettings(),
            clock
        );
    }

    private static MockResponse successResponse() {
        return new MockResponse().setResponseCode(200)
            .setBody(SUCCESS_BODY)
            .addHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaTypeWithoutParameters());
    }
}
