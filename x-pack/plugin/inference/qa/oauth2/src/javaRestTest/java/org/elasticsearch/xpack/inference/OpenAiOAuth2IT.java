/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.nimbusds.jose.JOSEObjectType;

import org.apache.http.HttpHeaders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end REST integration test for OpenAI OAuth2 client-credentials authentication.
 *
 * <p>Starts an in-process {@link no.nav.security.mock.oauth2.MockOAuth2Server} as the IdP and an ES {@link org.elasticsearch.test.http.MockWebServer}
 * standing in for the OpenAI API. Creates a {@code text_embedding} inference endpoint pointing at
 * both mocks, runs inference, and asserts that every outbound OpenAI request carries a
 * {@code Bearer} token obtained from the IdP.
 *
 * <p>Inherits its cluster {@code @ClassRule}, REST client auth, and helper methods
 * ({@code putModel}, {@code infer}, {@code updateEndpoint}, {@code deleteModel},
 * {@code assertNonEmptyInferenceResults}) from {@link InferenceBaseRestTest}.
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
public class OpenAiOAuth2IT extends InferenceBaseRestTest {

    private static final String ENDPOINT_ID = "openai-oauth2";
    private static final String CLIENT_ID = "client";
    private static final String ISSUER_ID = "default";
    private static final String SUBJECT = "subject"; // ignored for client_credentials (client id is used)
    private static final Duration TOKEN_EXPIRY = Duration.ofMinutes(30);
    /**
     * JSON float-array shape accepted by {@code OpenAiEmbeddingsResponseEntity}
     * (see {@code OpenAiEmbeddingsResponseEntityTests} for the alternative base64 shape).
     */
    private static final String EMBEDDINGS_RESPONSE = """
        {
          "object": "list",
          "data": [{"object":"embedding","index":0,"embedding":[0.014539449,-0.015288644]}],
          "model": "text-embedding-3-small",
          "usage": {"prompt_tokens":1,"total_tokens":1}
        }
        """;

    private static MockOAuth2Server idp;
    private static MockWebServer openai;

    @BeforeClass
    public static void startServers() throws Exception {
        idp = new MockOAuth2Server();
        idp.start();
        openai = new MockWebServer();
        openai.start();
    }

    @AfterClass
    public static void stopServers() {
        idp.shutdown();
        openai.close();
    }

    @After
    public void clearMockRequests() {
        openai.clearRequests();
    }

    /**
     * Verifies that both the validation call made during endpoint creation and the explicit
     * inference call are authenticated with a {@code Bearer} token from the IdP.
     *
     * <p>OpenAI endpoint creation triggers a validation inference call to the configured
     * {@code url}, so two responses must be enqueued before calling {@code putModel}.
     */
    public void testCreate_WithOAuth2_SendsBearerToOpenAiOnValidationAndInference() throws Exception {
        var tokenEndpoint = idp.tokenEndpointUrl(ISSUER_ID).toString();
        var openaiUrl = Strings.format("http://localhost:%d/v1/embeddings", openai.getPort());

        // Respond to the validation call during PUT, then to the explicit inference call.
        idp.enqueueCallback(tokenCallback());
        openai.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDINGS_RESPONSE));
        idp.enqueueCallback(tokenCallback());
        openai.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDINGS_RESPONSE));

        putModel(ENDPOINT_ID, buildModelConfig(openaiUrl, tokenEndpoint, "secret"), TaskType.TEXT_EMBEDDING);
        var result = infer(ENDPOINT_ID, TaskType.TEXT_EMBEDDING, List.of("hello"));

        assertNonEmptyInferenceResults(result, 1, TaskType.TEXT_EMBEDDING);
        assertThat(openai.requests(), hasSize(2)); // validation (PUT) + inference (POST)
        openai.requests().forEach(req -> {
            assertNotNull(req.getHeader(HttpHeaders.AUTHORIZATION));
            assertThat(req.getHeader(HttpHeaders.AUTHORIZATION), containsString("Bearer "));
        });

        deleteModel(ENDPOINT_ID, TaskType.TEXT_EMBEDDING);
    }

    /**
     * Verifies that rotating {@code client_secret} via {@code _update} does not break inference:
     * all four OpenAI requests (validation at create, infer before update, validation at update,
     * infer after update) carry a valid {@code Bearer} token.
     *
     * <p>The real {@code OAuth2TokenCache} invalidates the cached token when the model is updated,
     * so the post-update inference triggers a fresh token fetch with the new secret.
     */
    public void testUpdate_ClientSecret_KeepsEndpointWorkingWithBearer() throws Exception {
        var tokenEndpoint = idp.tokenEndpointUrl(ISSUER_ID).toString();
        var openaiUrl = Strings.format("http://localhost:%d/v1/embeddings", openai.getPort());

        // Create — triggers a validation call.
        idp.enqueueCallback(tokenCallback());
        openai.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDINGS_RESPONSE));
        putModel(ENDPOINT_ID, buildModelConfig(openaiUrl, tokenEndpoint, "secret-a"), TaskType.TEXT_EMBEDDING);

        // Infer #1.
        idp.enqueueCallback(tokenCallback());
        openai.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDINGS_RESPONSE));
        infer(ENDPOINT_ID, TaskType.TEXT_EMBEDDING, List.of("hello"));

        // Rotate the client secret. Credentials go in service_settings on the wire;
        // secret_settings is only the internal persisted form.
        // _update also triggers a validation call to the endpoint, so enqueue a response for it.
        idp.enqueueCallback(tokenCallback());
        openai.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDINGS_RESPONSE));
        updateEndpoint(ENDPOINT_ID, """
            {
                "service_settings": {
                    "client_secret": "secret-b"
                }
            }
            """, TaskType.TEXT_EMBEDDING);

        // Infer #2 — must acquire a fresh token with the rotated secret.
        idp.enqueueCallback(tokenCallback());
        openai.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDINGS_RESPONSE));
        infer(ENDPOINT_ID, TaskType.TEXT_EMBEDDING, List.of("world"));

        // Every OpenAI request carried a Bearer: validation@create + infer#1 + validation@update + infer#2.
        assertThat(openai.requests(), hasSize(4));
        openai.requests().forEach(req -> {
            assertNotNull(req.getHeader(HttpHeaders.AUTHORIZATION));
            assertThat(req.getHeader(HttpHeaders.AUTHORIZATION), containsString("Bearer "));
        });

        deleteModel(ENDPOINT_ID, TaskType.TEXT_EMBEDDING);
    }

    /**
     * Builds the PUT /_inference request body for an OpenAI text_embedding endpoint with OAuth2.
     *
     * <p>All credentials (including {@code client_secret}) go in {@code service_settings} on the
     * wire — {@code secret_settings} is only used when reading from the internal persisted index.
     */
    private static String buildModelConfig(String openaiUrl, String tokenEndpoint, String secret) {
        return Strings.format("""
            {
              "service": "openai",
              "service_settings": {
                "model_id": "text-embedding-3-small",
                "url": "%s",
                "client_id": "%s",
                "scopes": ["api"],
                "token_url": "%s",
                "client_secret": "%s"
              }
            }
            """, openaiUrl, CLIENT_ID, tokenEndpoint, secret);
    }

    /**
     * Builds a token callback with an explicit 30-minute expiry.
     * {@code expiry} is the last {@code @JvmOverloads} parameter of
     * {@link DefaultOAuth2TokenCallback}, so Java must pass every preceding argument rather
     * than relying on the library's default of 3600 seconds.
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
