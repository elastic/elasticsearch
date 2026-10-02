/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.secrets;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProject;
import org.elasticsearch.xpack.inference.common.oauth2.CachedToken;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2TokenSupplier;
import org.elasticsearch.xpack.inference.common.oauth2.TokenCache;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class OpenAiOAuth2ApplierTests extends ESTestCase {

    private static final String INFERENCE_ID = "inference-id";
    private static final String BEARER_VALUE = "bearer-12345";
    private static final String URL = "http://example.com/embeddings";
    private static final String FAILURE_MESSAGE = "supplier should not be invoked";

    public void testApplyTo_StampsBearerHeaderFromCache() {
        var cache = recordingCacheReturning(new CachedToken(BEARER_VALUE, Instant.now().plusSeconds(60)));
        var applier = new OpenAiOAuth2Applier(INFERENCE_ID, cache, listener -> fail(FAILURE_MESSAGE));

        var request = new HttpGet(URL);
        var future = new PlainActionFuture<HttpRequestBase>();
        applier.applyTo(request, future);

        var resultRequest = future.actionGet();
        assertThat(resultRequest.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), equalTo("Bearer " + BEARER_VALUE));
    }

    public void testApplyTo_PropagatesCacheFailure() {
        var failureMessage = "idp failure";
        var failure = new ElasticsearchException(failureMessage);
        var cache = new TokenCache() {
            @Override
            public void getToken(String inferenceId, OAuth2TokenSupplier supplier, ActionListener<CachedToken> listener) {
                listener.onFailure(failure);
            }

            @Override
            public void invalidate(InferenceIdAndProject key, ActionListener<Void> listener) {
                listener.onResponse(null);
            }

            @Override
            public void invalidateOnlLocalNode(String inferenceId) {}
        };
        var applier = new OpenAiOAuth2Applier(INFERENCE_ID, cache, listener -> fail(FAILURE_MESSAGE));

        var request = new HttpGet(URL);
        var future = new PlainActionFuture<HttpRequestBase>();
        applier.applyTo(request, future);

        var thrown = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(thrown.getMessage(), containsString(failureMessage));
    }

    public void testOnAuthenticationFailure_InvalidatesLocalToken() {
        var invalidatedIds = new ArrayList<String>();
        var cache = new TokenCache() {
            @Override
            public void getToken(String inferenceId, OAuth2TokenSupplier supplier, ActionListener<CachedToken> listener) {
                fail(FAILURE_MESSAGE);
            }

            @Override
            public void invalidate(InferenceIdAndProject key, ActionListener<Void> listener) {
                fail(FAILURE_MESSAGE);
            }

            @Override
            public void invalidateOnlLocalNode(String inferenceId) {
                invalidatedIds.add(inferenceId);
            }
        };
        var applier = new OpenAiOAuth2Applier(INFERENCE_ID, cache, listener -> fail(FAILURE_MESSAGE));

        applier.onAuthenticationFailure();

        assertThat(invalidatedIds, equalTo(List.of(INFERENCE_ID)));
    }

    private static TokenCache recordingCacheReturning(CachedToken token) {
        return new TokenCache() {
            @Override
            public void getToken(String inferenceId, OAuth2TokenSupplier supplier, ActionListener<CachedToken> listener) {
                assertThat(inferenceId, is(INFERENCE_ID));
                listener.onResponse(token);
            }

            @Override
            public void invalidate(InferenceIdAndProject key, ActionListener<Void> listener) {
                listener.onResponse(null);
            }

            @Override
            public void invalidateOnlLocalNode(String inferenceId) {}
        };
    }
}
