/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProject;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class NoopTokenCacheTests extends ESTestCase {

    private static final String INFERENCE_ID = "inference-id";

    public void testGetToken_AlwaysInvokesSupplier() {
        var supplierCallCount = new AtomicInteger();
        var token = new CachedToken("bearer-value", Instant.now().plusSeconds(60));
        OAuth2TokenSupplier supplier = listener -> {
            supplierCallCount.incrementAndGet();
            listener.onResponse(token);
        };
        var cache = new NoopTokenCache();

        var future = new PlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future);
        assertThat(future.actionGet(), equalTo(token));

        var future2 = new PlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future2);
        assertThat(future2.actionGet(), equalTo(token));

        assertThat(supplierCallCount.get(), equalTo(2));
    }

    public void testGetToken_PropagatesSupplierFailure() {
        var failure = new RuntimeException("idp down");
        OAuth2TokenSupplier supplier = listener -> listener.onFailure(failure);
        var cache = new NoopTokenCache();

        var future = new PlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future);
        var thrown = expectThrows(RuntimeException.class, future::actionGet);
        assertThat(thrown, equalTo(failure));
    }

    public void testInvalidate_RespondsImmediatelyWithNull() {
        var cache = new NoopTokenCache();
        var future = new PlainActionFuture<Void>();
        var key = new InferenceIdAndProject(INFERENCE_ID, ProjectId.DEFAULT);
        cache.invalidate(key, future);
        assertNull(future.actionGet());
    }
}
