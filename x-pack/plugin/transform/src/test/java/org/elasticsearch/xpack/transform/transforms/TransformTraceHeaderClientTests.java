/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;

import java.util.concurrent.atomic.AtomicReference;

public class TransformTraceHeaderClientTests extends ESTestCase {

    public void testHeadersInjectedWhenAbsent() throws Exception {
        final String opaqueId = "node1:transform:my-transform";
        final String productOrigin = "my-cluster";

        AtomicReference<String> capturedOpaqueId = new AtomicReference<>();
        AtomicReference<String> capturedProductOrigin = new AtomicReference<>();

        try (var threadPool = createThreadPool()) {
            var delegate = new NoOpClient(threadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    capturedOpaqueId.set(threadPool().getThreadContext().getHeader(Task.X_OPAQUE_ID_HTTP_HEADER));
                    capturedProductOrigin.set(threadPool().getThreadContext().getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER));
                    listener.onResponse(null);
                }
            };

            var client = new TransformTraceHeaderClient(delegate, opaqueId, productOrigin, true);
            client.execute(TransportSearchAction.TYPE, new SearchRequest(), ActionListener.noop());

            assertEquals(opaqueId, capturedOpaqueId.get());
            assertEquals(productOrigin, capturedProductOrigin.get());

            // headers are restored after dispatch — caller's context is clean
            assertNull(threadPool.getThreadContext().getHeader(Task.X_OPAQUE_ID_HTTP_HEADER));
            assertNull(threadPool.getThreadContext().getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER));
        }
    }

    public void testExistingOpaqueIdIsNotOverwritten() throws Exception {
        final String existingOpaqueId = "existing-opaque-id";
        final String opaqueId = "node1:transform:my-transform";
        final String productOrigin = "my-cluster";

        AtomicReference<String> capturedOpaqueId = new AtomicReference<>();

        try (var threadPool = createThreadPool()) {
            // pre-set an ambient X-Opaque-Id in the thread context
            threadPool.getThreadContext().putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, existingOpaqueId);

            var delegate = new NoOpClient(threadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    capturedOpaqueId.set(threadPool().getThreadContext().getHeader(Task.X_OPAQUE_ID_HTTP_HEADER));
                    listener.onResponse(null);
                }
            };

            var client = new TransformTraceHeaderClient(delegate, opaqueId, productOrigin, true);
            // must not throw "value for key [...] already present"
            client.execute(TransportSearchAction.TYPE, new SearchRequest(), ActionListener.noop());

            // existing value preserved, not overwritten
            assertEquals(existingOpaqueId, capturedOpaqueId.get());
        }
    }

    public void testHeadersNotInjectedWhenDisabled() throws Exception {
        final String opaqueId = "node1:transform:my-transform";
        final String productOrigin = "my-cluster";

        AtomicReference<String> capturedOpaqueId = new AtomicReference<>();
        AtomicReference<String> capturedProductOrigin = new AtomicReference<>();

        try (var threadPool = createThreadPool()) {
            var delegate = new NoOpClient(threadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    capturedOpaqueId.set(threadPool().getThreadContext().getHeader(Task.X_OPAQUE_ID_HTTP_HEADER));
                    capturedProductOrigin.set(threadPool().getThreadContext().getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER));
                    listener.onResponse(null);
                }
            };

            // enabled == false -> no headers injected
            var client = new TransformTraceHeaderClient(delegate, opaqueId, productOrigin, false);
            client.execute(TransportSearchAction.TYPE, new SearchRequest(), ActionListener.noop());

            assertNull(capturedOpaqueId.get());
            assertNull(capturedProductOrigin.get());
        }
    }
}
