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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.transform.Transform;

/**
 * A {@link FilterClient} that injects {@code X-Opaque-Id} and {@code X-elastic-product-origin} headers into the
 * {@link ThreadContext} for every request dispatched through it.
 *
 * <p>These headers land in {@link Task#HEADERS_TO_COPY}, so {@code TaskManager} propagates them to every child task
 * (local and cross-node) created for the request. Existing ambient values (e.g. from an enclosing HTTP request that
 * already set {@code X-Opaque-Id}) are left untouched to avoid a duplicate-key exception from
 * {@link ThreadContext#putHeader}.
 *
 * <p>The headers are written into a stored context around each dispatch and restored afterwards; this keeps the
 * caller's thread context clean.
 *
 * <p>Injection is gated by {@link Transform#TRACE_HEADERS_ENABLED_SETTING}, resolved once when the wrapper is created.
 */
public class TransformTraceHeaderClient extends FilterClient {

    private final String opaqueId;
    private final String productOrigin;
    private final boolean enabled;

    /**
     * @param opaqueId      value for {@code X-Opaque-Id}, typically {@code "<nodeId>:transform:<transformId>"}
     * @param productOrigin value for {@code X-elastic-product-origin}, typically the cluster name
     * @param enabled       when {@code false} the headers are not injected and requests pass through unchanged
     */
    TransformTraceHeaderClient(Client in, String opaqueId, String productOrigin, boolean enabled) {
        super(in);
        this.opaqueId = opaqueId;
        this.productOrigin = productOrigin;
        this.enabled = enabled;
    }

    /**
     * Wraps {@code in} so that every request it dispatches is stamped with the transform's trace headers, unless
     * {@link Transform#TRACE_HEADERS_ENABLED_SETTING} is disabled on this node. The opaque id is
     * {@code "<nodeId>:transform:<id>"} and the product origin is the cluster name.
     */
    public static Client create(Client in, ClusterService clusterService, String transformId) {
        String opaqueId = clusterService.localNode().getId() + ":transform:" + transformId;
        String productOrigin = clusterService.getClusterName().value();
        boolean enabled = Transform.TRACE_HEADERS_ENABLED_SETTING.get(clusterService.getSettings());
        return new TransformTraceHeaderClient(in, opaqueId, productOrigin, enabled);
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        if (enabled) {
            ThreadContext tc = threadPool().getThreadContext();
            try (ThreadContext.StoredContext ignore = tc.newStoredContext()) {
                if (tc.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER) == null) {
                    tc.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);
                }
                if (tc.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER) == null) {
                    tc.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, productOrigin);
                }
                super.doExecute(action, request, listener);
            }
        } else {
            super.doExecute(action, request, listener);
        }
    }
}
