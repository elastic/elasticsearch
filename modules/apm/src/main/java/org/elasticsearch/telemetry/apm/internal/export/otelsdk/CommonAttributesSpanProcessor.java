/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;

/** Adds the attributes that all the spans have in common, such as the names of the cluster and the node. */
public class CommonAttributesSpanProcessor implements SpanProcessor {

    private final ThreadContext threadContext;

    @Nullable
    private volatile String clusterName;
    @Nullable
    private volatile String nodeName;

    public CommonAttributesSpanProcessor(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    public void setClusterName(@Nullable String clusterName) {
        this.clusterName = clusterName;
    }

    public void setNodeName(@Nullable String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public void onStart(Context parentContext, ReadWriteSpan span) {
        final String clusterName = this.clusterName;
        if (clusterName != null) {
            span.setAttribute("es.cluster.name", clusterName);
        }
        final String nodeName = this.nodeName;
        if (nodeName != null) {
            span.setAttribute("es.node.name", nodeName);
        }

        // spans are started by the thread that the request is processed by, so this is the thread context of the request
        final String xOpaqueId = threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
        if (xOpaqueId != null) {
            span.setAttribute("es.x-opaque-id", xOpaqueId);
        }

        final String projectId = threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        if (projectId != null) {
            span.setAttribute("project.id", projectId);
        }
    }

    @Override
    public boolean isStartRequired() {
        return true;
    }

    @Override
    public void onEnd(ReadableSpan span) {}

    @Override
    public boolean isEndRequired() {
        return false;
    }
}
