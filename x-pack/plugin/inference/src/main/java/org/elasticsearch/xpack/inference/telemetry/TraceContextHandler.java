/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.tasks.Task;

public record TraceContextHandler(TraceContext traceContext) {

    public void propagateTraceContext(HttpPost httpPost) {
        if (traceContext == null) {
            return;
        }

        var traceParent = traceContext.traceParent();
        var traceState = traceContext.traceState();

        if (traceParent != null) {
            httpPost.setHeader(Task.TRACE_PARENT_HTTP_HEADER, traceParent);
        }

        if (traceState != null) {
            httpPost.setHeader(Task.TRACE_STATE, traceState);
        }
    }
}
