/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;

import java.util.List;

public final class HttpServerInstrumentations implements HttpServerInstrumentation {

    private final List<HttpServerInstrumentation> instrumentations;

    public HttpServerInstrumentations(List<HttpServerInstrumentation> instrumentations) {
        this.instrumentations = List.copyOf(instrumentations);
    }

    @Override
    public void start(ThreadContext threadContext, RestRequest request, String matchedRoute) {
        for (HttpServerInstrumentation instrumentation : instrumentations) {
            instrumentation.start(threadContext, request, matchedRoute);
        }
    }

    @Override
    public void recordException(RestRequest request, Throwable t) {
        for (HttpServerInstrumentation instrumentation : instrumentations) {
            instrumentation.recordException(request, t);
        }
    }

    @Override
    public Releasable prepareEnd(ThreadContext threadContext, RestRequest request, RestResponse response) {
        return Releasables.wrap(instrumentations.reversed().stream().map(i -> i.prepareEnd(threadContext, request, response)).toList());
    }
}
