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

import java.util.ArrayList;
import java.util.List;

public final class HttpServerInstrumentations implements HttpServerInstrumentation {

    private final List<HttpServerInstrumentation> instrumentations;

    public HttpServerInstrumentations(List<HttpServerInstrumentation> instrumentations) {
        this.instrumentations = List.copyOf(instrumentations);
    }

    @Override
    public InstrumentedHttpOperation start(RestRequest request, String matchedRoute) {
        final List<InstrumentedHttpOperation> operations = new ArrayList<>(instrumentations.size());
        for (HttpServerInstrumentation instrumentation : instrumentations) {
            operations.add(instrumentation.start(request, matchedRoute));
        }
        return () -> {
            final List<ThreadContext.StoredContext> storedContexts = new ArrayList<>(operations.size());
            try {
                for (InstrumentedHttpOperation operation : operations) {
                    storedContexts.add(operation.storeInThreadContext());
                }
            } catch (Exception e) {
                closeBackwards(storedContexts);
                throw e;
            }
            return () -> closeBackwards(storedContexts);
        };
    }

    /** Each of the stored contexts restores the state it found, which includes what the ones before it set up, so undo them backwards. */
    private static void closeBackwards(List<ThreadContext.StoredContext> storedContexts) {
        for (ThreadContext.StoredContext storedContext : storedContexts.reversed()) {
            storedContext.close();
        }
    }

    @Override
    public void recordException(RestRequest request, Throwable t) {
        for (HttpServerInstrumentation instrumentation : instrumentations) {
            instrumentation.recordException(request, t);
        }
    }

    @Override
    public Releasable prepareEnd(RestRequest request, RestResponse response) {
        return Releasables.wrap(instrumentations.reversed().stream().map(i -> i.prepareEnd(request, response)).toList());
    }
}
