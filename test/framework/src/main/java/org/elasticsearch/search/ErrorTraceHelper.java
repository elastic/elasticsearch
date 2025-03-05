/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Utilities around testing the `error_trace` message header in search.
 */
public enum ErrorTraceHelper {
    ;

    public static BooleanSupplier setupErrorTraceListener(InternalTestCluster internalCluster) {
        final AtomicBoolean transportMessageHasStackTrace = new AtomicBoolean(false);
        internalCluster.getDataNodeInstances(TransportService.class).forEach(ts -> ts.addMessageListener(new TransportMessageListener() {
            @Override
            public void onResponseSent(long requestId, String action, Exception error) {
                TransportMessageListener.super.onResponseSent(requestId, action, error);
                if (action.startsWith("indices:data/read/search")) {
                    Optional<Throwable> throwable = ExceptionsHelper.unwrapCausesAndSuppressed(error, t -> t.getStackTrace().length > 0);
                    transportMessageHasStackTrace.set(throwable.isPresent());
                }
            }
        }));
        return transportMessageHasStackTrace::get;
    }
}
