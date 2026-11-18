/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The fetch contexts open on this node, for {@code GET _esql/stats} and the {@value #OPEN_CONTEXTS_GAUGE} gauge. The search
 * stats of the node count them in {@code open_contexts} too, mixed with the contexts of {@code _search}.
 * <p>
 * A context counts from the moment a data node request registers it until its reader closes, whoever frees it: the
 * coordinator, the request itself, the reaper, the removal of its shard or the shutdown of the node.
 */
public final class OpenFetchContexts {
    public static final String OPEN_CONTEXTS_GAUGE = "es.esql.fetch.open_contexts.current";

    private final AtomicLong open = new AtomicLong();

    /**
     * @param meterRegistry gets the gauge, in builds with the fetch phase only
     */
    public OpenFetchContexts(MeterRegistry meterRegistry) {
        if (EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled()) {
            meterRegistry.registerLongAsyncGauge(OPEN_CONTEXTS_GAUGE, "ES|QL fetch contexts open on the node", "unit", open::get);
        }
    }

    /**
     * Counts {@code context} until its reader closes.
     */
    void opened(ReaderContext context) {
        open.incrementAndGet();
        context.addOnClose(open::decrementAndGet);
    }

    public long current() {
        return open.get();
    }
}
