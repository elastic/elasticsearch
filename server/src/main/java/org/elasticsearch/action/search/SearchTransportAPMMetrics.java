/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class SearchTransportAPMMetrics {
    private final LongHistogram actionLatencies;

    public SearchTransportAPMMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongHistogram(
                "es.search.phase.shard_transport_actions.latency.histogram",
                "Transport action execution times at the shard level, expressed as a histogram",
                "millis"
            )
        );
    }

    private SearchTransportAPMMetrics(LongHistogram actionLatencies) {
        this.actionLatencies = actionLatencies;
    }

    public static SearchTransportAPMMetrics NOOP = new SearchTransportAPMMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    public LongHistogram getActionLatencies() {
        return actionLatencies;
    }
}
