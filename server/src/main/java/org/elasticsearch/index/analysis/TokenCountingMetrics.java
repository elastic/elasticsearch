/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

/**
 * Metrics for token counting during indexing. Records the number of tokens produced
 * per field value as a histogram, enabling observation of the token count distribution
 * across the fleet.
 */
public class TokenCountingMetrics {

    public static final String FIELD_TOKEN_COUNT = "es.indexing.field.token_count.histogram";
    public static final TokenCountingMetrics NOOP = new TokenCountingMetrics(MeterRegistry.NOOP);

    private final LongHistogram tokenCountHistogram;

    public TokenCountingMetrics(MeterRegistry meterRegistry) {
        this.tokenCountHistogram = meterRegistry.registerLongHistogram(
            FIELD_TOKEN_COUNT,
            "Number of tokens produced per field value during indexing",
            "tokens"
        );
    }

    /**
     * Records the number of tokens produced for a single field value.
     */
    public void recordTokenCount(int count) {
        tokenCountHistogram.record(count);
    }
}
