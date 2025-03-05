/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.metric;

import java.util.Map;

/**
 * Record arbitrary values that are summarized statistically, useful for percentiles and histograms.
 */
public interface LongHistogram extends Instrument {
    /**
     * Record a sample for the measured item
     * @param value
     */
    void record(long value);

    /**
     * Record a sample for the measured item
     * @param attributes key-value pairs to associate with the current sample
     */
    void record(long value, Map<String, Object> attributes);

    /**
     * Noop histogram for tests
     */
    LongHistogram NOOP = new LongHistogram() {
        @Override
        public String getName() {
            return "noop";
        }

        @Override
        public void record(long value) {

        }

        @Override
        public void record(long value, Map<String, Object> attributes) {

        }
    };
}
