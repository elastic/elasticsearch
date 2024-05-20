/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

import java.util.Map;

/**
 * Record arbitrary values that are summarized statistically, useful for percentiles and histograms.
 */
public interface DoubleHistogram extends Instrument {
    /**
     * Record a sample for the measured item
     * @param value
     */
    void record(double value);

    /**
     * Record a sample for the measured item
     * @param attributes key-value pairs to associate with the current sample
     */
    void record(double value, Map<String, Object> attributes);

    /**
     * Noop histogram for tests
     */
    DoubleHistogram NOOP = new DoubleHistogram() {
        @Override
        public String getName() {
            return "noop";
        }

        @Override
        public void record(double value) {

        }

        @Override
        public void record(double value, Map<String, Object> attributes) {

        }
    };
}
