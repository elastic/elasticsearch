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
 * A counter that supports decreasing and increasing values.
 * Useful for capturing the number of requests in a queue.
 */
public interface DoubleUpDownCounter extends Instrument {
    /**
     * Add to the counter
     * @param inc may be negative.
     */
    void add(double inc);

    /**
     * Add to the counter
     * @param inc may be negative.
     * @param attributes key-value pairs to associate with this increment
     */
    void add(double inc, Map<String, Object> attributes);

    /**
     * Noop counter for use in tests
     */
    DoubleUpDownCounter NOOP = new DoubleUpDownCounter() {
        @Override
        public String getName() {
            return "noop";
        }

        @Override
        public void add(double inc) {

        }

        @Override
        public void add(double inc, Map<String, Object> attributes) {

        }
    };
}
