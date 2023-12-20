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
 * A monotonically increasing metric that uses a double.
 * Useful for capturing the number of bytes received, number of requests, etc.
 */
public interface DoubleCounter extends Instrument {
    /**
     * Add one to the current counter.
     */
    void increment();

    /**
     * Increment the counter.
     * @param inc amount to increment, non-negative
     */
    void incrementBy(double inc);

    /**
     * Increment the counter.
     * @param inc amount to increment, non-negative
     * @param attributes key-value pairs to associate with this increment
     */
    void incrementBy(double inc, Map<String, Object> attributes);

    /**
     * Noop counter for use in tests.
     */
    DoubleCounter NOOP = new DoubleCounter() {
        @Override
        public String getName() {
            return "noop";
        }

        @Override
        public void increment() {

        }

        @Override
        public void incrementBy(double inc) {

        }

        @Override
        public void incrementBy(double inc, Map<String, Object> attributes) {

        }
    };
}
