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
 * A monotonically increasing metric that uses a long.  Useful for integral values such as the number of bytes received,
 * number of requests, etc.
 */
public interface LongCounter extends Instrument {
    /**
     * Add one to the current counter
     */
    void increment();

    /**
     * Increment the counter
     * @param inc amount to increment
     */
    void incrementBy(long inc);

    /**
     * Increment the counter.
     * @param inc amount to increment
     * @param attributes key-value pairs to associate with this increment
     */
    void incrementBy(long inc, Map<String, Object> attributes);

    /**
     * Noop counter for use in tests.
     */
    LongCounter NOOP = new LongCounter() {
        @Override
        public String getName() {
            return "noop";
        }

        @Override
        public void increment() {

        }

        @Override
        public void incrementBy(long inc) {

        }

        @Override
        public void incrementBy(long inc, Map<String, Object> attributes) {

        }
    };
}
