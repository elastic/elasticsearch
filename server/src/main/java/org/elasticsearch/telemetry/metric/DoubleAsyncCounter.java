/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.metric;

/**
 * A monotonically increasing double based on a callback.
 */
public interface DoubleAsyncCounter extends Instrument, AutoCloseable {

    /**
     * Closing this instrument stops it from recording measurements and removes it from the {@link MeterRegistry}.
     */
    @Override
    void close();

    /**
     * Noop counter for use in tests.
     */
    DoubleAsyncCounter NOOP = new DoubleAsyncCounter() {
        @Override
        public void close() {

        }

        @Override
        public String getName() {
            return "noop";
        }

    };
}
