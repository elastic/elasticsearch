/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

/**
 * Record non-additive double values. eg number of running threads, current load
 */
public interface LongGaugeObserver extends Instrument, AutoCloseable {

    /**
     * Noop gauge for tests
     */
    LongGaugeObserver NOOP = new LongGaugeObserver() {
        @Override
        public String getName() {
            return "noop";
        }

        @Override
        public void close() throws Exception {

        }
    };
}
