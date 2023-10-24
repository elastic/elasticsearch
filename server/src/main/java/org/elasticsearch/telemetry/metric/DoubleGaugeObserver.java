/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

import java.util.function.Supplier;

/**
 * Record non-additive double values. eg number of running threads, current load
 */
public interface DoubleGaugeObserver extends Instrument, AutoCloseable {
    void setObserver(Supplier<DoubleAttributes> observer);

    /**
     * Noop gauge for tests
     */
    DoubleGaugeObserver NOOP = new DoubleGaugeObserver() {
        @Override
        public String getName() {
            return "noop";
        }

        @Override
        public void setObserver(Supplier<DoubleAttributes> observer) {

        }

        @Override
        public void close() throws Exception {

        }
    };
}
