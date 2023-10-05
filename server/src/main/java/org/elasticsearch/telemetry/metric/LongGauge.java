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
 * Record non-additive long values.
 */
public interface LongGauge extends Instrument {

    /**
     * Record the current value of the measured item.
     * @param value
     */
    void record(long value);

    /**
     * Record the current value
     * @param attributes key-value pairs to associate with the current measurement
     */
    void record(long value, Map<String, Object> attributes);

    /**
     * Noop gauge for tests
     */
    LongGauge NOOP = new LongGauge() {
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
