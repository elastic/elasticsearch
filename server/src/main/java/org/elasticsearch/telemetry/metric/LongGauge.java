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
 * A gauge instrument that synchronously records non-additive long values.
 */
public interface LongGauge extends Instrument {

    /**
     * Set the gauge value.
     * @param value current gauge value
     */
    void set(long value);

    /**
     * Set the gauge value.
     * @param value current gauge value
     * @param attributes key-value pairs to associate with this increment
     */
    void set(long value, Map<String, Object> attributes);

    /**
     * Noop counter for use in tests.
     */
    LongGauge NOOP = new LongGauge() {
        @Override
        public String getName() {
            return "noop";
        }

        @Override
        public void set(long value) {

        }

        @Override
        public void set(long value, Map<String, Object> attributes) {

        }
    };
}
