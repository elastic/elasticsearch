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
 * A gauge instrument that synchronously records non-additive double values.
 */
public interface DoubleGauge extends Instrument {

    /**
     * Set the gauge value.
     * @param value current gauge value
     */
    void set(double value);

    /**
     * Set the gauge value.
     * @param value current gauge value
     * @param attributes key-value pairs to associate with this increment
     */
    void set(double value, Map<String, Object> attributes);

    /**
     * Noop counter for use in tests.
     */
    DoubleGauge NOOP = new DoubleGauge() {
        @Override
        public String getName() {
            return "noop";
        }

        @Override
        public void set(double value) {

        }

        @Override
        public void set(double value, Map<String, Object> attributes) {

        }
    };
}
