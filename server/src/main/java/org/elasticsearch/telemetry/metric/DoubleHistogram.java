/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.telemetry.MetricName;

import java.util.Map;

public interface DoubleHistogram extends Instrument {
    void record(double value);

    void record(double value, Map<String, Object> attributes);

    void record(double value, Map<String, Object> attributes, ThreadContext threadContext);

    DoubleHistogram NOOP = new DoubleHistogram() {
        @Override
        public MetricName getName() {
            return null;
        }

        @Override
        public void record(double value) {

        }

        @Override
        public void record(double value, Map<String, Object> attributes) {

        }

        @Override
        public void record(double value, Map<String, Object> attributes, ThreadContext threadContext) {

        }
    };
}
