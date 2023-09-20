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

public interface LongCounter extends Instrument {
    void increment();

    void incrementBy(long inc);

    void incrementBy(long inc, Map<String, Object> attributes);

    void incrementBy(double inc, Map<String, Object> attributes, ThreadContext threadContext);

    LongCounter NOOP = new LongCounter() {
        @Override
        public MetricName getName() {
            return null;
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

        @Override
        public void incrementBy(double inc, Map<String, Object> attributes, ThreadContext threadContext) {

        }
    };
}
