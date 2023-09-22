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

public interface LongHistogram extends Instrument {
    void record(long value);

    void record(long value, Map<String, Object> attributes);

    void record(long value, Map<String, Object> attributes, ThreadContext threadContext);

    LongHistogram NOOP = new LongHistogram() {
        @Override
        public MetricName getName() {
            return null;
        }

        @Override
        public void record(long value) {

        }

        @Override
        public void record(long value, Map<String, Object> attributes) {

        }

        @Override
        public void record(long value, Map<String, Object> attributes, ThreadContext threadContext) {

        }
    };
}
