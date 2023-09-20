/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

import org.elasticsearch.telemetry.MetricName;

public interface Metric {
    <T> Counter registerCounter(MetricName name, String description, T unit);

    <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit);

    <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit);

    Metric NOOP = new Metric() {
        @Override
        public <T> Counter registerCounter(MetricName name, String description, T unit) {
            return Counter.NOOP;
        }

        @Override
        public <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit) {
            return DoubleGauge.NOOP;
        }

        @Override
        public <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit) {
            return DoubleHistogram.NOOP;
        }
    };
}
