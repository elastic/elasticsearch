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
    <T> DoubleCounter registerDoubleCounter(MetricName name, String description, T unit);

    <T> DoubleUpDownCounter registerDoubleUpDownCounter(MetricName name, String description, T unit);

    <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit);

    <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit);

    <T> LongCounter registerLongCounter(MetricName name, String description, T unit);

    <T> LongUpDownCounter registerLongUpDownCounter(MetricName name, String description, T unit);

    <T> LongGauge registerLongGauge(MetricName name, String description, T unit);

    <T> LongHistogram registerLongHistogram(MetricName name, String description, T unit);

    Metric NOOP = new Metric() {
        @Override
        public <T> DoubleCounter registerDoubleCounter(MetricName name, String description, T unit) {
            return DoubleCounter.NOOP;
        }

        public <T> DoubleUpDownCounter registerDoubleUpDownCounter(MetricName name, String description, T unit) {
            return DoubleUpDownCounter.NOOP;
        }

        @Override
        public <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit) {
            return DoubleGauge.NOOP;
        }

        @Override
        public <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit) {
            return DoubleHistogram.NOOP;
        }

        @Override
        public <T> LongCounter registerLongCounter(MetricName name, String description, T unit) {
            return LongCounter.NOOP;
        }

        public <T> LongUpDownCounter registerLongUpDownCounter(MetricName name, String description, T unit) {
            return LongUpDownCounter.NOOP;
        }

        @Override

        public <T> LongGauge registerLongGauge(MetricName name, String description, T unit) {
            return LongGauge.NOOP;
        }

        @Override

        public <T> LongHistogram registerLongHistogram(MetricName name, String description, T unit) {
            return LongHistogram.NOOP;
        }
    };
}
