/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

public interface Metric {
    <T> DoubleCounter registerDoubleCounter(MetricName name, String description, T unit);

    DoubleCounter getDoubleCounter(MetricName name);

    <T> DoubleUpDownCounter registerDoubleUpDownCounter(MetricName name, String description, T unit);

    DoubleUpDownCounter getDoubleUpDownCounter(MetricName name);

    <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit);

    DoubleGauge getDoubleGauge(MetricName name);

    <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit);

    DoubleHistogram getDoubleHistogram(MetricName name);

    <T> LongCounter registerLongCounter(MetricName name, String description, T unit);

    LongCounter getLongCounter(MetricName name);

    <T> LongUpDownCounter registerLongUpDownCounter(MetricName name, String description, T unit);

    LongUpDownCounter getLongUpDownCounter(MetricName name);

    <T> LongGauge registerLongGauge(MetricName name, String description, T unit);

    LongGauge getLongGauge(MetricName name);

    <T> LongHistogram registerLongHistogram(MetricName name, String description, T unit);

    LongHistogram getLongHistogram(MetricName name);

    Metric NOOP = new Metric() {
        @Override
        public <T> DoubleCounter registerDoubleCounter(MetricName name, String description, T unit) {
            return DoubleCounter.NOOP;
        }

        @Override
        public DoubleCounter getDoubleCounter(MetricName name) {
            return DoubleCounter.NOOP;
        }

        public <T> DoubleUpDownCounter registerDoubleUpDownCounter(MetricName name, String description, T unit) {
            return DoubleUpDownCounter.NOOP;
        }

        @Override
        public DoubleUpDownCounter getDoubleUpDownCounter(MetricName name) {
            return DoubleUpDownCounter.NOOP;
        }

        @Override
        public <T> DoubleGauge registerDoubleGauge(MetricName name, String description, T unit) {
            return DoubleGauge.NOOP;
        }

        @Override
        public DoubleGauge getDoubleGauge(MetricName name) {
            return DoubleGauge.NOOP;
        }

        @Override
        public <T> DoubleHistogram registerDoubleHistogram(MetricName name, String description, T unit) {
            return DoubleHistogram.NOOP;
        }

        @Override
        public DoubleHistogram getDoubleHistogram(MetricName name) {
            return DoubleHistogram.NOOP;
        }

        @Override
        public <T> LongCounter registerLongCounter(MetricName name, String description, T unit) {
            return LongCounter.NOOP;
        }

        @Override
        public LongCounter getLongCounter(MetricName name) {
            return LongCounter.NOOP;
        }

        @Override
        public <T> LongUpDownCounter registerLongUpDownCounter(MetricName name, String description, T unit) {
            return LongUpDownCounter.NOOP;
        }

        @Override
        public LongUpDownCounter getLongUpDownCounter(MetricName name) {
            return LongUpDownCounter.NOOP;
        }

        @Override
        public <T> LongGauge registerLongGauge(MetricName name, String description, T unit) {
            return LongGauge.NOOP;
        }

        @Override
        public LongGauge getLongGauge(MetricName name) {
            return LongGauge.NOOP;
        }

        @Override
        public <T> LongHistogram registerLongHistogram(MetricName name, String description, T unit) {
            return LongHistogram.NOOP;
        }

        @Override
        public LongHistogram getLongHistogram(MetricName name) {
            return LongHistogram.NOOP;
        }
    };
}
