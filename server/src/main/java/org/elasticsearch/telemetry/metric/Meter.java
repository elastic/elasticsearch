/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

public interface Meter {
    <T> DoubleCounter registerDoubleCounter(String name, String description, String unit);

    DoubleCounter getDoubleCounter(String name);

    <T> DoubleUpDownCounter registerDoubleUpDownCounter(String name, String description, String unit);

    DoubleUpDownCounter getDoubleUpDownCounter(String name);

    <T> DoubleGauge registerDoubleGauge(String name, String description, String unit);

    DoubleGauge getDoubleGauge(String name);

    <T> DoubleHistogram registerDoubleHistogram(String name, String description, String unit);

    DoubleHistogram getDoubleHistogram(String name);

    <T> LongCounter registerLongCounter(String name, String description, String unit);

    LongCounter getLongCounter(String name);

    <T> LongUpDownCounter registerLongUpDownCounter(String name, String description, String unit);

    LongUpDownCounter getLongUpDownCounter(String name);

    <T> LongGauge registerLongGauge(String name, String description, String unit);

    LongGauge getLongGauge(String name);

    <T> LongHistogram registerLongHistogram(String name, String description, String unit);

    LongHistogram getLongHistogram(String name);

    Meter NOOP = new Meter() {
        @Override
        public <T> DoubleCounter registerDoubleCounter(String name, String description, String unit) {
            return DoubleCounter.NOOP;
        }

        @Override
        public DoubleCounter getDoubleCounter(String name) {
            return DoubleCounter.NOOP;
        }

        public <T> DoubleUpDownCounter registerDoubleUpDownCounter(String name, String description, String unit) {
            return DoubleUpDownCounter.NOOP;
        }

        @Override
        public DoubleUpDownCounter getDoubleUpDownCounter(String name) {
            return DoubleUpDownCounter.NOOP;
        }

        @Override
        public <T> DoubleGauge registerDoubleGauge(String name, String description, String unit) {
            return DoubleGauge.NOOP;
        }

        @Override
        public DoubleGauge getDoubleGauge(String name) {
            return DoubleGauge.NOOP;
        }

        @Override
        public <T> DoubleHistogram registerDoubleHistogram(String name, String description, String unit) {
            return DoubleHistogram.NOOP;
        }

        @Override
        public DoubleHistogram getDoubleHistogram(String name) {
            return DoubleHistogram.NOOP;
        }

        @Override
        public <T> LongCounter registerLongCounter(String name, String description, String unit) {
            return LongCounter.NOOP;
        }

        @Override
        public LongCounter getLongCounter(String name) {
            return LongCounter.NOOP;
        }

        @Override
        public <T> LongUpDownCounter registerLongUpDownCounter(String name, String description, String unit) {
            return LongUpDownCounter.NOOP;
        }

        @Override
        public LongUpDownCounter getLongUpDownCounter(String name) {
            return LongUpDownCounter.NOOP;
        }

        @Override
        public <T> LongGauge registerLongGauge(String name, String description, String unit) {
            return LongGauge.NOOP;
        }

        @Override
        public LongGauge getLongGauge(String name) {
            return LongGauge.NOOP;
        }

        @Override
        public <T> LongHistogram registerLongHistogram(String name, String description, String unit) {
            return LongHistogram.NOOP;
        }

        @Override
        public LongHistogram getLongHistogram(String name) {
            return LongHistogram.NOOP;
        }
    };
}
