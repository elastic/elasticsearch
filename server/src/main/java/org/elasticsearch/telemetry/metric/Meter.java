/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

/**
 * Container for metering instruments.  Meters with the same name and type (DoubleCounter, etc) can
 * only be registered once.
 * TODO(stu): describe name, unit and description
 */
public interface Meter {
    /**
     * Register a {@link DoubleCounter}.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    DoubleCounter registerDoubleCounter(String name, String description, String unit);

    /**
     * Retrieved a previously registered {@link DoubleCounter}.
     * @param name name of the counter
     * @return the registered meter.
     */
    DoubleCounter getDoubleCounter(String name);

    /**
     * Register a {@link DoubleUpDownCounter}.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    DoubleUpDownCounter registerDoubleUpDownCounter(String name, String description, String unit);

    /**
     * Retrieved a previously registered {@link DoubleUpDownCounter}.
     * @param name name of the counter
     * @return the registered meter.
     */
    DoubleUpDownCounter getDoubleUpDownCounter(String name);

    /**
     * Register a {@link DoubleGauge}.  The returned object may be reused.
     * @param name name of the gauge
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    DoubleGauge registerDoubleGauge(String name, String description, String unit);

    /**
     * Retrieved a previously registered {@link DoubleGauge}.
     * @param name name of the gauge
     * @return the registered meter.
     */
    DoubleGauge getDoubleGauge(String name);

    /**
     * Register a {@link DoubleHistogram}.  The returned object may be reused.
     * @param name name of the histogram
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    DoubleHistogram registerDoubleHistogram(String name, String description, String unit);

    /**
     * Retrieved a previously registered {@link DoubleHistogram}.
     * @param name name of the histogram
     * @return the registered meter.
     */
    DoubleHistogram getDoubleHistogram(String name);

    /**
     * Register a {@link LongCounter}.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    LongCounter registerLongCounter(String name, String description, String unit);

    /**
     * Retrieved a previously registered {@link LongCounter}.
     * @param name name of the counter
     * @return the registered meter.
     */
    LongCounter getLongCounter(String name);

    /**
     * Register a {@link LongUpDownCounter}.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    LongUpDownCounter registerLongUpDownCounter(String name, String description, String unit);

    /**
     * Retrieved a previously registered {@link LongUpDownCounter}.
     * @param name name of the counter
     * @return the registered meter.
     */
    LongUpDownCounter getLongUpDownCounter(String name);

    /**
     * Register a {@link LongGauge}.  The returned object may be reused.
     * @param name name of the gauge
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    LongGauge registerLongGauge(String name, String description, String unit);

    /**
     * Retrieved a previously registered {@link LongGauge}.
     * @param name name of the gauge
     * @return the registered meter.
     */
    LongGauge getLongGauge(String name);

    /**
     * Register a {@link LongHistogram}.  The returned object may be reused.
     * @param name name of the histogram
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    LongHistogram registerLongHistogram(String name, String description, String unit);

    /**
     * Retrieved a previously registered {@link LongHistogram}.
     * @param name name of the histogram
     * @return the registered meter.
     */
    LongHistogram getLongHistogram(String name);

    /**
     * Noop implementation for tests
     */
    Meter NOOP = new Meter() {
        @Override
        public DoubleCounter registerDoubleCounter(String name, String description, String unit) {
            return DoubleCounter.NOOP;
        }

        @Override
        public DoubleCounter getDoubleCounter(String name) {
            return DoubleCounter.NOOP;
        }

        public DoubleUpDownCounter registerDoubleUpDownCounter(String name, String description, String unit) {
            return DoubleUpDownCounter.NOOP;
        }

        @Override
        public DoubleUpDownCounter getDoubleUpDownCounter(String name) {
            return DoubleUpDownCounter.NOOP;
        }

        @Override
        public DoubleGauge registerDoubleGauge(String name, String description, String unit) {
            return DoubleGauge.NOOP;
        }

        @Override
        public DoubleGauge getDoubleGauge(String name) {
            return DoubleGauge.NOOP;
        }

        @Override
        public DoubleHistogram registerDoubleHistogram(String name, String description, String unit) {
            return DoubleHistogram.NOOP;
        }

        @Override
        public DoubleHistogram getDoubleHistogram(String name) {
            return DoubleHistogram.NOOP;
        }

        @Override
        public LongCounter registerLongCounter(String name, String description, String unit) {
            return LongCounter.NOOP;
        }

        @Override
        public LongCounter getLongCounter(String name) {
            return LongCounter.NOOP;
        }

        @Override
        public LongUpDownCounter registerLongUpDownCounter(String name, String description, String unit) {
            return LongUpDownCounter.NOOP;
        }

        @Override
        public LongUpDownCounter getLongUpDownCounter(String name) {
            return LongUpDownCounter.NOOP;
        }

        @Override
        public LongGauge registerLongGauge(String name, String description, String unit) {
            return LongGauge.NOOP;
        }

        @Override
        public LongGauge getLongGauge(String name) {
            return LongGauge.NOOP;
        }

        @Override
        public LongHistogram registerLongHistogram(String name, String description, String unit) {
            return LongHistogram.NOOP;
        }

        @Override
        public LongHistogram getLongHistogram(String name) {
            return LongHistogram.NOOP;
        }
    };
}
