/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

/**
 * Container for metering instruments.  Meters with the same name and type (DoubleCounter, etc) can
 * only be registered once.
 * TODO(stu): describe name, unit and description
 */

public interface MeterRegistry {
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
     * @param observer callback to use. This is called once during reporting period.
     *                 Must not throw an exception and must be safe to call from different threads.
     * @return the registered meter.
     */
    default DoubleGauge registerDoubleGauge(String name, String description, String unit, Supplier<DoubleWithAttributes> observer) {
        return registerDoublesGauge(name, description, unit, () -> Collections.singleton(observer.get()));
    }

    /**
     * Register a {@link DoubleGauge}.  The returned object may be reused.
     * @param name name of the gauge
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param observer callback to use. This is called once during reporting period.
     *                 Must not throw an exception and must be safe to call from different threads.
     * @return the registered meter.
     */
    DoubleGauge registerDoublesGauge(String name, String description, String unit, Supplier<Collection<DoubleWithAttributes>> observer);

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
     * Register a {@link LongAsyncCounter} with an asynchronous callback.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param observer a callback to provide a metric value upon observation (metric interval)
     */
    default LongAsyncCounter registerLongAsyncCounter(String name, String description, String unit, Supplier<LongWithAttributes> observer) {
        return registerLongsAsyncCounter(name, description, unit, () -> Collections.singleton(observer.get()));
    }

    /**
     * Register a {@link LongAsyncCounter} with an asynchronous callback.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param observer a callback to provide a metric values upon observation (metric interval)
     */
    LongAsyncCounter registerLongsAsyncCounter(
        String name,
        String description,
        String unit,
        Supplier<Collection<LongWithAttributes>> observer
    );

    /**
     * Retrieved a previously registered {@link LongAsyncCounter}.
     * @param name name of the counter
     * @return the registered meter.
     */
    LongAsyncCounter getLongAsyncCounter(String name);

    /**
     * Register a {@link DoubleAsyncCounter} with an asynchronous callback.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param observer a callback to provide a metric value upon observation (metric interval)
     */
    default DoubleAsyncCounter registerDoubleAsyncCounter(
        String name,
        String description,
        String unit,
        Supplier<DoubleWithAttributes> observer
    ) {
        return registerDoublesAsyncCounter(name, description, unit, () -> Collections.singleton(observer.get()));
    }

    /**
     * Register a {@link DoubleAsyncCounter} with an asynchronous callback.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param observer a callback to provide a metric values upon observation (metric interval)
     */
    DoubleAsyncCounter registerDoublesAsyncCounter(
        String name,
        String description,
        String unit,
        Supplier<Collection<DoubleWithAttributes>> observer
    );

    /**
     * Retrieved a previously registered {@link DoubleAsyncCounter}.
     * @param name name of the counter
     * @return the registered meter.
     */
    DoubleAsyncCounter getDoubleAsyncCounter(String name);

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
     * @param observer callback to use. This is called once during reporting period.
     *                 Must not throw an exception and must be safe to call from different threads.
     * @return the registered meter.
     */
    default LongGauge registerLongGauge(String name, String description, String unit, Supplier<LongWithAttributes> observer) {
        return registerLongsGauge(name, description, unit, () -> Collections.singleton(observer.get()));
    }

    /**
     * Register a {@link LongGauge}.  The returned object may be reused.
     * @param name name of the gauge
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param observer callback to use. This is called once during reporting period.
     *                 Must not throw an exception and must be safe to call from different threads.
     * @return the registered meter.
     */
    LongGauge registerLongsGauge(String name, String description, String unit, Supplier<Collection<LongWithAttributes>> observer);

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
    MeterRegistry NOOP = new MeterRegistry() {
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
        public DoubleGauge registerDoublesGauge(
            String name,
            String description,
            String unit,
            Supplier<Collection<DoubleWithAttributes>> observer
        ) {
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
        public LongAsyncCounter registerLongsAsyncCounter(
            String name,
            String description,
            String unit,
            Supplier<Collection<LongWithAttributes>> observer
        ) {
            return LongAsyncCounter.NOOP;
        }

        @Override
        public LongAsyncCounter getLongAsyncCounter(String name) {
            return LongAsyncCounter.NOOP;
        }

        @Override
        public DoubleAsyncCounter registerDoublesAsyncCounter(
            String name,
            String description,
            String unit,
            Supplier<Collection<DoubleWithAttributes>> observer
        ) {
            return DoubleAsyncCounter.NOOP;
        }

        @Override
        public DoubleAsyncCounter getDoubleAsyncCounter(String name) {
            return DoubleAsyncCounter.NOOP;
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
        public LongGauge registerLongsGauge(
            String name,
            String description,
            String unit,
            Supplier<Collection<LongWithAttributes>> observer
        ) {
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
