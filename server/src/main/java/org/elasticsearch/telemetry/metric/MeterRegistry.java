/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.metric;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
     * Register a {@link DoubleUpDownCounter}.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    DoubleUpDownCounter registerDoubleUpDownCounter(String name, String description, String unit);

    /**
     * Register a {@link DoubleGauge}.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    DoubleGauge registerDoubleGauge(String name, String description, String unit);

    /**
     * Register a {@link DoubleAsyncGauge}.  The returned object may be reused.
     * @param name name of the gauge
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param observer callback to use. This is called once during reporting period.
     *                 Must not throw an exception and must be safe to call from different threads.
     * @return the registered meter.
     */
    default DoubleAsyncGauge registerDoubleAsyncGauge(
        String name,
        String description,
        String unit,
        Supplier<DoubleWithAttributes> observer
    ) {
        return registerDoublesAsyncGauge(name, description, unit, () -> Collections.singleton(observer.get()));
    }

    /**
     * Register a {@link DoubleAsyncGauge}.  The returned object may be reused.
     * @param name name of the gauge
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param observer callback to use. This is called once during reporting period.
     *                 Must not throw an exception and must be safe to call from different threads.
     * @return the registered meter.
     */
    DoubleAsyncGauge registerDoublesAsyncGauge(
        String name,
        String description,
        String unit,
        Supplier<Collection<DoubleWithAttributes>> observer
    );

    /**
     * Register a {@link DoubleHistogram}.  The returned object may be reused.
     * @param name name of the histogram
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    DoubleHistogram registerDoubleHistogram(String name, String description, String unit);

    /**
     * Register a {@link DoubleHistogram} with explicit bucket boundaries.  The returned object may be reused.
     * Callers that need bucket boundaries tuned to a specific range should prefer this over
     * {@link #registerDoubleHistogram(String, String, String)}, which uses the APM default sqrt(2) ladder.
     * @param name name of the histogram
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param bucketBoundaries explicit upper-inclusive bucket boundaries, in ascending order
     * @return the registered meter.
     */
    DoubleHistogram registerDoubleHistogram(String name, String description, String unit, List<Double> bucketBoundaries);

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
     * Register a {@link LongUpDownCounter}.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    LongUpDownCounter registerLongUpDownCounter(String name, String description, String unit);

    /**
     * Register a {@link LongGauge}.  The returned object may be reused.
     * @param name name of the counter
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    LongGauge registerLongGauge(String name, String description, String unit);

    /**
     * Register a {@link LongAsyncGauge}.  The returned object may be reused.
     * @param name name of the gauge
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param observer callback to use. This is called once during reporting period.
     *                 Must not throw an exception and must be safe to call from different threads.
     * @return the registered meter.
     */
    default LongAsyncGauge registerLongAsyncGauge(String name, String description, String unit, Supplier<LongWithAttributes> observer) {
        return registerLongsAsyncGauge(name, description, unit, () -> Collections.singleton(observer.get()));
    }

    /**
     * Register a {@link LongAsyncGauge}.  The returned object may be reused.
     * @param name name of the gauge
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param observer callback to use. This is called once during reporting period.
     *                 Must not throw an exception and must be safe to call from different threads.
     * @return the registered meter.
     */
    LongAsyncGauge registerLongsAsyncGauge(String name, String description, String unit, Supplier<Collection<LongWithAttributes>> observer);

    /**
     * Register a {@link LongHistogram}.  The returned object may be reused.
     * @param name name of the histogram
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @return the registered meter.
     */
    LongHistogram registerLongHistogram(String name, String description, String unit);

    /**
     * Register a {@link LongHistogram} with explicit bucket boundaries.  The returned object may be reused.
     * Callers that need bucket boundaries tuned to a specific range should prefer this over
     * {@link #registerLongHistogram(String, String, String)}, which uses the APM default sqrt(2) ladder.
     * @param name name of the histogram
     * @param description description of purpose
     * @param unit the unit (bytes, sec, hour)
     * @param bucketBoundaries explicit upper-inclusive bucket boundaries, in ascending order
     * @return the registered meter.
     */
    LongHistogram registerLongHistogram(String name, String description, String unit, List<Long> bucketBoundaries);

    /**
     * Noop implementation for tests
     */
    MeterRegistry NOOP = new MeterRegistry() {
        @Override
        public DoubleCounter registerDoubleCounter(String name, String description, String unit) {
            return DoubleCounter.NOOP;
        }

        public DoubleUpDownCounter registerDoubleUpDownCounter(String name, String description, String unit) {
            return DoubleUpDownCounter.NOOP;
        }

        @Override
        public DoubleGauge registerDoubleGauge(String name, String description, String unit) {
            return DoubleGauge.NOOP;
        }

        @Override
        public DoubleAsyncGauge registerDoublesAsyncGauge(
            String name,
            String description,
            String unit,
            Supplier<Collection<DoubleWithAttributes>> observer
        ) {
            return DoubleAsyncGauge.NOOP;
        }

        @Override
        public DoubleHistogram registerDoubleHistogram(String name, String description, String unit) {
            return DoubleHistogram.NOOP;
        }

        @Override
        public DoubleHistogram registerDoubleHistogram(String name, String description, String unit, List<Double> bucketBoundaries) {
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
        public DoubleAsyncCounter registerDoublesAsyncCounter(
            String name,
            String description,
            String unit,
            Supplier<Collection<DoubleWithAttributes>> observer
        ) {
            return DoubleAsyncCounter.NOOP;
        }

        @Override
        public LongUpDownCounter registerLongUpDownCounter(String name, String description, String unit) {
            return LongUpDownCounter.NOOP;
        }

        @Override
        public LongGauge registerLongGauge(String name, String description, String unit) {
            return LongGauge.NOOP;
        }

        @Override
        public LongAsyncGauge registerLongsAsyncGauge(
            String name,
            String description,
            String unit,
            Supplier<Collection<LongWithAttributes>> observer
        ) {
            return LongAsyncGauge.NOOP;
        }

        @Override
        public LongHistogram registerLongHistogram(String name, String description, String unit) {
            return LongHistogram.NOOP;
        }

        @Override
        public LongHistogram registerLongHistogram(String name, String description, String unit, List<Long> bucketBoundaries) {
            return LongHistogram.NOOP;
        }
    };
}
