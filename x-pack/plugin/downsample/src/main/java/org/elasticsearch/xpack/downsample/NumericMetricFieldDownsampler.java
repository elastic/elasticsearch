/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Class that collects all raw values for a numeric metric field and computes its aggregate (downsampled)
 * values. Based on the supported metric types, the subclasses of this class compute values for
 * gauge and metric types.
 */
abstract sealed class NumericMetricFieldDownsampler extends AbstractFieldDownsampler<SortedNumericDoubleValues> permits
    AggregateMetricDoubleFieldDownsampler, NumericMetricFieldDownsampler.AggregateGauge, NumericMetricFieldDownsampler.LastValue,
    NumericMetricFieldDownsampler.AggregateCounter {

    NumericMetricFieldDownsampler(String name, IndexFieldData<?> fieldData) {
        super(name, fieldData);
    }

    @Override
    public SortedNumericDoubleValues getLeaf(LeafReaderContext context) {
        LeafNumericFieldData numericFieldData = (LeafNumericFieldData) fieldData.load(context);
        return numericFieldData.getDoubleValues();
    }

    public static boolean supportsFieldType(MappedFieldType fieldType) {
        TimeSeriesParams.MetricType metricType = fieldType.getMetricType();
        return metricType == TimeSeriesParams.MetricType.GAUGE || metricType == TimeSeriesParams.MetricType.COUNTER;
    }

    static NumericMetricFieldDownsampler create(
        String fieldName,
        MappedFieldType fieldType,
        IndexFieldData<?> fieldData,
        DownsampleConfig.SamplingMethod samplingMethod,
        DownsamplerCountPerValueType fieldCounts
    ) {
        assert supportsFieldType(fieldType)
            : "only gauges and counters accepted, other metrics should have been handled by dedicated downsamplers";
        TimeSeriesParams.MetricType metricType = fieldType.getMetricType();
        if (samplingMethod == DownsampleConfig.SamplingMethod.LAST_VALUE) {
            fieldCounts.increaseNumericFields();
            return new NumericMetricFieldDownsampler.LastValue(fieldName, fieldData);
        }
        if (metricType == TimeSeriesParams.MetricType.GAUGE) {
            fieldCounts.increaseNumericFields();
            return new NumericMetricFieldDownsampler.AggregateGauge(fieldName, fieldData);
        }
        fieldCounts.increaseAggregateCounterFields();
        return new NumericMetricFieldDownsampler.AggregateCounter(fieldName, fieldData);
    }

    static final double MAX_NO_VALUE = -Double.MAX_VALUE;
    static final double MIN_NO_VALUE = Double.MAX_VALUE;

    /**
     * {@link NumericMetricFieldDownsampler} implementation for creating an aggregate gauge metric field
     */
    static final class AggregateGauge extends NumericMetricFieldDownsampler {

        double max = MAX_NO_VALUE;
        double min = MIN_NO_VALUE;
        final CompensatedSum sum = new CompensatedSum();
        long count;

        AggregateGauge(String name, IndexFieldData<?> fieldData) {
            super(name, fieldData);
        }

        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                isEmpty = false;
                int docValuesCount = docValues.docValueCount();
                for (int j = 0; j < docValuesCount; j++) {
                    double value = docValues.nextValue();
                    this.max = Math.max(value, max);
                    this.min = Math.min(value, min);
                    sum.add(value);
                    count++;
                }
            }
        }

        @Override
        public void reset() {
            isEmpty = true;
            max = MAX_NO_VALUE;
            min = MIN_NO_VALUE;
            sum.reset(0, 0);
            count = 0;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.startObject(name());
                builder.field("min", min);
                builder.field("max", max);
                builder.field("sum", sum.value());
                builder.field("value_count", count);
                builder.endObject();
            }
        }
    }

    /**
     * {@link NumericMetricFieldDownsampler} implementation for sampling the last value of a numeric metric field.
     * Important note: This class assumes that field values are collected and sorted by descending order by time.
     */
    static final class LastValue extends NumericMetricFieldDownsampler {

        double lastValue = Double.NaN;

        LastValue(String name, IndexFieldData<?> fieldData) {
            super(name, fieldData);
        }

        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            if (isEmpty() == false) {
                return;
            }

            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId)) {
                    isEmpty = false;
                    lastValue = docValues.nextValue();
                    return;
                }
            }
        }

        public Double lastValue() {
            if (isEmpty()) {
                return null;
            }
            return lastValue;
        }

        @Override
        public void reset() {
            isEmpty = true;
            lastValue = Double.NaN;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.field(name(), lastValue);
            }
        }
    }

    /**
     * {@link NumericMetricFieldDownsampler} implementation for creating the required downsampling doc to support a pre-aggregated
     *  counter metric field. This producer tracks the following:
     * - The first value for this counter per tsid and bucket, so it can be stored in the downsampled document.
     * - The last-seen timestamp, so it can update the extraDataPoints structure which is shared across all aggregate counter producers.
     * Important note: This class assumes that field values are collected and sorted by descending order by time.
     */
    static final class AggregateCounter extends NumericMetricFieldDownsampler {

        final Deque<CounterResetDataPoints.ResetPoint> resetStack = new ArrayDeque<>();
        double downsampledValue = Double.NaN;
        long lastTimestamp = -1;
        // Cross bucket value
        double previousValue = Double.NaN;
        // This value captures the persisted value of the previous bucket for the same tsid and
        // allows us to avoid persisting the after-the-reset-document
        double previousBucketValue = Double.NaN;

        AggregateCounter(String name, IndexFieldData<?> fieldData) {
            super(name, fieldData);
        }

        public void collect(SortedNumericDoubleValues counterDocValues, long[] timestamps, IntArrayList docIdBuffer) throws IOException {
            assert timestamps.length == docIdBuffer.size() : "timestamps and docIdBuffer should have the same size";
            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                var currentTimestamp = timestamps[i];
                if (counterDocValues.advanceExact(docId) == false || currentTimestamp < 0) {
                    continue;
                }
                int docValuesCount = counterDocValues.docValueCount();
                assert docValuesCount > 0;
                isEmpty = false;

                var currentCounterValue = counterDocValues.nextValue();
                // If this the first time we encounter a value for this tsid
                if (Double.isNaN(previousValue)) {
                    downsampledValue = currentCounterValue;
                    previousValue = currentCounterValue;
                    lastTimestamp = currentTimestamp;
                    continue;
                }

                // when we detect a reset, (remember that field values are collected and sorted by descending order by time)
                if (currentCounterValue > previousValue) {
                    // We check if we need to persist the previous value too
                    // If timestamp -1 means that the previous value is already persisted by a previous bucket, nothing extra to persist
                    if (lastTimestamp > 0) {
                        // If we have a previous value in this bucket, we need to see if the last persisted value is enough to capture the
                        // reset or not.
                        double lastPersisted = Double.NaN;
                        if (resetStack.isEmpty() == false) {
                            lastPersisted = resetStack.peek().value();
                        } else if (Double.isNaN(previousBucketValue) == false) {
                            lastPersisted = previousBucketValue;
                        }
                        // If there is no known last persisted value or the last persisted is larger than the current value,
                        // we need to store the previous document to capture the reset.
                        if (Double.isNaN(lastPersisted) || Double.compare(currentCounterValue, lastPersisted) < 0) {
                            resetStack.push(new CounterResetDataPoints.ResetPoint(lastTimestamp, previousValue));
                        }
                    }
                    // This is the last value before reset, which we always need to persist
                    resetStack.push(new CounterResetDataPoints.ResetPoint(currentTimestamp, currentCounterValue));
                }
                downsampledValue = currentCounterValue;
                previousValue = currentCounterValue;
                assert lastTimestamp == -1 || currentTimestamp < lastTimestamp;
                lastTimestamp = currentTimestamp;
            }
        }

        public void reset() {
            isEmpty = true;
            previousBucketValue = downsampledValue;
            downsampledValue = Double.NaN;
            lastTimestamp = -1;
            resetStack.clear();
        }

        public void tsidReset() {
            reset();
            previousValue = Double.NaN;
            previousBucketValue = Double.NaN;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.field(name(), downsampledValue);
            }
        }

        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            throw new UnsupportedOperationException("This producer should never be called without timestamps");
        }

        /**
         * Update {@link CounterResetDataPoints} which contains all reset counter values,
         * with the latest reset points of this counter field.
         * @param counterResetDataPoints the extra reset data values for every counter for this bucket
         */
        public void updateResetDataPoints(CounterResetDataPoints counterResetDataPoints) {
            if (resetStack.isEmpty()) {
                return;
            }
            // It is possible that the first reset data point is the same with the first data point
            // we skip this if this is the case
            var firstResetPoint = resetStack.pop();
            if (firstResetPoint.value() != downsampledValue) {
                counterResetDataPoints.addDataPoint(name(), firstResetPoint);
            }
            while (resetStack.isEmpty() == false) {
                counterResetDataPoints.addDataPoint(name(), resetStack.pop());
            }
        }
    }
}
