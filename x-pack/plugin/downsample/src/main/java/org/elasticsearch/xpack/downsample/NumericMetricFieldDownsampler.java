/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.core.Nullable;
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

    // Downsamplers are shared across leaf collectors, but doc-values iterators are leaf-local and forward-only.
    // Keep the active iterator and read its docID() when switching back to it instead of storing per-leaf positions.
    private DocIdSetIterator leafDocIdIterator;
    int leafDocIdIteratorDoc = -1;
    boolean leafIteratorExhausted;

    NumericMetricFieldDownsampler(String name, IndexFieldData<?> fieldData) {
        super(name, fieldData);
    }

    @Override
    public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
        if (isDone() || docIdBuffer.isEmpty()) {
            return;
        }

        DocIdSetIterator docIdIterator = docValues.docIdIterator();
        if (docIdIterator == null) {
            collectUsingAdvanceExact(docValues, docIdBuffer);
            return;
        }

        resetLeafIteratorStateIfNeeded(docIdIterator);
        if (leafIteratorExhausted) {
            return;
        }

        collectUsingDocIdIterator(docValues, docIdBuffer);
    }

    private void collectUsingAdvanceExact(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
        for (int i = 0; i < docIdBuffer.size() && isDone() == false; i++) {
            int docId = docIdBuffer.get(i);
            if (docValues.advanceExact(docId) == false) {
                continue;
            }
            collectCurrentValues(docValues);
        }
    }

    void resetLeafIteratorStateIfNeeded(DocIdSetIterator docIdIterator) {
        if (leafDocIdIterator != docIdIterator) {
            // If we see a previously used iterator again, its own docID() is the last position for that leaf.
            leafDocIdIterator = docIdIterator;
            leafDocIdIteratorDoc = docIdIterator.docID();
            leafIteratorExhausted = leafDocIdIteratorDoc == DocIdSetIterator.NO_MORE_DOCS;
        }
    }

    /**
     * Collects buffered docs by intersecting the ordered buffer with the numeric doc-values iterator.
     * <p>
     * This path is used only when the field exposes a {@link DocIdSetIterator}. The iterator is leaf-local
     * and forward-only, so we never try to rewind it. When collection later returns to a previously seen
     * leaf, {@link #resetLeafIteratorStateIfNeeded(DocIdSetIterator)} reads the iterator's own
     * {@link DocIdSetIterator#docID()} and resumes from that position.
     *
     * <pre>
     * buffered doc ids:  [ 3 ][ 7 ][ 9 ][ 15 ]
     * doc-values docs:   [ 1 ][ 3 ][ 8 ][ 9 ][ 20 ]
     *
     * 1. Advance the doc-values iterator to the first buffered doc or beyond.
     *
     *    buffered doc ids:  [ 3 ][ 7 ][ 9 ][ 15 ]
     *                         ^
     *    doc-values docs:   [ 1 ][ 3 ][ 8 ][ 9 ][ 20 ]
     *                         ^
     *
     * 2. Keep comparing the current doc-values doc to the current buffered target:
     *
     *    current doc &lt; target doc  -> advance doc-values to target doc
     *    current doc == target doc -> collect current values, then move to the next buffered target
     *    current doc &gt; target doc  -> binary-search the next buffered target that is >= current doc
     *
     * If the iterator is exhausted, the leaf is marked exhausted and later collections for the same iterator
     * return without touching doc values. This turns the problem into a forward-only merge instead of probing
     * every buffered doc with {@code advanceExact(docId)}.
     * </pre>
     */
    private void collectUsingDocIdIterator(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
        int bufferedDocCount = docIdBuffer.size();
        int[] bufferedDocIds = docIdBuffer.buffer;
        if (leafDocIdIteratorDoc > bufferedDocIds[bufferedDocCount - 1]) {
            return;
        }

        int firstBufferedDocId = bufferedDocIds[0];
        if (leafDocIdIteratorDoc < firstBufferedDocId) {
            // advance to the closest doc that >= firstBufferedDocId
            advanceLeafDocIdIterator(firstBufferedDocId);
            if (leafIteratorExhausted) {
                return;
            }
        }

        // find the closest index in bufferedDocIds that points to a doc that is >= leafDocIdIteratorDoc
        int index = lowerBound(bufferedDocIds, 0, bufferedDocCount, leafDocIdIteratorDoc);
        while (index < bufferedDocCount && leafIteratorExhausted == false && isDone() == false) {
            int targetDocId = bufferedDocIds[index];
            if (leafDocIdIteratorDoc < targetDocId) {
                // advance to the closest doc that is >= targetDocId
                advanceLeafDocIdIterator(targetDocId);
                continue;
            }

            // found the intersection (means that the particular field exists for the current doc)
            if (leafDocIdIteratorDoc == targetDocId) {
                collectCurrentValues(docValues);
                index++;
                if (index < bufferedDocCount && isDone() == false) {
                    advanceLeafDocIdIterator(bufferedDocIds[index]);
                }
                continue;
            }

            // move to the next buffered doc, which is the closest to the next doc of the particular field
            index = lowerBound(bufferedDocIds, index + 1, bufferedDocCount, leafDocIdIteratorDoc);
        }
    }

    void advanceLeafDocIdIterator(int targetDocId) throws IOException {
        leafDocIdIteratorDoc = leafDocIdIterator.advance(targetDocId);
        leafIteratorExhausted = leafDocIdIteratorDoc == DocIdSetIterator.NO_MORE_DOCS;
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
        public void collectCurrentValues(SortedNumericDoubleValues docValues) throws IOException {
            int docValuesCount = docValues.docValueCount();
            for (int j = 0; j < docValuesCount; j++) {
                double value = docValues.nextValue();
                this.max = Math.max(value, max);
                this.min = Math.min(value, min);
                sum.add(value);
                count++;
            }
            state = State.IN_PROGRESS;
        }

        @Override
        public void reset() {
            state = State.EMPTY;
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
        public void collectCurrentValues(SortedNumericDoubleValues docValues) throws IOException {
            lastValue = docValues.nextValue();
            state = State.BUCKET_COMPLETED;
        }

        public Double lastValue() {
            if (isEmpty()) {
                return null;
            }
            return lastValue;
        }

        @Override
        public void reset() {
            state = State.EMPTY;
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
     * counter metric field. The way the counter values are aggregated depends on the fields temporality, which is the same for the
     * same tsid. This class delegates to {@link TemporalityAwareCollector}s to perform the actual aggregation.
     * Important note: This class assumes that field values are collected and sorted by descending order by time.
     */
    static final class AggregateCounter extends NumericMetricFieldDownsampler {

        private final CumulativeCollector cumulativeCollector;
        private final DeltaCollector deltaCollector;
        private TemporalityAwareCollector temporalityCollector;

        AggregateCounter(String name, IndexFieldData<?> fieldData) {
            super(name, fieldData);
            cumulativeCollector = new CumulativeCollector(name);
            deltaCollector = new DeltaCollector();
        }

        public void collect(
            SortedNumericDoubleValues counterDocValues,
            LongArrayList timestampBuffer,
            IntArrayList docIdBuffer,
            Temporality temporality
        ) throws IOException {
            assert assertTemporality(temporality) : "delegate should change only after a tsid reset";
            if (docIdBuffer.isEmpty()) {
                return;
            }
            assert timestampBuffer.size() == docIdBuffer.size() : "timestampBuffer and docIdBuffer should have the same size";
            if (temporalityCollector == null) {
                temporalityCollector = switch (temporality) {
                    case DELTA -> deltaCollector;
                    case CUMULATIVE, DEFAULT -> cumulativeCollector;
                };
            }
            DocIdSetIterator docIdIterator = counterDocValues.docIdIterator();
            if (docIdIterator == null) {
                collectCounterUsingAdvanceExact(counterDocValues, timestampBuffer, docIdBuffer);
            } else {
                resetLeafIteratorStateIfNeeded(docIdIterator);
                if (leafIteratorExhausted == false) {
                    collectCounterUsingDocIdIterator(counterDocValues, timestampBuffer, docIdBuffer);
                }
            }
        }

        private void collectCounterUsingAdvanceExact(
            SortedNumericDoubleValues counterDocValues,
            LongArrayList timestampBuffer,
            IntArrayList docIdBuffer
        ) throws IOException {
            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                long currentTimestamp = timestampBuffer.get(i);
                if (counterDocValues.advanceExact(docId) == false || currentTimestamp < 0) {
                    continue;
                }
                int docValuesCount = counterDocValues.docValueCount();
                assert docValuesCount > 0;
                temporalityCollector.collect(counterDocValues.nextValue(), currentTimestamp);
                state = State.IN_PROGRESS;
            }
        }

        /**
         * Mirrors {@link NumericMetricFieldDownsampler#collectUsingDocIdIterator} but threads the timestamp
         * alongside each matched doc. {@code timestamps[index]} is always the timestamp for
         * {@code docIdBuffer.get(index)}, so the same {@code index} used to walk the buffer also addresses
         * the correct timestamp — even when {@link #lowerBound} skips ahead past unmatched entries.
         * <p>
         * When a doc is skipped due to {@code timestamp < 0}, the iterator has already advanced to that doc;
         * the subsequent {@link #advanceLeafDocIdIterator} call to the next target naturally moves past it.
         */
        private void collectCounterUsingDocIdIterator(
            SortedNumericDoubleValues counterDocValues,
            LongArrayList timestampBuffer,
            IntArrayList docIdBuffer
        ) throws IOException {
            int bufferedDocCount = docIdBuffer.size();
            int[] bufferedDocIds = docIdBuffer.buffer;
            if (leafDocIdIteratorDoc > bufferedDocIds[bufferedDocCount - 1]) {
                return;
            }

            int firstBufferedDocId = bufferedDocIds[0];
            if (leafDocIdIteratorDoc < firstBufferedDocId) {
                advanceLeafDocIdIterator(firstBufferedDocId);
                if (leafIteratorExhausted) {
                    return;
                }
            }

            int index = lowerBound(bufferedDocIds, 0, bufferedDocCount, leafDocIdIteratorDoc);
            while (index < bufferedDocCount && leafIteratorExhausted == false) {
                int targetDocId = bufferedDocIds[index];
                if (leafDocIdIteratorDoc < targetDocId) {
                    advanceLeafDocIdIterator(targetDocId);
                    continue;
                }

                if (leafDocIdIteratorDoc == targetDocId) {
                    long timestamp = timestampBuffer.get(index);
                    if (timestamp >= 0) {
                        int docValuesCount = counterDocValues.docValueCount();
                        assert docValuesCount > 0;
                        temporalityCollector.collect(counterDocValues.nextValue(), timestamp);
                        state = State.IN_PROGRESS;
                    }
                    index++;
                    if (index < bufferedDocCount) {
                        advanceLeafDocIdIterator(bufferedDocIds[index]);
                    }
                    continue;
                }

                // leafDocIdIteratorDoc > targetDocId: skip ahead in the buffer to the next candidate
                index = lowerBound(bufferedDocIds, index + 1, bufferedDocCount, leafDocIdIteratorDoc);
            }
        }

        private boolean assertTemporality(Temporality temporality) {
            if (temporalityCollector == null) {
                return true;
            } else if (temporalityCollector == cumulativeCollector) {
                return temporality == Temporality.DEFAULT || temporality == Temporality.CUMULATIVE;
            } else if (temporalityCollector == deltaCollector) {
                return temporality == Temporality.DELTA;
            }
            throw new IllegalStateException("unexpected temporality collector");
        }

        public void reset() {
            state = State.EMPTY;
            if (temporalityCollector != null) {
                temporalityCollector.reset();
            }
        }

        public void tsidReset() {
            state = State.EMPTY;
            if (temporalityCollector != null) {
                temporalityCollector.tsidReset();
                temporalityCollector = null;
            }
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false && temporalityCollector != null) {
                builder.field(name(), temporalityCollector.downsampledValue());
            }
        }

        // For testing
        @Nullable
        TemporalityAwareCollector delegateCollector() {
            return temporalityCollector;
        }

        double downsampledValue() {
            return temporalityCollector != null ? temporalityCollector.downsampledValue() : Double.NaN;
        }

        /**
         * Throws UnsupportedOperationException, use {@link #collect(SortedNumericDoubleValues, LongArrayList, IntArrayList, Temporality) }
         * instead.
         */
        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            throw new UnsupportedOperationException("This producer should never be called without timestamps");
        }

        /**
         * Throws UnsupportedOperationException, use {@link #collect(SortedNumericDoubleValues, LongArrayList, IntArrayList, Temporality) }
         * instead.
         */
        @Override
        public void collectCurrentValues(SortedNumericDoubleValues docValues) {
            throw new UnsupportedOperationException("This producer should never be called without timestamps");
        }

        /**
         * Update {@link ResetDataPoints} which contains all reset counter values,
         * with the latest reset points of this counter field.
         * @param resetDataPoints the extra reset data values for every counter for this bucket
         */
        public void updateResetDataPoints(ResetDataPoints resetDataPoints) {
            if (temporalityCollector == cumulativeCollector) {
                cumulativeCollector.updateResetDataPoints(resetDataPoints);
            }
        }

        interface TemporalityAwareCollector {
            void collect(double counterValue, long timestamps) throws IOException;

            void reset();

            void tsidReset();

            double downsampledValue();
        }

        /**
         * For {@link Temporality#DELTA} temporality, we just collect the sum of all counter values.
         * Important note: This class assumes that field values are collected and sorted by descending order by time.
         */
        static class DeltaCollector implements TemporalityAwareCollector {
            private final CompensatedSum downsampledValue = new CompensatedSum();

            public void collect(double counterValue, long unused) throws IOException {
                downsampledValue.add(counterValue);
            }

            public double downsampledValue() {
                return downsampledValue.value();
            }

            public void reset() {
                downsampledValue.reset(0, 0);
            }

            @Override
            public void tsidReset() {
                reset();
            }
        }

        /**
         * For {@link Temporality#CUMULATIVE} temporality, we track the following:
         * - The first value for this counter per tsid and bucket, so it can be stored in the downsampled document.
         * - The last-seen timestamp, so it can update the extraDataPoints structure which is shared across all aggregate counter producers.
         */
        static class CumulativeCollector implements TemporalityAwareCollector {

            private final String name;
            private final Deque<ResetDataPoints.ResetPoint> resetStack = new ArrayDeque<>();
            private double downsampledValue = Double.NaN;

            // Visible for testing
            long lastTimestamp = -1;
            // Cross bucket value
            double previousValue = Double.NaN;
            // This value captures the persisted value of the previous bucket for the same tsid and
            // allows us to avoid persisting the after-the-reset-document
            double previousBucketValue = Double.NaN;

            CumulativeCollector(String name) {
                this.name = name;
            }

            @Override
            public void collect(double counterValue, long timestamp) throws IOException {
                // If this the first time we encounter a value for this tsid
                if (Double.isNaN(previousValue)) {
                    downsampledValue = counterValue;
                    previousValue = counterValue;
                    lastTimestamp = timestamp;
                    return;
                }

                // when we detect a reset, (remember that field values are collected and sorted by descending order by time)
                if (counterValue > previousValue) {
                    // We check if we need to persist the previous value too
                    // If timestamp -1 means that the previous value is already persisted by a previous bucket, nothing extra to persist
                    if (lastTimestamp > 0) {
                        // If we have a previous value in this bucket, we need to see if the last persisted value is enough to capture
                        // the
                        // reset or not.
                        double lastPersisted = Double.NaN;
                        if (resetStack.isEmpty() == false) {
                            lastPersisted = ((ResetDataPoints.CounterResetValue) resetStack.peek().value()).value();
                        } else if (Double.isNaN(previousBucketValue) == false) {
                            lastPersisted = previousBucketValue;
                        }
                        // If there is no known last persisted value or the last persisted is larger than the current value,
                        // we need to store the previous document to capture the reset.
                        if (Double.isNaN(lastPersisted) || Double.compare(counterValue, lastPersisted) < 0) {
                            resetStack.push(new ResetDataPoints.ResetPoint(lastTimestamp, previousValue));
                        }
                    }
                    // This is the last value before reset, which we always need to persist
                    resetStack.push(new ResetDataPoints.ResetPoint(timestamp, counterValue));
                }
                downsampledValue = counterValue;
                previousValue = counterValue;
                assert lastTimestamp == -1 || timestamp < lastTimestamp;
                lastTimestamp = timestamp;
            }

            public void reset() {
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
            public double downsampledValue() {
                return downsampledValue;
            }

            /**
             * Update {@link ResetDataPoints} which contains all reset counter values,
             * with the latest reset points of this counter field.
             * @param resetDataPoints the extra reset data values for every counter for this bucket
             */
            void updateResetDataPoints(ResetDataPoints resetDataPoints) {
                if (resetStack.isEmpty()) {
                    return;
                }
                // It is possible that the first reset data point is the same with the first data point
                // we skip this if this is the case
                var firstResetPoint = resetStack.pop();
                if (((ResetDataPoints.CounterResetValue) firstResetPoint.value()).value() != downsampledValue) {
                    resetDataPoints.addDataPoint(name, firstResetPoint);
                }
                while (resetStack.isEmpty() == false) {
                    resetDataPoints.addDataPoint(name, resetStack.pop());
                }
            }
        }
    }
}
