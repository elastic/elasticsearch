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
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogramHolder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.analytics.mapper.ExponentialHistogramFieldMapper;
import org.elasticsearch.xpack.core.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;
import org.elasticsearch.xpack.core.exponentialhistogram.fielddata.LeafExponentialHistogramFieldData;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A producer that can be used for downsampling ONLY an exponential histogram field whether it's a metric or a label.
 */
abstract class ExponentialHistogramFieldDownsampler extends AbstractFieldDownsampler<ExponentialHistogramValuesReader> {
    static final String TYPE = "exponential_histogram";

    ExponentialHistogramFieldDownsampler(String name, IndexFieldData<?> fieldData) {
        super(name, fieldData);
    }

    /**
     * @return the requested producer based on the sampling method for an exponential histogram field
     */
    static ExponentialHistogramFieldDownsampler create(
        String name,
        IndexFieldData<?> fieldData,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        return switch (samplingMethod) {
            case AGGREGATE -> new AggregateHistogram(name, fieldData);
            case LAST_VALUE -> new LastValueProducer(name, fieldData);
        };
    }

    protected abstract ExponentialHistogram downsampledValue();

    static boolean isAggregateDownsampler(DownsampleConfig.SamplingMethod samplingMethod) {
        return samplingMethod == DownsampleConfig.SamplingMethod.AGGREGATE;
    }

    public static boolean supportsFieldType(MappedFieldType fieldType) {
        return ExponentialHistogramFieldMapper.CONTENT_TYPE.equals(fieldType.typeName());
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            builder.field(name());
            ExponentialHistogramXContent.serialize(builder, downsampledValue());
        }
    }

    @Override
    public ExponentialHistogramValuesReader getLeaf(LeafReaderContext context) throws IOException {
        LeafExponentialHistogramFieldData exponentialHistogramFieldData = (LeafExponentialHistogramFieldData) fieldData.load(context);
        return exponentialHistogramFieldData.getHistogramValues();
    }

    /**
     * Temporality-aware aggregate downsampler for exponential histograms. The aggregation strategy depends on the
     * temporality of the time series:
     * <ul>
     *   <li>{@link Temporality#DELTA} and {@link Temporality#DEFAULT}: merge all histograms in the bucket</li>
     *   <li>{@link Temporality#CUMULATIVE}: preserve the oldest value per bucket, tracking resets</li>
     * </ul>
     * Important note: This class assumes that field values are collected and sorted by descending order by time.
     */
    static final class AggregateHistogram extends ExponentialHistogramFieldDownsampler {

        private final CumulativeCollector cumulativeCollector;
        private final DeltaCollector deltaCollector;
        private TemporalityAwareCollector temporalityCollector;

        AggregateHistogram(String name, IndexFieldData<?> fieldData) {
            super(name, fieldData);
            cumulativeCollector = new CumulativeCollector(name);
            deltaCollector = new DeltaCollector();
        }

        @Override
        public void collect(ExponentialHistogramValuesReader docValues, IntArrayList docIdBuffer) throws IOException {
            throw new UnsupportedOperationException("Use collect(docValues, timestamps, docIdBuffer, temporality) instead");
        }

        @Override
        public void collectCurrentValues(ExponentialHistogramValuesReader docValues) {
            throw new UnsupportedOperationException("This producer should never be called without timestamps");
        }

        void collect(
            ExponentialHistogramValuesReader docValues,
            LongArrayList timestampBuffer,
            IntArrayList docIdBuffer,
            Temporality temporality
        ) throws IOException {
            assert assertTemporality(temporality) : "delegate should change only after a tsid reset";
            if (temporalityCollector == null) {
                temporalityCollector = switch (temporality) {
                    case DELTA, DEFAULT -> deltaCollector;
                    case CUMULATIVE -> cumulativeCollector;
                };
            }
            assert timestampBuffer.size() == docIdBuffer.size() : "timestamps and docIdBuffer should have the same size";
            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                state = State.IN_PROGRESS;
                ExponentialHistogram value = docValues.histogramValue();
                temporalityCollector.collect(value, timestampBuffer.get(i));
            }
        }

        private boolean assertTemporality(Temporality temporality) {
            if (temporalityCollector == null) {
                return true;
            } else if (temporalityCollector == cumulativeCollector) {
                return temporality == Temporality.CUMULATIVE;
            } else if (temporalityCollector == deltaCollector) {
                return temporality == Temporality.DEFAULT || temporality == Temporality.DELTA;
            }
            throw new IllegalStateException("unexpected temporality collector");
        }

        @Override
        public void reset() {
            state = State.EMPTY;
            if (temporalityCollector != null) {
                temporalityCollector.reset();
            }
        }

        void tsidReset() {
            state = State.EMPTY;
            if (temporalityCollector != null) {
                temporalityCollector.tsidReset();
                temporalityCollector = null;
            }
        }

        @Override
        protected ExponentialHistogram downsampledValue() {
            return temporalityCollector != null ? temporalityCollector.downsampledValue() : null;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false && temporalityCollector != null) {
                builder.field(name());
                ExponentialHistogramXContent.serialize(builder, temporalityCollector.downsampledValue());
            }
        }

        @Nullable
        TemporalityAwareCollector delegateCollector() {
            return temporalityCollector;
        }

        void updateResetDataPoints(ResetDataPoints resetDataPoints) {
            if (temporalityCollector == cumulativeCollector) {
                cumulativeCollector.updateResetDataPoints(resetDataPoints);
            }
        }

        interface TemporalityAwareCollector {
            void collect(ExponentialHistogram value, long timestamp) throws IOException;

            void reset();

            void tsidReset();

            ExponentialHistogram downsampledValue();
        }

        /**
         * For {@link Temporality#DELTA} and {@link Temporality#DEFAULT} temporality, merges all histograms in the bucket.
         */
        static class DeltaCollector implements TemporalityAwareCollector {

            // The merger doesn't need to be closed: it uses a no-op circuit breaker
            private ExponentialHistogramMerger merger = null;

            @Override
            public void collect(ExponentialHistogram value, long timestamp) {
                if (merger == null) {
                    merger = ExponentialHistogramMerger.create(ExponentialHistogramCircuitBreaker.noop());
                }
                merger.add(value);
            }

            @Override
            public ExponentialHistogram downsampledValue() {
                ExponentialHistogram result = merger != null ? merger.get() : null;
                // Reset to null to prevent accidental changes to the returned histogram
                merger = null;
                return result;
            }

            @Override
            public void reset() {
                merger = null;
            }

            @Override
            public void tsidReset() {
                reset();
            }
        }

        /**
         * For {@link Temporality#CUMULATIVE} temporality, we track the following:
         * <ul>
         *   <li>The oldest histogram value for this field per tsid and bucket, so it can be stored in the downsampled document.</li>
         *   <li>The last-seen timestamp, so it can update the {@link ResetDataPoints} structure which is shared across all
         *       aggregate histogram producers.</li>
         * </ul>
         * Reset detection uses both valueCount comparison and {@link ExponentialHistogramMerger#setToDifference}:
         * a reset is detected when either the value count dropped (going forward in time) or the consecutive pair
         * fails the cumulative structure check. This is stricter than the ES|QL {@code increase} implementation
         * which only checks value count.
         * <p>
         * Important note: This class assumes that field values are collected and sorted by descending order by time.
         */
        static class CumulativeCollector implements TemporalityAwareCollector {

            private final String name;
            // No need to close: uses a no-op circuit breaker
            private final ExponentialHistogramMerger diffMerger = ExponentialHistogramMerger.create(
                ExponentialHistogramCircuitBreaker.noop()
            );
            private final Deque<ResetDataPoints.ResetPoint> resetStack = new ArrayDeque<>();
            private CompressedExponentialHistogramHolder downsampledHolder;

            // Visible for testing
            long lastTimestamp = -1;
            // Cross-bucket value: tracks the last histogram seen across bucket boundaries
            CompressedExponentialHistogramHolder previousValueHolder;
            // Captures the persisted value of the previous bucket for the same tsid.
            // Allows us to avoid persisting the after-the-reset document when it would be redundant.
            CompressedExponentialHistogramHolder previousBucketValueHolder;

            CumulativeCollector(String name) {
                this.name = name;
            }

            @Override
            public void collect(ExponentialHistogram value, long timestamp) {
                // If this is the first time we encounter a value for this tsid
                if (previousValueHolder == null) {
                    previousValueHolder = createHolder(value);
                    downsampledHolder = createHolder(value);
                    lastTimestamp = timestamp;
                    return;
                }

                // First value in a new bucket (after reset())
                if (downsampledHolder == null) {
                    downsampledHolder = createHolder(value);
                }

                // When we detect a reset (remember that field values are collected and sorted by descending order by time):
                // either the value count dropped (going forward) or the pair isn't truly cumulative
                if (value.valueCount() > previousValueHolder.accessor().valueCount()
                    || diffMerger.setToDifference(previousValueHolder.accessor(), value) == false) {
                    // We check if we need to persist the previous value too.
                    // lastTimestamp == -1 means that the previous value is already persisted by a previous bucket, nothing extra to
                    // persist.
                    if (lastTimestamp > 0) {
                        // If we have a previous value in this bucket, we need to see if the last persisted value is enough to capture
                        // the reset or not.
                        ExponentialHistogram lastPersisted = null;
                        if (resetStack.isEmpty() == false) {
                            lastPersisted = ((ResetDataPoints.HistogramResetValue) resetStack.peek().value()).value();
                        } else if (previousBucketValueHolder != null) {
                            lastPersisted = previousBucketValueHolder.accessor();
                        }
                        // If there is no known last persisted value or the last persisted has a larger value count than the
                        // current value, we need to store the previous document to capture the reset.
                        if (lastPersisted == null || value.valueCount() < lastPersisted.valueCount()) {
                            resetStack.push(new ResetDataPoints.ResetPoint(lastTimestamp, copyHistogram(previousValueHolder.accessor())));
                        }
                    }
                    // This is the last value before reset, which we always need to persist
                    resetStack.push(new ResetDataPoints.ResetPoint(timestamp, copyHistogram(value)));
                }
                downsampledHolder.set(value);
                previousValueHolder.set(value);
                assert lastTimestamp == -1 || timestamp < lastTimestamp;
                lastTimestamp = timestamp;
            }

            @Override
            public ExponentialHistogram downsampledValue() {
                return downsampledHolder != null ? downsampledHolder.accessor() : null;
            }

            @Override
            public void reset() {
                previousBucketValueHolder = downsampledHolder;
                downsampledHolder = null;
                lastTimestamp = -1;
                resetStack.clear();
            }

            @Override
            public void tsidReset() {
                reset();
                previousValueHolder = null;
                previousBucketValueHolder = null;
            }

            /**
             * Update {@link ResetDataPoints} which contains all reset histogram values,
             * with the latest reset points of this histogram field.
             * @param resetDataPoints the extra reset data values for every cumulative field for this bucket
             */
            void updateResetDataPoints(ResetDataPoints resetDataPoints) {
                if (resetStack.isEmpty()) {
                    return;
                }
                // It is possible that the first reset data point is the same as the downsampled value;
                // we skip this if that is the case.
                var firstResetPoint = resetStack.pop();
                var histogramValue = (ResetDataPoints.HistogramResetValue) firstResetPoint.value();
                if (downsampledHolder == null || histogramValue.value().equals(downsampledHolder.accessor()) == false) {
                    resetDataPoints.addDataPoint(name, firstResetPoint);
                }
                while (resetStack.isEmpty() == false) {
                    resetDataPoints.addDataPoint(name, resetStack.pop());
                }
            }

            private static CompressedExponentialHistogramHolder createHolder(ExponentialHistogram value) {
                // No need to close: uses a no-op circuit breaker
                CompressedExponentialHistogramHolder holder = CompressedExponentialHistogramHolder.create(
                    ExponentialHistogramCircuitBreaker.noop()
                );
                holder.set(value);
                return holder;
            }

            private static ExponentialHistogram copyHistogram(ExponentialHistogram value) {
                return createHolder(value).accessor();
            }
        }
    }

    /**
     * Downsamples an exponential histogram by preserving the last value.
     * Important note: This class assumes that field values are collected and sorted by descending order by time
     */
    static class LastValueProducer extends ExponentialHistogramFieldDownsampler {
        private ExponentialHistogram lastValue = null;

        LastValueProducer(String name, IndexFieldData<?> fieldData) {
            super(name, fieldData);
        }

        @Override
        public void reset() {
            state = State.EMPTY;
            lastValue = null;
        }

        @Override
        public void collect(ExponentialHistogramValuesReader docValues, IntArrayList docIdBuffer) throws IOException {
            if (isDone()) {
                return;
            }
            for (int i = 0; i < docIdBuffer.size() && isDone() == false; i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                collectCurrentValues(docValues);
            }
        }

        @Override
        public void collectCurrentValues(ExponentialHistogramValuesReader docValues) throws IOException {
            lastValue = docValues.histogramValue();
            state = State.BUCKET_COMPLETED;
        }

        @Override
        protected ExponentialHistogram downsampledValue() {
            return lastValue;
        }
    }
}
