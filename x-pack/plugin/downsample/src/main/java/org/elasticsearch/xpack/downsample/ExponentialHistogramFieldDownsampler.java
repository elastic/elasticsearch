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
    static AbstractFieldDownsampler<?> create(String name, IndexFieldData<?> fieldData, DownsampleConfig.SamplingMethod samplingMethod) {
        return switch (samplingMethod) {
            case AGGREGATE -> new ExponentialHistogramFieldDownsampler.MergeProducer(name, fieldData);
            case LAST_VALUE -> new ExponentialHistogramFieldDownsampler.LastValueProducer(name, fieldData);
        };
    }

    protected abstract ExponentialHistogram downsampledValue();

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
     * Downsamples an exponential histogram by merging all values.
     */
    static class MergeProducer extends ExponentialHistogramFieldDownsampler {
        private ExponentialHistogramMerger merger = null;

        MergeProducer(String name, IndexFieldData<?> fieldData) {
            super(name, fieldData);
        }

        @Override
        public void reset() {
            isEmpty = true;
            merger = null;
        }

        @Override
        protected ExponentialHistogram downsampledValue() {
            if (isEmpty()) {
                return null;
            }
            ExponentialHistogram exponentialHistogram = merger.get();
            merger.close();
            return exponentialHistogram;
        }

        @Override
        public void collect(ExponentialHistogramValuesReader docValues, IntArrayList docIdBuffer) throws IOException {
            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                isEmpty = false;
                if (merger == null) {
                    merger = ExponentialHistogramMerger.create(ExponentialHistogramCircuitBreaker.noop());
                }
                ExponentialHistogram value = docValues.histogramValue();
                merger.add(value);
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
            isEmpty = true;
            lastValue = null;
        }

        @Override
        public void collect(ExponentialHistogramValuesReader docValues, IntArrayList docIdBuffer) throws IOException {
            if (isEmpty() == false) {
                return;
            }

            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                isEmpty = false;
                lastValue = docValues.histogramValue();
                return;
            }
        }

        @Override
        protected ExponentialHistogram downsampledValue() {
            return lastValue;
        }
    }
}
