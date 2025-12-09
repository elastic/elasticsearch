/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;

import java.io.IOException;

/**
 * Class that collects all raw values for an exponential histogram metric field and computes its aggregate (downsampled)
 * values.
 */
final class ExponentialHistogramMetricFieldProducer extends AbstractDownsampleFieldProducer<ExponentialHistogramValuesReader> {

    private ExponentialHistogramMerger merger = null;

    ExponentialHistogramMetricFieldProducer(String name) {
        super(name);
    }

    /**
     * @return the requested produces based on the sampling method for metric of type exponential histogram
     */
    static AbstractDownsampleFieldProducer<?> create(String name, DownsampleConfig.SamplingMethod samplingMethod) {
        return switch (samplingMethod) {
            case AGGREGATE -> new ExponentialHistogramMetricFieldProducer(name);
            case LAST_VALUE -> new LastValueFieldProducer.ExponentialHistogramFieldProducer(name);
        };
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

    @Override
    public void reset() {
        isEmpty = true;
        merger = null;
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            builder.field(name());
            ExponentialHistogramXContent.serialize(builder, merger.get());
            merger.close();
        }
    }
}
