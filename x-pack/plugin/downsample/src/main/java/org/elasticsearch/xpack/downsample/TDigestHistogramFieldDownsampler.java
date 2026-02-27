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
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafHistogramFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;
import org.elasticsearch.xpack.core.analytics.mapper.TDigestFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Class that collects all raw values for a (tdigest) histogram metric field and computes its aggregate (downsampled)
 * values.
 */
abstract class TDigestHistogramFieldDownsampler extends AbstractFieldDownsampler<HistogramValues> {

    static final double DEFAULT_COMPRESSION = 100;
    // MergingDigest is the best fit because we have pre-constructed histograms
    static final TDigestState.Type DEFAULT_TYPE = TDigestState.Type.MERGING;
    private static final String VALUES_FIELD = "values";
    private static final String CENTROIDS_FIELD = "centroids";
    protected final String valueLabel;

    TDigestHistogramFieldDownsampler(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        super(name, fieldData);
        valueLabel = TDigestFieldMapper.CONTENT_TYPE.equals(fieldType.typeName()) ? CENTROIDS_FIELD : VALUES_FIELD;
    }

    @Override
    public HistogramValues getLeaf(LeafReaderContext context) throws IOException {
        LeafHistogramFieldData histogramFieldData = (LeafHistogramFieldData) fieldData.load(context);
        return histogramFieldData.getHistogramValues();
    }

    public static boolean supportsFieldType(MappedFieldType fieldType) {
        return TDigestFieldMapper.CONTENT_TYPE.equals(fieldType.typeName())
            || HistogramFieldMapper.CONTENT_TYPE.equals(fieldType.typeName());
    }

    /**
     * @return the requested produces based on the sampling method for metric of type tdigest histogram
     */
    public static TDigestHistogramFieldDownsampler create(
        String name,
        MappedFieldType fieldType,
        IndexFieldData<?> fieldData,
        DownsampleConfig.SamplingMethod samplingMethod
    ) {
        return switch (samplingMethod) {
            case AGGREGATE -> new Aggregate(name, fieldType, fieldData);
            case LAST_VALUE -> new LastValue(name, fieldType, fieldData);
        };
    }

    private static class Aggregate extends TDigestHistogramFieldDownsampler {

        private final TDigestState.Type type;
        private final double compression;
        private TDigestState tDigestState = null;

        Aggregate(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
            super(name, fieldType, fieldData);
            this.type = DEFAULT_TYPE;
            this.compression = DEFAULT_COMPRESSION;
        }

        public void collect(HistogramValues docValues, IntArrayList docIdBuffer) throws IOException {
            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                isEmpty = false;
                if (tDigestState == null) {
                    // TODO: figure out what circuit breaker to use here and in the other histogram
                    tDigestState = TDigestState.createOfType(new NoopCircuitBreaker("downsampling-histograms"), type, compression);
                }
                final HistogramValue sketch = docValues.histogram();
                while (sketch.next()) {
                    tDigestState.add(sketch.value(), sketch.count());
                }
            }
        }

        @Override
        public void reset() {
            isEmpty = true;
            tDigestState = null;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                Iterator<Centroid> centroids = tDigestState.uniqueCentroids();
                final List<Double> values = new ArrayList<>();
                final List<Long> counts = new ArrayList<>();
                while (centroids.hasNext()) {
                    Centroid centroid = centroids.next();
                    values.add(centroid.mean());
                    counts.add(centroid.count());
                }
                builder.startObject(name()).field("counts", counts).field(valueLabel, values).endObject();
                tDigestState.close();
            }
        }
    }

    private static class LastValue extends TDigestHistogramFieldDownsampler {

        private HistogramValue lastValue = null;

        LastValue(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
            super(name, fieldType, fieldData);
        }

        public void collect(HistogramValues docValues, IntArrayList docIdBuffer) throws IOException {
            if (isEmpty() == false) {
                return;
            }
            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                isEmpty = false;
                lastValue = docValues.histogram();
                return;
            }
        }

        @Override
        public void reset() {
            isEmpty = true;
            lastValue = null;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                final HistogramValue histogramValue = lastValue;
                final List<Double> values = new ArrayList<>();
                final List<Long> counts = new ArrayList<>();
                while (histogramValue.next()) {
                    values.add(histogramValue.value());
                    counts.add(histogramValue.count());
                }
                builder.startObject(name()).field("counts", counts).field(valueLabel, values).endObject();
            }
        }
    }
}
