/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics;

import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.DoubleHistogramIterationValue;
import org.apache.lucene.document.BinaryDocValuesField;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.tdigest.Centroid;

import java.io.IOException;

public final class AnalyticsTestsUtils {

    /**
     * Generates an index fields for histogram fields. Used in tests of aggregations that work on histogram fields.
     */
    public static BinaryDocValuesField histogramFieldDocValues(String fieldName, double[] values) throws IOException {
        TDigestState histogram = TDigestState.create(100.0); // default
        for (double value : values) {
            histogram.add(value);
        }
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        histogram.compress();
        for (Centroid centroid : histogram.centroids()) {
            streamOutput.writeVInt(centroid.count());
            streamOutput.writeDouble(centroid.mean());
        }
        return new BinaryDocValuesField(fieldName, streamOutput.bytes().toBytesRef());
    }

    public static BinaryDocValuesField hdrHistogramFieldDocValues(String fieldName, double[] values) throws IOException {
        DoubleHistogram histogram = new DoubleHistogram(3);
        histogram.setAutoResize(true);
        for (double value : values) {
            histogram.recordValue(value);
        }
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        for (DoubleHistogramIterationValue value : histogram.recordedValues()) {
            streamOutput.writeVInt((int) value.getCountAtValueIteratedTo());
            streamOutput.writeDouble(value.getValueIteratedTo());
        }
        return new BinaryDocValuesField(fieldName, streamOutput.bytes().toBytesRef());
    }

    public static BinaryDocValuesField histogramFieldDocValues(String fieldName, double[] values, int[] counts) throws IOException {
        assert values.length == counts.length;
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        for (int i = 0; i < values.length; i++) {
            streamOutput.writeVInt(counts[i]);
            streamOutput.writeDouble(values[i]);
        }
        return new BinaryDocValuesField(fieldName, streamOutput.bytes().toBytesRef());
    }

}
