/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */


package org.elasticsearch.xpack.analytics;

import java.io.IOException;

import org.apache.lucene.document.BinaryDocValuesField;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.search.aggregations.metrics.TDigestState;

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;

public final class AnalyticsTestsUtils {

    /**
     * Generates an index fields for histogram fields. Used in tests of aggregations that work on histogram fields.
     */
    public static BinaryDocValuesField histogramFieldDocValues(String fieldName, double[] values) throws IOException {
        TDigest histogram = new TDigestState(100.0); //default
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

}
