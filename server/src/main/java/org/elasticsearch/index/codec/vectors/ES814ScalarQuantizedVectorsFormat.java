/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.FlatVectorsFormat;
import org.apache.lucene.codecs.FlatVectorsReader;
import org.apache.lucene.codecs.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;

public class ES814ScalarQuantizedVectorsFormat extends FlatVectorsFormat {
    public static final String QUANTIZED_VECTOR_COMPONENT = "QVEC";

    static final String NAME = "ES814ScalarQuantizedVectorsFormat";

    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;
    static final String META_CODEC_NAME = "Lucene99ScalarQuantizedVectorsFormatMeta";
    static final String VECTOR_DATA_CODEC_NAME = "Lucene99ScalarQuantizedVectorsFormatData";
    static final String META_EXTENSION = "vemq";
    static final String VECTOR_DATA_EXTENSION = "veq";

    private static final FlatVectorsFormat rawVectorFormat = new Lucene99FlatVectorsFormat();

    /** The minimum confidence interval */
    private static final float MINIMUM_CONFIDENCE_INTERVAL = 0.9f;

    /** The maximum confidence interval */
    private static final float MAXIMUM_CONFIDENCE_INTERVAL = 1f;

    /**
     * Controls the confidence interval used to scalar quantize the vectors the default value is
     * calculated as `1-1/(vector_dimensions + 1)`
     */
    public final Float confidenceInterval;

    public ES814ScalarQuantizedVectorsFormat(Float confidenceInterval) {
        if (confidenceInterval != null
            && (confidenceInterval < MINIMUM_CONFIDENCE_INTERVAL || confidenceInterval > MAXIMUM_CONFIDENCE_INTERVAL)) {
            throw new IllegalArgumentException(
                "confidenceInterval must be between "
                    + MINIMUM_CONFIDENCE_INTERVAL
                    + " and "
                    + MAXIMUM_CONFIDENCE_INTERVAL
                    + "; confidenceInterval="
                    + confidenceInterval
            );
        }
        this.confidenceInterval = confidenceInterval;
    }

    @Override
    public String toString() {
        return NAME + "(name=" + NAME + ", confidenceInterval=" + confidenceInterval + ", rawVectorFormat=" + rawVectorFormat + ")";
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES814ScalarQuantizedVectorsWriter(state, confidenceInterval, rawVectorFormat.fieldsWriter(state));
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES814ScalarQuantizedVectorsReader(new Lucene99ScalarQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state)));
    }

    static class ES814ScalarQuantizedVectorsReader extends FlatVectorsReader {

        final FlatVectorsReader reader;

        ES814ScalarQuantizedVectorsReader(FlatVectorsReader reader) {
            this.reader = reader;
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
            return reader.getRandomVectorScorer(field, target);
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
            return reader.getRandomVectorScorer(field, target);
        }

        @Override
        public void checkIntegrity() throws IOException {
            reader.checkIntegrity();
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            return reader.getFloatVectorValues(field);
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            return reader.getByteVectorValues(field);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public long ramBytesUsed() {
            return reader.ramBytesUsed();
        }
    }
}
