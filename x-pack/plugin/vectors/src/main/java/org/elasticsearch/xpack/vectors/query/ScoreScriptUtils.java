/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ScoreScriptUtils {

    public static class DenseVectorFunction {
        final ScoreScript scoreScript;
        final float[] queryVector;
        final DenseVectorScriptDocValues docValues;

        public DenseVectorFunction(ScoreScript scoreScript,
                                   List<Number> queryVector,
                                   String field) {
            this(scoreScript, queryVector, field, false);
        }

        /**
         * Constructs a dense vector function.
         *
         * @param scoreScript The script in which this function was referenced.
         * @param queryVector The query vector.
         * @param normalizeQuery Whether the provided query should be normalized to unit length.
         */
        public DenseVectorFunction(ScoreScript scoreScript,
                                   List<Number> queryVector,
                                   String field,
                                   boolean normalizeQuery) {
            this.scoreScript = scoreScript;
            this.docValues = (DenseVectorScriptDocValues) scoreScript.getDoc().get(field);

            this.queryVector = new float[queryVector.size()];
            double queryMagnitude = 0.0;
            for (int i = 0; i < queryVector.size(); i++) {
                float value = queryVector.get(i).floatValue();
                this.queryVector[i] = value;
                queryMagnitude += value * value;
            }
            queryMagnitude = Math.sqrt(queryMagnitude);

            if (normalizeQuery) {
                for (int dim = 0; dim < this.queryVector.length; dim++) {
                    this.queryVector[dim] /= queryMagnitude;
                }
            }
        }

        BytesRef getEncodedVector() {
            try {
                docValues.setNextDocId(scoreScript._getDocId());
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }

            // Validate the encoded vector's length.
            BytesRef vector = docValues.getEncodedValue();
            if (vector == null) {
                throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
            }

            int vectorLength = VectorEncoderDecoder.denseVectorLength(scoreScript._getIndexVersion(), vector);
            if (queryVector.length != vectorLength) {
                throw new IllegalArgumentException("The query vector has a different number of dimensions [" +
                    queryVector.length + "] than the document vectors [" + vectorLength + "].");
            }
            return vector;
        }
    }

    // Calculate l1 norm (Manhattan distance) between a query's dense vector and documents' dense vectors
    public static final class L1Norm extends DenseVectorFunction {

        public L1Norm(ScoreScript scoreScript, List<Number> queryVector, String field) {
            super(scoreScript, queryVector, field);
        }

        public double l1norm() {
            BytesRef vector = getEncodedVector();
            ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);

            double l1norm = 0;

            for (float queryValue : queryVector) {
                l1norm += Math.abs(queryValue - byteBuffer.getFloat());
            }
            return l1norm;
        }
    }

    // Calculate l2 norm (Euclidean distance) between a query's dense vector and documents' dense vectors
    public static final class L2Norm extends DenseVectorFunction {

        public L2Norm(ScoreScript scoreScript, List<Number> queryVector, String field) {
            super(scoreScript, queryVector, field);
        }

        public double l2norm() {
            BytesRef vector = getEncodedVector();
            ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);

            double l2norm = 0;
            for (float queryValue : queryVector) {
                double diff = queryValue - byteBuffer.getFloat();
                l2norm += diff * diff;
            }
            return Math.sqrt(l2norm);
        }
    }

    // Calculate a dot product between a query's dense vector and documents' dense vectors
    public static final class DotProduct extends DenseVectorFunction {

        public DotProduct(ScoreScript scoreScript, List<Number> queryVector, String field) {
            super(scoreScript, queryVector, field);
        }

        public double dotProduct() {
            BytesRef vector = getEncodedVector();
            ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);

            double dotProduct = 0;
            for (float queryValue : queryVector) {
                dotProduct += queryValue * byteBuffer.getFloat();
            }
            return dotProduct;
        }
    }

    // Calculate cosine similarity between a query's dense vector and documents' dense vectors
    public static final class CosineSimilarity extends DenseVectorFunction {

        public CosineSimilarity(ScoreScript scoreScript, List<Number> queryVector, String field) {
            super(scoreScript, queryVector, field, true);
        }

        public double cosineSimilarity() {
            BytesRef vector = getEncodedVector();
            ByteBuffer byteBuffer = ByteBuffer.wrap(vector.bytes, vector.offset, vector.length);

            double dotProduct = 0.0;
            double vectorMagnitude = 0.0f;
            if (scoreScript._getIndexVersion().onOrAfter(Version.V_7_5_0)) {
                for (float queryValue : queryVector) {
                    dotProduct += queryValue * byteBuffer.getFloat();
                }
                vectorMagnitude = VectorEncoderDecoder.decodeVectorMagnitude(scoreScript._getIndexVersion(), vector);
            } else {
                for (float queryValue : queryVector) {
                    float docValue = byteBuffer.getFloat();
                    dotProduct += queryValue * docValue;
                    vectorMagnitude += docValue * docValue;
                }
                vectorMagnitude = (float) Math.sqrt(vectorMagnitude);
            }
            return dotProduct / vectorMagnitude;
        }
    }
}
