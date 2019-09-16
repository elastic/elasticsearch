/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder.sortSparseDimsFloatValues;

public class ScoreScriptUtils {

    //**************FUNCTIONS FOR DENSE VECTORS
    // Functions are implemented as classes to accept a hidden parameter scoreScript that contains some index settings.
    // Also, constructors for some functions accept queryVector to calculate and cache queryVectorMagnitude only once
    // per script execution for all documents.

    public static class DenseVectorFunction {
        final ScoreScript scoreScript;
        final float[] queryVector;

        public DenseVectorFunction(ScoreScript scoreScript, List<Number> queryVector) {
            this(scoreScript, queryVector, false);
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
                                   boolean normalizeQuery) {
            this.scoreScript = scoreScript;

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

        public void validateDocVector(BytesRef vector) {
            if (vector == null) {
                throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
            }

            int vectorLength = VectorEncoderDecoder.denseVectorLength(scoreScript._getIndexVersion(), vector);
            if (queryVector.length != vectorLength) {
                throw new IllegalArgumentException("The query vector has a different number of dimensions [" +
                    queryVector.length + "] than the document vectors [" + vectorLength + "].");
            }
        }
    }

    // Calculate l1 norm (Manhattan distance) between a query's dense vector and documents' dense vectors
    public static final class L1Norm extends DenseVectorFunction {

        public L1Norm(ScoreScript scoreScript, List<Number> queryVector) {
            super(scoreScript, queryVector);
        }

        public double l1norm(VectorScriptDocValues.DenseVectorScriptDocValues dvs) {
            BytesRef vector = dvs.getEncodedValue();
            validateDocVector(vector);
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

        public L2Norm(ScoreScript scoreScript, List<Number> queryVector) {
            super(scoreScript, queryVector);
        }

        public double l2norm(VectorScriptDocValues.DenseVectorScriptDocValues dvs) {
            BytesRef vector = dvs.getEncodedValue();
            validateDocVector(vector);
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

        public DotProduct(ScoreScript scoreScript, List<Number> queryVector) {
            super(scoreScript, queryVector);
        }

        public double dotProduct(VectorScriptDocValues.DenseVectorScriptDocValues dvs){
            BytesRef vector = dvs.getEncodedValue();
            validateDocVector(vector);
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

        public CosineSimilarity(ScoreScript scoreScript, List<Number> queryVector) {
            super(scoreScript, queryVector, true);
        }

        public double cosineSimilarity(VectorScriptDocValues.DenseVectorScriptDocValues dvs) {
            BytesRef vector = dvs.getEncodedValue();
            validateDocVector(vector);

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

    //**************FUNCTIONS FOR SPARSE VECTORS
    // Functions are implemented as classes to accept a hidden parameter scoreScript that contains some index settings.
    // Also, constructors for some functions accept queryVector to calculate and cache queryVectorMagnitude only once
    // per script execution for all documents.

    public static class SparseVectorFunction {
        final ScoreScript scoreScript;
        final float[] queryValues;
        final int[] queryDims;

        // prepare queryVector once per script execution
        // queryVector represents a map of dimensions to values
        public SparseVectorFunction(ScoreScript scoreScript, Map<String, Number> queryVector) {
            this.scoreScript = scoreScript;
            //break vector into two arrays dims and values
            int n = queryVector.size();
            queryValues = new float[n];
            queryDims = new int[n];
            int i = 0;
            for (Map.Entry<String, Number> dimValue : queryVector.entrySet()) {
                try {
                    queryDims[i] = Integer.parseInt(dimValue.getKey());
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("Failed to parse a query vector dimension, it must be an integer!", e);
                }
                queryValues[i] = dimValue.getValue().floatValue();
                i++;
            }
            // Sort dimensions in the ascending order and sort values in the same order as their corresponding dimensions
            sortSparseDimsFloatValues(queryDims, queryValues, n);
        }

        public void validateDocVector(BytesRef vector) {
            if (vector == null) {
                throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
            }
        }
    }

    // Calculate l1 norm (Manhattan distance) between a query's sparse vector and documents' sparse vectors
    public static final class L1NormSparse extends SparseVectorFunction {
        public L1NormSparse(ScoreScript scoreScript,Map<String, Number> queryVector) {
            super(scoreScript, queryVector);
        }

        public double l1normSparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef vector = dvs.getEncodedValue();
            validateDocVector(vector);

            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(scoreScript._getIndexVersion(), vector);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(scoreScript._getIndexVersion(), vector);
            int queryIndex = 0;
            int docIndex = 0;
            double l1norm = 0;
            while (queryIndex < queryDims.length && docIndex < docDims.length) {
                if (queryDims[queryIndex] == docDims[docIndex]) {
                    l1norm += Math.abs(queryValues[queryIndex] - docValues[docIndex]);
                    queryIndex++;
                    docIndex++;
                } else if (queryDims[queryIndex] > docDims[docIndex]) {
                    l1norm += Math.abs(docValues[docIndex]); // 0 for missing query dim
                    docIndex++;
                } else {
                    l1norm += Math.abs(queryValues[queryIndex]); // 0 for missing doc dim
                    queryIndex++;
                }
            }
            while (queryIndex < queryDims.length) {
                l1norm += Math.abs(queryValues[queryIndex]); // 0 for missing doc dim
                queryIndex++;
            }
            while (docIndex < docDims.length) {
                l1norm += Math.abs(docValues[docIndex]); // 0 for missing query dim
                docIndex++;
            }
            return l1norm;
        }
    }

    // Calculate l2 norm (Euclidean distance) between a query's sparse vector and documents' sparse vectors
    public static final class L2NormSparse extends SparseVectorFunction {
        public L2NormSparse(ScoreScript scoreScript, Map<String, Number> queryVector) {
           super(scoreScript, queryVector);
        }

        public double l2normSparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef vector = dvs.getEncodedValue();
            validateDocVector(vector);

            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(scoreScript._getIndexVersion(), vector);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(scoreScript._getIndexVersion(), vector);
            int queryIndex = 0;
            int docIndex = 0;
            double l2norm = 0;
            while (queryIndex < queryDims.length && docIndex < docDims.length) {
                if (queryDims[queryIndex] == docDims[docIndex]) {
                    double diff = queryValues[queryIndex] - docValues[docIndex];
                    l2norm += diff * diff;
                    queryIndex++;
                    docIndex++;
                } else if (queryDims[queryIndex] > docDims[docIndex]) {
                    double diff = docValues[docIndex]; // 0 for missing query dim
                    l2norm += diff * diff;
                    docIndex++;
                } else {
                    double diff = queryValues[queryIndex]; // 0 for missing doc dim
                    l2norm += diff * diff;
                    queryIndex++;
                }
            }
            while (queryIndex < queryDims.length) {
                l2norm += queryValues[queryIndex] * queryValues[queryIndex]; // 0 for missing doc dims
                queryIndex++;
            }
            while (docIndex < docDims.length) {
                l2norm += docValues[docIndex]* docValues[docIndex]; // 0 for missing query dims
                docIndex++;
            }
            return Math.sqrt(l2norm);
        }
    }

    // Calculate a dot product between a query's sparse vector and documents' sparse vectors
    public static final class DotProductSparse extends SparseVectorFunction {
        public DotProductSparse(ScoreScript scoreScript, Map<String, Number> queryVector) {
           super(scoreScript, queryVector);
        }

        public double dotProductSparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef vector = dvs.getEncodedValue();
            validateDocVector(vector);

            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(scoreScript._getIndexVersion(), vector);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(scoreScript._getIndexVersion(), vector);
            return intDotProductSparse(queryValues, queryDims, docValues, docDims);
        }
    }

    // Calculate cosine similarity between a query's sparse vector and documents' sparse vectors
    public static final class CosineSimilaritySparse extends SparseVectorFunction {
        final double queryVectorMagnitude;

        public CosineSimilaritySparse(ScoreScript scoreScript, Map<String, Number> queryVector) {
            super(scoreScript, queryVector);
            double dotProduct = 0;
            for (int i = 0; i< queryDims.length; i++) {
                dotProduct +=  queryValues[i] *  queryValues[i];
            }
            this.queryVectorMagnitude = Math.sqrt(dotProduct);
        }

        public double cosineSimilaritySparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef vector = dvs.getEncodedValue();
            validateDocVector(vector);

            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(scoreScript._getIndexVersion(), vector);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(scoreScript._getIndexVersion(), vector);

            double docQueryDotProduct = intDotProductSparse(queryValues, queryDims, docValues, docDims);
            double docVectorMagnitude = 0.0f;
            if (scoreScript._getIndexVersion().onOrAfter(Version.V_7_5_0)) {
                docVectorMagnitude = VectorEncoderDecoder.decodeVectorMagnitude(scoreScript._getIndexVersion(), vector);
            } else {
                for (float docValue : docValues) {
                    docVectorMagnitude += docValue * docValue;
                }
                docVectorMagnitude = (float) Math.sqrt(docVectorMagnitude);
            }

            return docQueryDotProduct / (docVectorMagnitude * queryVectorMagnitude);
        }
    }

    private static double intDotProductSparse(float[] v1Values, int[] v1Dims, float[] v2Values, int[] v2Dims) {
        double v1v2DotProduct = 0;
        int v1Index = 0;
        int v2Index = 0;
        // find common dimensions among vectors v1 and v2 and calculate dotProduct based on common dimensions
        while (v1Index < v1Values.length && v2Index < v2Values.length) {
            if (v1Dims[v1Index] == v2Dims[v2Index]) {
                v1v2DotProduct += v1Values[v1Index] * v2Values[v2Index];
                v1Index++;
                v2Index++;
            } else if (v1Dims[v1Index] > v2Dims[v2Index]) {
                v2Index++;
            } else {
                v1Index++;
            }
        }
        return v1v2DotProduct;
    }
}
