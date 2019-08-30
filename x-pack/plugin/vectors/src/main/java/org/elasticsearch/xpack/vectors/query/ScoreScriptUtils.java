/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder.sortSparseDimsFloatValues;

public class ScoreScriptUtils {

    //**************FUNCTIONS FOR DENSE VECTORS

    /**
     * Calculate l1 norm - Manhattan distance
     * between a query's dense vector and documents' dense vectors
     *
     * @param queryVector the query vector parsed as {@code List<Number>} from json
     * @param dvs VectorScriptDocValues representing encoded documents' vectors
    */
    public static double l1norm(List<Number> queryVector, VectorScriptDocValues.DenseVectorScriptDocValues dvs){
        BytesRef value = dvs.getEncodedValue();
        float[] docVector = VectorEncoderDecoder.decodeDenseVector(value);
        if (queryVector.size() != docVector.length) {
            throw new IllegalArgumentException("Can't calculate l1norm! The number of dimensions of the query vector [" +
                queryVector.size() + "] is different from the documents' vectors [" + docVector.length + "].");
        }
        Iterator<Number> queryVectorIter = queryVector.iterator();
        double l1norm = 0;
        for (int dim = 0; dim < docVector.length; dim++){
            l1norm += Math.abs(queryVectorIter.next().floatValue() - docVector[dim]);
        }
        return l1norm;
    }

    /**
     * Calculate l2 norm - Euclidean distance
     * between a query's dense vector and documents' dense vectors
     *
     * @param queryVector the query vector parsed as {@code List<Number>} from json
     * @param dvs VectorScriptDocValues representing encoded documents' vectors
    */
    public static double l2norm(List<Number> queryVector, VectorScriptDocValues.DenseVectorScriptDocValues dvs){
        BytesRef value = dvs.getEncodedValue();
        float[] docVector = VectorEncoderDecoder.decodeDenseVector(value);
        if (queryVector.size() != docVector.length) {
            throw new IllegalArgumentException("Can't calculate l2norm! The number of dimensions of the query vector [" +
                queryVector.size() + "] is different from the documents' vectors [" + docVector.length + "].");
        }
        Iterator<Number> queryVectorIter = queryVector.iterator();
        double l2norm = 0;
        for (int dim = 0; dim < docVector.length; dim++){
            double diff = queryVectorIter.next().floatValue() - docVector[dim];
            l2norm += diff * diff;
        }
        return Math.sqrt(l2norm);
    }


    /**
     * Calculate a dot product between a query's dense vector and documents' dense vectors
     *
     * @param queryVector the query vector parsed as {@code List<Number>} from json
     * @param dvs VectorScriptDocValues representing encoded documents' vectors
     */
    public static double dotProduct(List<Number> queryVector, VectorScriptDocValues.DenseVectorScriptDocValues dvs){
        BytesRef value = dvs.getEncodedValue();
        float[] docVector = VectorEncoderDecoder.decodeDenseVector(value);
        if (queryVector.size() != docVector.length) {
            throw new IllegalArgumentException("Can't calculate dotProduct! The number of dimensions of the query vector [" +
                queryVector.size() + "] is different from the documents' vectors [" + docVector.length + "].");
        }
        return intDotProduct(queryVector, docVector);
    }

    /**
     * Calculate cosine similarity between a query's dense vector and documents' dense vectors
     *
     * CosineSimilarity is implemented as a class to use
     * painless script caching to calculate queryVectorMagnitude
     * only once per script execution for all documents.
     * A user will call `cosineSimilarity(params.queryVector, doc['my_vector'])`
     */
    public static final class CosineSimilarity {
        final double queryVectorMagnitude;
        final List<Number> queryVector;

        // calculate queryVectorMagnitude once per query execution
        public CosineSimilarity(List<Number> queryVector) {
            this.queryVector = queryVector;

            double dotProduct = 0;
            for (Number value : queryVector) {
                float floatValue = value.floatValue();
                dotProduct += floatValue * floatValue;
            }
            this.queryVectorMagnitude = Math.sqrt(dotProduct);
        }

        public double cosineSimilarity(VectorScriptDocValues.DenseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            float[] docVector = VectorEncoderDecoder.decodeDenseVector(value);
            if (queryVector.size() != docVector.length) {
                throw new IllegalArgumentException("Can't calculate cosineSimilarity! The number of dimensions of the query vector [" +
                    queryVector.size() + "] is different from the documents' vectors [" + docVector.length + "].");
            }

            // calculate docVector magnitude
            double dotProduct = 0f;
            for (int dim = 0; dim < docVector.length; dim++) {
                dotProduct += (double) docVector[dim] * docVector[dim];
            }
            final double docVectorMagnitude = Math.sqrt(dotProduct);

            double docQueryDotProduct = intDotProduct(queryVector, docVector);
            return docQueryDotProduct / (docVectorMagnitude * queryVectorMagnitude);
        }
    }

    private static double intDotProduct(List<Number> v1, float[] v2){
        double v1v2DotProduct = 0;
        Iterator<Number> v1Iter = v1.iterator();
        for (int dim = 0; dim < v2.length; dim++) {
            v1v2DotProduct += v1Iter.next().floatValue() * v2[dim];
        }
        return v1v2DotProduct;
    }


    //**************FUNCTIONS FOR SPARSE VECTORS

    public static class VectorSparseFunctions {
        final float[] queryValues;
        final int[] queryDims;

        // prepare queryVector once per script execution
        // queryVector represents a map of dimensions to values
        public VectorSparseFunctions(Map<String, Number> queryVector) {
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
    }

    /**
     * Calculate l1 norm - Manhattan distance
     * between a query's sparse vector and documents' sparse vectors
     *
     * L1NormSparse is implemented as a class to use
     * painless script caching to prepare queryVector
     * only once per script execution for all documents.
     * A user will call `l1normSparse(params.queryVector, doc['my_vector'])`
     */
    public static final class L1NormSparse extends VectorSparseFunctions {
        public L1NormSparse(Map<String, Number> queryVector) {
            super(queryVector);
        }

        public double l1normSparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(value);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(value);
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

    /**
     * Calculate l2 norm - Euclidean distance
     * between a query's sparse vector and documents' sparse vectors
     *
     * L2NormSparse is implemented as a class to use
     * painless script caching to prepare queryVector
     * only once per script execution for all documents.
     * A user will call `l2normSparse(params.queryVector, doc['my_vector'])`
     */
    public static final class L2NormSparse extends VectorSparseFunctions {
        public L2NormSparse(Map<String, Number> queryVector) {
           super(queryVector);
        }

        public double l2normSparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(value);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(value);
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

    /**
     * Calculate a dot product between a query's sparse vector and documents' sparse vectors
     *
     * DotProductSparse is implemented as a class to use
     * painless script caching to prepare queryVector
     * only once per script execution for all documents.
     * A user will call `dotProductSparse(params.queryVector, doc['my_vector'])`
     */
    public static final class DotProductSparse extends VectorSparseFunctions {
        public DotProductSparse(Map<String, Number> queryVector) {
           super(queryVector);
        }

        public double dotProductSparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(value);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(value);
            return intDotProductSparse(queryValues, queryDims, docValues, docDims);
        }
    }

    /**
     * Calculate cosine similarity between a query's sparse vector and documents' sparse vectors
     *
     * CosineSimilaritySparse is implemented as a class to use
     * painless script caching to prepare queryVector and calculate queryVectorMagnitude
     * only once per script execution for all documents.
     * A user will call `cosineSimilaritySparse(params.queryVector, doc['my_vector'])`
     */
    public static final class CosineSimilaritySparse extends VectorSparseFunctions {
        final double queryVectorMagnitude;

        public CosineSimilaritySparse(Map<String, Number> queryVector) {
            super(queryVector);
            double dotProduct = 0;
            for (int i = 0; i< queryDims.length; i++) {
                dotProduct +=  queryValues[i] *  queryValues[i];
            }
            this.queryVectorMagnitude = Math.sqrt(dotProduct);
        }

        public double cosineSimilaritySparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            int[] docDims = VectorEncoderDecoder.decodeSparseVectorDims(value);
            float[] docValues = VectorEncoderDecoder.decodeSparseVector(value);

            // calculate docVector magnitude
            double dotProduct = 0;
            for (float docValue : docValues) {
                dotProduct += (double) docValue * docValue;
            }
            final double docVectorMagnitude = Math.sqrt(dotProduct);

            double docQueryDotProduct = intDotProductSparse(queryValues, queryDims, docValues, docDims);
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
