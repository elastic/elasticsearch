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

import static org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder.sortSparseDimsDoubleValues;

public class ScoreScriptUtils {

    //**************FUNCTIONS FOR DENSE VECTORS

    /**
     * Calculate a dot product between a query's dense vector and documents' dense vectors
     *
     * @param queryVector the query vector parsed as {@code List<Number>} from json
     * @param dvs VectorScriptDocValues representing encoded documents' vectors
     */
    public static double dotProduct(List<Number> queryVector, VectorScriptDocValues.DenseVectorScriptDocValues dvs){
        BytesRef value = dvs.getEncodedValue();
        if (value == null) return 0;
        float[] docVector = VectorEncoderDecoder.decodeDenseVector(value);
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
            double doubleValue;
            double dotProduct = 0;
            for (Number value : queryVector) {
                doubleValue = value.doubleValue();
                dotProduct += doubleValue * doubleValue;
            }
            this.queryVectorMagnitude = Math.sqrt(dotProduct);
        }

        public double cosineSimilarity(VectorScriptDocValues.DenseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            if (value == null) return 0;
            float[] docVector = VectorEncoderDecoder.decodeDenseVector(value);

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
        int dims = Math.min(v1.size(), v2.length);
        double v1v2DotProduct = 0;
        int dim = 0;
        Iterator<Number> v1Iter = v1.iterator();
        while(dim < dims) {
            v1v2DotProduct += v1Iter.next().doubleValue() * v2[dim];
            dim++;
        }
        return v1v2DotProduct;
    }


    //**************FUNCTIONS FOR SPARSE VECTORS

    /**
     * Calculate a dot product between a query's sparse vector and documents' sparse vectors
     *
     * DotProductSparse is implemented as a class to use
     * painless script caching to prepare queryVector
     * only once per script execution for all documents.
     * A user will call `dotProductSparse(params.queryVector, doc['my_vector'])`
     */
    public static final class DotProductSparse {
        final double[] queryValues;
        final int[] queryDims;

        // prepare queryVector once per script execution
        // queryVector represents a map of dimensions to values
        public DotProductSparse(Map<String, Number> queryVector) {
            //break vector into two arrays dims and values
            int n = queryVector.size();
            queryDims = new int[n];
            queryValues = new double[n];
            int i = 0;
            for (Map.Entry<String, Number> dimValue : queryVector.entrySet()) {
                try {
                    queryDims[i] = Integer.parseInt(dimValue.getKey());
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("Failed to parse a query vector dimension, it must be an integer!", e);
                }
                queryValues[i] = dimValue.getValue().doubleValue();
                i++;
            }
            // Sort dimensions in the ascending order and sort values in the same order as their corresponding dimensions
            sortSparseDimsDoubleValues(queryDims, queryValues, n);
        }

        public double dotProductSparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            if (value == null) return 0;
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
    public static final class CosineSimilaritySparse {
        final double[] queryValues;
        final int[] queryDims;
        final double queryVectorMagnitude;

        // prepare queryVector once per script execution
        public CosineSimilaritySparse(Map<String, Number> queryVector) {
            //break vector into two arrays dims and values
            int n = queryVector.size();
            queryValues = new double[n];
            queryDims = new int[n];
            double dotProduct = 0;
            int i = 0;
            for (Map.Entry<String, Number> dimValue : queryVector.entrySet()) {
                try {
                    queryDims[i] = Integer.parseInt(dimValue.getKey());
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("Failed to parse a query vector dimension, it must be an integer!", e);
                }
                queryValues[i] = dimValue.getValue().doubleValue();
                dotProduct +=  queryValues[i] *  queryValues[i];
                i++;
            }
            this.queryVectorMagnitude = Math.sqrt(dotProduct);
            // Sort dimensions in the ascending order and sort values in the same order as their corresponding dimensions
            sortSparseDimsDoubleValues(queryDims, queryValues, n);
        }

        public double cosineSimilaritySparse(VectorScriptDocValues.SparseVectorScriptDocValues dvs) {
            BytesRef value = dvs.getEncodedValue();
            if (value == null) return 0;
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

    private static double intDotProductSparse(double[] v1Values, int[] v1Dims, float[] v2Values, int[] v2Dims) {
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
