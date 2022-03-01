/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.script.ScoreScript;

import java.io.IOException;
import java.util.List;

public class ScoreScriptUtils {

    public static class DenseVectorFunction {
        final ScoreScript scoreScript;
        final float[] queryVector;
        final DenseVectorDocValuesField field;

        public DenseVectorFunction(ScoreScript scoreScript, List<Number> queryVector, String fieldName) {
            this(scoreScript, queryVector, fieldName, false);
        }

        /**
         * Constructs a dense vector function.
         *
         * @param scoreScript The script in which this function was referenced.
         * @param queryVector The query vector.
         * @param normalizeQuery Whether the provided query should be normalized to unit length.
         */
        public DenseVectorFunction(ScoreScript scoreScript, List<Number> queryVector, String fieldName, boolean normalizeQuery) {
            this.scoreScript = scoreScript;
            this.field = (DenseVectorDocValuesField) scoreScript.field(fieldName);
            DenseVector.checkDimensions(field.get().getDims(), queryVector.size());

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

        void setNextVector() {
            try {
                field.setNextDocId(scoreScript._getDocId());
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
            if (field.isEmpty()) {
                throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
            }
        }
    }

    // Calculate l1 norm (Manhattan distance) between a query's dense vector and documents' dense vectors
    public static final class L1Norm extends DenseVectorFunction {

        public L1Norm(ScoreScript scoreScript, List<Number> queryVector, String field) {
            super(scoreScript, queryVector, field);
        }

        public double l1norm() {
            setNextVector();
            return field.get().l1Norm(queryVector);
        }
    }

    // Calculate l2 norm (Euclidean distance) between a query's dense vector and documents' dense vectors
    public static final class L2Norm extends DenseVectorFunction {

        public L2Norm(ScoreScript scoreScript, List<Number> queryVector, String field) {
            super(scoreScript, queryVector, field);
        }

        public double l2norm() {
            setNextVector();
            return field.get().l2Norm(queryVector);
        }
    }

    // Calculate a dot product between a query's dense vector and documents' dense vectors
    public static final class DotProduct extends DenseVectorFunction {

        public DotProduct(ScoreScript scoreScript, List<Number> queryVector, String field) {
            super(scoreScript, queryVector, field);
        }

        public double dotProduct() {
            setNextVector();
            return field.get().dotProduct(queryVector);
        }
    }

    // Calculate cosine similarity between a query's dense vector and documents' dense vectors
    public static final class CosineSimilarity extends DenseVectorFunction {

        public CosineSimilarity(ScoreScript scoreScript, List<Number> queryVector, String field) {
            super(scoreScript, queryVector, field, true);
        }

        public double cosineSimilarity() {
            setNextVector();
            // query vector normalized in constructor
            return field.get().cosineSimilarity(queryVector, false);
        }
    }
}
