/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.script.field.vectors.DenseVector;
import org.elasticsearch.script.field.vectors.DenseVectorDocValuesField;

import java.io.IOException;
import java.util.List;

public class VectorScoreScriptUtils {

    public static class DenseVectorFunction {
        protected final ScoreScript scoreScript;
        protected final DenseVectorDocValuesField field;

        public DenseVectorFunction(ScoreScript scoreScript, DenseVectorDocValuesField field) {
            this.scoreScript = scoreScript;
            this.field = field;
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

    public static class ByteDenseVectorFunction extends DenseVectorFunction {
        protected final byte[] queryVector;
        protected final float qvMagnitude;

        /**
         * Constructs a dense vector function used for byte-sized vectors.
         *
         * @param scoreScript The script in which this function was referenced.
         * @param field The vector field.
         * @param queryVector The query vector.
         */
        public ByteDenseVectorFunction(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field);
            DenseVector.checkDimensions(field.get().getDims(), queryVector.size());
            this.queryVector = new byte[queryVector.size()];
            float[] validateValues = new float[queryVector.size()];
            int queryMagnitude = 0;
            for (int i = 0; i < queryVector.size(); i++) {
                final Number number = queryVector.get(i);
                byte value = number.byteValue();
                this.queryVector[i] = value;
                queryMagnitude += value * value;
                validateValues[i] = number.floatValue();
            }
            this.qvMagnitude = (float) Math.sqrt(queryMagnitude);
            field.getElementType().checkVectorBounds(validateValues);
        }
    }

    public static class FloatDenseVectorFunction extends DenseVectorFunction {
        protected final float[] queryVector;

        /**
         * Constructs a dense vector function used for float vectors.
         *
         * @param scoreScript The script in which this function was referenced.
         * @param field The vector field.
         * @param queryVector The query vector.
         * @param normalizeQuery Whether the provided query should be normalized to unit length.
         */
        public FloatDenseVectorFunction(
            ScoreScript scoreScript,
            DenseVectorDocValuesField field,
            List<Number> queryVector,
            boolean normalizeQuery
        ) {
            super(scoreScript, field);
            DenseVector.checkDimensions(field.get().getDims(), queryVector.size());

            this.queryVector = new float[queryVector.size()];
            double queryMagnitude = 0.0;
            for (int i = 0; i < queryVector.size(); i++) {
                float value = queryVector.get(i).floatValue();
                this.queryVector[i] = value;
                queryMagnitude += value * value;
            }
            queryMagnitude = Math.sqrt(queryMagnitude);
            field.getElementType().checkVectorBounds(this.queryVector);

            if (normalizeQuery) {
                for (int dim = 0; dim < this.queryVector.length; dim++) {
                    this.queryVector[dim] /= (float) queryMagnitude;
                }
            }
        }
    }

    // Calculate l1 norm (Manhattan distance) between a query's dense vector and documents' dense vectors
    public interface L1NormInterface {
        double l1norm();
    }

    public static class ByteL1Norm extends ByteDenseVectorFunction implements L1NormInterface {

        public ByteL1Norm(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector);
        }

        public double l1norm() {
            setNextVector();
            return field.get().l1Norm(queryVector);
        }
    }

    public static class FloatL1Norm extends FloatDenseVectorFunction implements L1NormInterface {

        public FloatL1Norm(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector, false);
        }

        public double l1norm() {
            setNextVector();
            return field.get().l1Norm(queryVector);
        }
    }

    public static final class L1Norm {

        private final L1NormInterface function;

        public L1Norm(ScoreScript scoreScript, List<Number> queryVector, String fieldName) {
            DenseVectorDocValuesField field = (DenseVectorDocValuesField) scoreScript.field(fieldName);
            function = switch (field.getElementType()) {
                case BYTE -> new ByteL1Norm(scoreScript, field, queryVector);
                case FLOAT -> new FloatL1Norm(scoreScript, field, queryVector);
            };
        }

        public double l1norm() {
            return function.l1norm();
        }
    }

    // Calculate l2 norm (Manhattan distance) between a query's dense vector and documents' dense vectors
    public interface L2NormInterface {
        double l2norm();
    }

    public static class ByteL2Norm extends ByteDenseVectorFunction implements L2NormInterface {

        public ByteL2Norm(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector);
        }

        public double l2norm() {
            setNextVector();
            return field.get().l2Norm(queryVector);
        }
    }

    public static class FloatL2Norm extends FloatDenseVectorFunction implements L2NormInterface {

        public FloatL2Norm(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector, false);
        }

        public double l2norm() {
            setNextVector();
            return field.get().l2Norm(queryVector);
        }
    }

    public static final class L2Norm {

        private final L2NormInterface function;

        public L2Norm(ScoreScript scoreScript, List<Number> queryVector, String fieldName) {
            DenseVectorDocValuesField field = (DenseVectorDocValuesField) scoreScript.field(fieldName);
            function = switch (field.getElementType()) {
                case BYTE -> new ByteL2Norm(scoreScript, field, queryVector);
                case FLOAT -> new FloatL2Norm(scoreScript, field, queryVector);
            };
        }

        public double l2norm() {
            return function.l2norm();
        }
    }

    // Calculate a dot product between a query's dense vector and documents' dense vectors
    public interface DotProductInterface {
        double dotProduct();
    }

    public static class ByteDotProduct extends ByteDenseVectorFunction implements DotProductInterface {

        public ByteDotProduct(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector);
        }

        public double dotProduct() {
            setNextVector();
            return field.get().dotProduct(queryVector);
        }
    }

    public static class FloatDotProduct extends FloatDenseVectorFunction implements DotProductInterface {

        public FloatDotProduct(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector, false);
        }

        public double dotProduct() {
            setNextVector();
            return field.get().dotProduct(queryVector);
        }
    }

    public static final class DotProduct {

        private final DotProductInterface function;

        public DotProduct(ScoreScript scoreScript, List<Number> queryVector, String fieldName) {
            DenseVectorDocValuesField field = (DenseVectorDocValuesField) scoreScript.field(fieldName);
            function = switch (field.getElementType()) {
                case BYTE -> new ByteDotProduct(scoreScript, field, queryVector);
                case FLOAT -> new FloatDotProduct(scoreScript, field, queryVector);
            };
        }

        public double dotProduct() {
            return function.dotProduct();
        }
    }

    // Calculate cosine similarity between a query's dense vector and documents' dense vectors
    public interface CosineSimilarityInterface {
        double cosineSimilarity();
    }

    public static class ByteCosineSimilarity extends ByteDenseVectorFunction implements CosineSimilarityInterface {

        public ByteCosineSimilarity(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector);
        }

        public double cosineSimilarity() {
            setNextVector();
            return field.get().cosineSimilarity(queryVector, qvMagnitude);
        }
    }

    public static class FloatCosineSimilarity extends FloatDenseVectorFunction implements CosineSimilarityInterface {

        public FloatCosineSimilarity(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector, true);
        }

        public double cosineSimilarity() {
            setNextVector();
            return field.get().cosineSimilarity(queryVector, false);
        }
    }

    public static final class CosineSimilarity {

        private final CosineSimilarityInterface function;

        public CosineSimilarity(ScoreScript scoreScript, List<Number> queryVector, String fieldName) {
            DenseVectorDocValuesField field = (DenseVectorDocValuesField) scoreScript.field(fieldName);
            function = switch (field.getElementType()) {
                case BYTE -> new ByteCosineSimilarity(scoreScript, field, queryVector);
                case FLOAT -> new FloatCosineSimilarity(scoreScript, field, queryVector);
            };
        }

        public double cosineSimilarity() {
            return function.cosineSimilarity();
        }
    }
}
