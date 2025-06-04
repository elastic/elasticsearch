/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.script.field.vectors.DenseVector;
import org.elasticsearch.script.field.vectors.DenseVectorDocValuesField;

import java.io.IOException;
import java.util.HexFormat;
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
        // either byteQueryVector or floatQueryVector will be non-null
        protected final byte[] byteQueryVector;
        protected final float[] floatQueryVector;
        // only valid if byteQueryVector is used
        protected final float qvMagnitude;

        /**
         * Constructs a dense vector function used for byte-sized vectors.
         *
         * @param scoreScript The script in which this function was referenced.
         * @param field The vector field.
         * @param queryVector The query vector.
         * @param normalizeFloatQuery {@code true} if the query vector is a float vector, then normalize it.
         * @param allowedTypes The types the vector is allowed to be.
         */
        public ByteDenseVectorFunction(
            ScoreScript scoreScript,
            DenseVectorDocValuesField field,
            List<Number> queryVector,
            boolean normalizeFloatQuery,
            ElementType... allowedTypes
        ) {
            super(scoreScript, field);
            field.getElementType().checkDimensions(field.get().getDims(), queryVector.size());
            float[] floatValues = new float[queryVector.size()];
            double queryMagnitude = 0;
            for (int i = 0; i < queryVector.size(); i++) {
                float value = queryVector.get(i).floatValue();
                floatValues[i] = value;
                queryMagnitude += value * value;
            }
            queryMagnitude = Math.sqrt(queryMagnitude);

            switch (ElementType.checkValidVector(floatValues, allowedTypes)) {
                case FLOAT:
                    byteQueryVector = null;
                    floatQueryVector = floatValues;
                    qvMagnitude = -1;   // invalid valid, not used for float vectors

                    if (normalizeFloatQuery) {
                        for (int i = 0; i < floatQueryVector.length; i++) {
                            floatQueryVector[i] /= (float) queryMagnitude;
                        }
                    }
                    break;
                case BYTE:
                    floatQueryVector = null;
                    byteQueryVector = new byte[floatValues.length];
                    for (int i = 0; i < floatValues.length; i++) {
                        byteQueryVector[i] = (byte) floatValues[i];
                    }
                    this.qvMagnitude = (float) queryMagnitude;
                    break;
                default:
                    throw new AssertionError("Unexpected element type");
            }

        }

        /**
         * Constructs a dense vector function used for byte-sized vectors.
         *
         * @param scoreScript The script in which this function was referenced.
         * @param field The vector field.
         * @param queryVector The query vector.
         */
        public ByteDenseVectorFunction(ScoreScript scoreScript, DenseVectorDocValuesField field, byte[] queryVector) {
            super(scoreScript, field);
            byteQueryVector = queryVector;
            floatQueryVector = null;
            double queryMagnitude = 0.0f;
            for (byte value : queryVector) {
                queryMagnitude += value * value;
            }
            this.qvMagnitude = (float) Math.sqrt(queryMagnitude);
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
            super(scoreScript, field, queryVector, false, ElementType.BYTE);
        }

        public ByteL1Norm(ScoreScript scoreScript, DenseVectorDocValuesField field, byte[] queryVector) {
            super(scoreScript, field, queryVector);
        }

        public double l1norm() {
            setNextVector();
            return field.get().l1Norm(byteQueryVector);
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

        @SuppressWarnings("unchecked")
        public L1Norm(ScoreScript scoreScript, Object queryVector, String fieldName) {
            DenseVectorDocValuesField field = (DenseVectorDocValuesField) scoreScript.field(fieldName);
            function = switch (field.getElementType()) {
                case BYTE, BIT -> {
                    if (queryVector instanceof List) {
                        yield new ByteL1Norm(scoreScript, field, (List<Number>) queryVector);
                    } else if (queryVector instanceof String s) {
                        byte[] parsedQueryVector = HexFormat.of().parseHex(s);
                        yield new ByteL1Norm(scoreScript, field, parsedQueryVector);
                    }
                    throw new IllegalArgumentException("Unsupported input object for byte vectors: " + queryVector.getClass().getName());
                }
                case FLOAT -> {
                    if (queryVector instanceof List) {
                        yield new FloatL1Norm(scoreScript, field, (List<Number>) queryVector);
                    }
                    throw new IllegalArgumentException("Unsupported input object for float vectors: " + queryVector.getClass().getName());
                }
            };
        }

        public double l1norm() {
            return function.l1norm();
        }
    }

    // Calculate Hamming distances between a query's dense vector and documents' dense vectors
    public interface HammingDistanceInterface {
        int hamming();
    }

    public static class ByteHammingDistance extends ByteDenseVectorFunction implements HammingDistanceInterface {

        public ByteHammingDistance(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector, false, ElementType.BYTE);
        }

        public ByteHammingDistance(ScoreScript scoreScript, DenseVectorDocValuesField field, byte[] queryVector) {
            super(scoreScript, field, queryVector);
        }

        public int hamming() {
            setNextVector();
            return field.get().hamming(byteQueryVector);
        }
    }

    public static final class Hamming {

        private final HammingDistanceInterface function;

        @SuppressWarnings("unchecked")
        public Hamming(ScoreScript scoreScript, Object queryVector, String fieldName) {
            DenseVectorDocValuesField field = (DenseVectorDocValuesField) scoreScript.field(fieldName);
            if (field.getElementType() == DenseVectorFieldMapper.ElementType.FLOAT) {
                throw new IllegalArgumentException("hamming distance is only supported for byte or bit vectors");
            }
            if (queryVector instanceof List) {
                function = new ByteHammingDistance(scoreScript, field, (List<Number>) queryVector);
            } else if (queryVector instanceof String s) {
                byte[] parsedQueryVector = HexFormat.of().parseHex(s);
                function = new ByteHammingDistance(scoreScript, field, parsedQueryVector);
            } else {
                throw new IllegalArgumentException("Unsupported input object for byte vectors: " + queryVector.getClass().getName());
            }
        }

        public double hamming() {
            return function.hamming();
        }
    }

    // Calculate l2 norm (Manhattan distance) between a query's dense vector and documents' dense vectors
    public interface L2NormInterface {
        double l2norm();
    }

    public static class ByteL2Norm extends ByteDenseVectorFunction implements L2NormInterface {

        public ByteL2Norm(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector, false, ElementType.BYTE);
        }

        public ByteL2Norm(ScoreScript scoreScript, DenseVectorDocValuesField field, byte[] queryVector) {
            super(scoreScript, field, queryVector);
        }

        public double l2norm() {
            setNextVector();
            return field.get().l2Norm(byteQueryVector);
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

        @SuppressWarnings("unchecked")
        public L2Norm(ScoreScript scoreScript, Object queryVector, String fieldName) {
            DenseVectorDocValuesField field = (DenseVectorDocValuesField) scoreScript.field(fieldName);
            function = switch (field.getElementType()) {
                case BYTE, BIT -> {
                    if (queryVector instanceof List) {
                        yield new ByteL2Norm(scoreScript, field, (List<Number>) queryVector);
                    } else if (queryVector instanceof String s) {
                        byte[] parsedQueryVector = HexFormat.of().parseHex(s);
                        yield new ByteL2Norm(scoreScript, field, parsedQueryVector);
                    }
                    throw new IllegalArgumentException("Unsupported input object for byte vectors: " + queryVector.getClass().getName());
                }
                case FLOAT -> {
                    if (queryVector instanceof List) {
                        yield new FloatL2Norm(scoreScript, field, (List<Number>) queryVector);
                    }
                    throw new IllegalArgumentException("Unsupported input object for float vectors: " + queryVector.getClass().getName());
                }
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

    public static class BitDotProduct extends DenseVectorFunction implements DotProductInterface {
        private final byte[] byteQueryVector;
        private final float[] floatQueryVector;

        public BitDotProduct(ScoreScript scoreScript, DenseVectorDocValuesField field, byte[] queryVector) {
            super(scoreScript, field);
            if (field.getElementType() != DenseVectorFieldMapper.ElementType.BIT) {
                throw new IllegalArgumentException("cannot calculate bit dot product for non-bit vectors");
            }
            int fieldDims = field.get().getDims();
            if (fieldDims != queryVector.length * Byte.SIZE && fieldDims != queryVector.length) {
                throw new IllegalArgumentException(
                    "The query vector has an incorrect number of dimensions. Must be ["
                        + fieldDims / 8
                        + "] for bitwise operations, or ["
                        + fieldDims
                        + "] for byte wise operations: provided ["
                        + queryVector.length
                        + "]."
                );
            }
            this.byteQueryVector = queryVector;
            this.floatQueryVector = null;
        }

        public BitDotProduct(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field);
            if (field.getElementType() != DenseVectorFieldMapper.ElementType.BIT) {
                throw new IllegalArgumentException("cannot calculate bit dot product for non-bit vectors");
            }
            float[] floatQueryVector = new float[queryVector.size()];
            byte[] byteQueryVector = new byte[queryVector.size()];
            boolean isFloat = false;
            for (int i = 0; i < queryVector.size(); i++) {
                Number number = queryVector.get(i);
                floatQueryVector[i] = number.floatValue();
                byteQueryVector[i] = number.byteValue();
                if (isFloat
                    || floatQueryVector[i] % 1.0f != 0.0f
                    || floatQueryVector[i] < Byte.MIN_VALUE
                    || floatQueryVector[i] > Byte.MAX_VALUE) {
                    isFloat = true;
                }
            }
            int fieldDims = field.get().getDims();
            if (isFloat) {
                this.floatQueryVector = floatQueryVector;
                this.byteQueryVector = null;
                if (fieldDims != floatQueryVector.length) {
                    throw new IllegalArgumentException(
                        "The query vector has an incorrect number of dimensions. Must be ["
                            + fieldDims
                            + "] for float wise operations: provided ["
                            + floatQueryVector.length
                            + "]."
                    );
                }
            } else {
                this.floatQueryVector = null;
                this.byteQueryVector = byteQueryVector;
                if (fieldDims != byteQueryVector.length * Byte.SIZE && fieldDims != byteQueryVector.length) {
                    throw new IllegalArgumentException(
                        "The query vector has an incorrect number of dimensions. Must be ["
                            + fieldDims / 8
                            + "] for bitwise operations, or ["
                            + fieldDims
                            + "] for byte wise operations: provided ["
                            + byteQueryVector.length
                            + "]."
                    );
                }
            }
        }

        @Override
        public double dotProduct() {
            setNextVector();
            return byteQueryVector != null ? field.get().dotProduct(byteQueryVector) : field.get().dotProduct(floatQueryVector);
        }
    }

    public static class ByteDotProduct extends ByteDenseVectorFunction implements DotProductInterface {

        public ByteDotProduct(ScoreScript scoreScript, DenseVectorDocValuesField field, List<Number> queryVector) {
            super(scoreScript, field, queryVector, false, ElementType.BYTE, ElementType.FLOAT);
        }

        public ByteDotProduct(ScoreScript scoreScript, DenseVectorDocValuesField field, byte[] queryVector) {
            super(scoreScript, field, queryVector);
        }

        public double dotProduct() {
            setNextVector();
            if (floatQueryVector != null) {
                return field.get().dotProduct(floatQueryVector);
            } else {
                return field.get().dotProduct(byteQueryVector);
            }
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

        @SuppressWarnings("unchecked")
        public DotProduct(ScoreScript scoreScript, Object queryVector, String fieldName) {
            DenseVectorDocValuesField field = (DenseVectorDocValuesField) scoreScript.field(fieldName);
            function = switch (field.getElementType()) {
                case BIT -> {
                    if (queryVector instanceof List) {
                        yield new BitDotProduct(scoreScript, field, (List<Number>) queryVector);
                    } else if (queryVector instanceof String s) {
                        byte[] parsedQueryVector = HexFormat.of().parseHex(s);
                        yield new BitDotProduct(scoreScript, field, parsedQueryVector);
                    }
                    throw new IllegalArgumentException("Unsupported input object for bit vectors: " + queryVector.getClass().getName());
                }
                case BYTE -> {
                    if (queryVector instanceof List) {
                        yield new ByteDotProduct(scoreScript, field, (List<Number>) queryVector);
                    } else if (queryVector instanceof String s) {
                        byte[] parsedQueryVector = HexFormat.of().parseHex(s);
                        yield new ByteDotProduct(scoreScript, field, parsedQueryVector);
                    }
                    throw new IllegalArgumentException("Unsupported input object for byte vectors: " + queryVector.getClass().getName());
                }
                case FLOAT -> {
                    if (queryVector instanceof List) {
                        yield new FloatDotProduct(scoreScript, field, (List<Number>) queryVector);
                    }
                    throw new IllegalArgumentException("Unsupported input object for float vectors: " + queryVector.getClass().getName());
                }
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
            super(scoreScript, field, queryVector, true, ElementType.BYTE, ElementType.FLOAT);
        }

        public ByteCosineSimilarity(ScoreScript scoreScript, DenseVectorDocValuesField field, byte[] queryVector) {
            super(scoreScript, field, queryVector);
        }

        public double cosineSimilarity() {
            setNextVector();
            if (floatQueryVector != null) {
                // float vector is already normalized by the superclass constructor
                return field.get().cosineSimilarity(floatQueryVector, false);
            } else {
                return field.get().cosineSimilarity(byteQueryVector, qvMagnitude);
            }
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

        @SuppressWarnings("unchecked")
        public CosineSimilarity(ScoreScript scoreScript, Object queryVector, String fieldName) {
            DenseVectorDocValuesField field = (DenseVectorDocValuesField) scoreScript.field(fieldName);
            function = switch (field.getElementType()) {
                case BYTE, BIT -> {
                    if (queryVector instanceof List) {
                        yield new ByteCosineSimilarity(scoreScript, field, (List<Number>) queryVector);
                    } else if (queryVector instanceof String s) {
                        byte[] parsedQueryVector = HexFormat.of().parseHex(s);
                        yield new ByteCosineSimilarity(scoreScript, field, parsedQueryVector);
                    }
                    throw new IllegalArgumentException("Unsupported input object for byte vectors: " + queryVector.getClass().getName());
                }
                case FLOAT -> {
                    if (queryVector instanceof List) {
                        yield new FloatCosineSimilarity(scoreScript, field, (List<Number>) queryVector);
                    }
                    throw new IllegalArgumentException("Unsupported input object for float vectors: " + queryVector.getClass().getName());
                }
            };
        }

        public double cosineSimilarity() {
            return function.cosineSimilarity();
        }
    }
}
