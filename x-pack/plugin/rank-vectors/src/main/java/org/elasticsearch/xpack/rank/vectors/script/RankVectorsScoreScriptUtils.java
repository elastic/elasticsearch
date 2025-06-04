/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.script;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.field.vectors.DenseVector;
import org.elasticsearch.script.field.vectors.RankVectorsDocValuesField;

import java.io.IOException;
import java.util.HexFormat;
import java.util.List;

public class RankVectorsScoreScriptUtils {

    public static class RankVectorsFunction {
        protected final ScoreScript scoreScript;
        protected final RankVectorsDocValuesField field;

        public RankVectorsFunction(ScoreScript scoreScript, RankVectorsDocValuesField field) {
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
                throw new IllegalArgumentException("A document doesn't have a value for a multi-vector field!");
            }
        }
    }

    public static class ByteRankVectorsFunction extends RankVectorsFunction {
        protected final byte[][] queryVector;

        /**
         * Constructs a dense vector function used for byte-sized vectors.
         *
         * @param scoreScript The script in which this function was referenced.
         * @param field The vector field.
         * @param queryVector The query vector.
         */
        public ByteRankVectorsFunction(ScoreScript scoreScript, RankVectorsDocValuesField field, List<List<Number>> queryVector) {
            super(scoreScript, field);
            if (queryVector.isEmpty()) {
                throw new IllegalArgumentException("The query vector is empty.");
            }
            field.getElementType().checkDimensions(field.get().getDims(), queryVector.get(0).size());
            this.queryVector = new byte[queryVector.size()][queryVector.get(0).size()];
            float[] validateValues = new float[queryVector.size()];
            int lastSize = -1;
            for (int i = 0; i < queryVector.size(); i++) {
                if (lastSize != -1 && lastSize != queryVector.get(i).size()) {
                    throw new IllegalArgumentException(
                        "The query vector contains inner vectors which have inconsistent number of dimensions."
                    );
                }
                lastSize = queryVector.get(i).size();
                for (int j = 0; j < queryVector.get(i).size(); j++) {
                    final Number number = queryVector.get(i).get(j);
                    byte value = number.byteValue();
                    this.queryVector[i][j] = value;
                    validateValues[i] = number.floatValue();
                }
                field.getElementType().checkVectorBounds(validateValues);
            }
        }

        /**
         * Constructs a dense vector function used for byte-sized vectors.
         *
         * @param scoreScript The script in which this function was referenced.
         * @param field The vector field.
         * @param queryVector The query vector.
         */
        public ByteRankVectorsFunction(ScoreScript scoreScript, RankVectorsDocValuesField field, byte[][] queryVector) {
            super(scoreScript, field);
            this.queryVector = queryVector;
        }
    }

    public static class FloatRankVectorsFunction extends RankVectorsFunction {
        protected final float[][] queryVector;

        /**
         * Constructs a dense vector function used for float vectors.
         *
         * @param scoreScript The script in which this function was referenced.
         * @param field The vector field.
         * @param queryVector The query vector.
         */
        public FloatRankVectorsFunction(ScoreScript scoreScript, RankVectorsDocValuesField field, List<List<Number>> queryVector) {
            super(scoreScript, field);
            if (queryVector.isEmpty()) {
                throw new IllegalArgumentException("The query vector is empty.");
            }
            DenseVector.checkDimensions(field.get().getDims(), queryVector.get(0).size());

            this.queryVector = new float[queryVector.size()][queryVector.get(0).size()];
            int lastSize = -1;
            for (int i = 0; i < queryVector.size(); i++) {
                if (lastSize != -1 && lastSize != queryVector.get(i).size()) {
                    throw new IllegalArgumentException(
                        "The query vector contains inner vectors which have inconsistent number of dimensions."
                    );
                }
                lastSize = queryVector.get(i).size();
                for (int j = 0; j < queryVector.get(i).size(); j++) {
                    this.queryVector[i][j] = queryVector.get(i).get(j).floatValue();
                }
                field.getElementType().checkVectorBounds(this.queryVector[i]);
            }
        }
    }

    // Calculate Hamming distances between a query's dense vector and documents' dense vectors
    public interface MaxSimInvHammingDistanceInterface {
        float maxSimInvHamming();
    }

    public static class ByteMaxSimInvHammingDistance extends ByteRankVectorsFunction implements MaxSimInvHammingDistanceInterface {

        public ByteMaxSimInvHammingDistance(ScoreScript scoreScript, RankVectorsDocValuesField field, List<List<Number>> queryVector) {
            super(scoreScript, field, queryVector);
        }

        public ByteMaxSimInvHammingDistance(ScoreScript scoreScript, RankVectorsDocValuesField field, byte[][] queryVector) {
            super(scoreScript, field, queryVector);
        }

        public float maxSimInvHamming() {
            setNextVector();
            return field.get().maxSimInvHamming(queryVector);
        }
    }

    private record BytesOrList(byte[][] bytes, List<List<Number>> list) {}

    @SuppressWarnings("unchecked")
    private static BytesOrList parseBytes(Object queryVector) {
        if (queryVector instanceof List) {
            // check if its a list of strings or list of lists
            if (((List<?>) queryVector).get(0) instanceof List) {
                return new BytesOrList(null, ((List<List<Number>>) queryVector));
            } else if (((List<?>) queryVector).get(0) instanceof String) {
                byte[][] parsedQueryVector = new byte[((List<?>) queryVector).size()][];
                int lastSize = -1;
                for (int i = 0; i < ((List<?>) queryVector).size(); i++) {
                    parsedQueryVector[i] = HexFormat.of().parseHex((String) ((List<?>) queryVector).get(i));
                    if (lastSize != -1 && lastSize != parsedQueryVector[i].length) {
                        throw new IllegalArgumentException(
                            "The query vector contains inner vectors which have inconsistent number of dimensions."
                        );
                    }
                    lastSize = parsedQueryVector[i].length;
                }
                return new BytesOrList(parsedQueryVector, null);
            } else {
                throw new IllegalArgumentException("Unsupported input object for byte vectors: " + queryVector.getClass().getName());
            }
        } else {
            throw new IllegalArgumentException("Unsupported input object for byte vectors: " + queryVector.getClass().getName());
        }
    }

    public static final class MaxSimInvHamming {

        private final MaxSimInvHammingDistanceInterface function;

        public MaxSimInvHamming(ScoreScript scoreScript, Object queryVector, String fieldName) {
            RankVectorsDocValuesField field = (RankVectorsDocValuesField) scoreScript.field(fieldName);
            if (field.getElementType() == DenseVectorFieldMapper.ElementType.FLOAT) {
                throw new IllegalArgumentException("hamming distance is only supported for byte or bit vectors");
            }
            BytesOrList bytesOrList = parseBytes(queryVector);
            if (bytesOrList.bytes != null) {
                this.function = new ByteMaxSimInvHammingDistance(scoreScript, field, bytesOrList.bytes);
            } else {
                this.function = new ByteMaxSimInvHammingDistance(scoreScript, field, bytesOrList.list);
            }
        }

        public double maxSimInvHamming() {
            return function.maxSimInvHamming();
        }
    }

    // Calculate a dot product between a query's dense vector and documents' dense vectors
    public interface MaxSimDotProductInterface {
        double maxSimDotProduct();
    }

    public static class MaxSimBitDotProduct extends RankVectorsFunction implements MaxSimDotProductInterface {
        private final byte[][] byteQueryVector;
        private final float[][] floatQueryVector;

        public MaxSimBitDotProduct(ScoreScript scoreScript, RankVectorsDocValuesField field, byte[][] queryVector) {
            super(scoreScript, field);
            if (field.getElementType() != DenseVectorFieldMapper.ElementType.BIT) {
                throw new IllegalArgumentException("Cannot calculate bit dot product for non-bit vectors");
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

        public MaxSimBitDotProduct(ScoreScript scoreScript, RankVectorsDocValuesField field, List<List<Number>> queryVector) {
            super(scoreScript, field);
            if (queryVector.isEmpty()) {
                throw new IllegalArgumentException("The query vector is empty.");
            }
            if (field.getElementType() != DenseVectorFieldMapper.ElementType.BIT) {
                throw new IllegalArgumentException("cannot calculate bit dot product for non-bit vectors");
            }
            float[][] floatQueryVector = new float[queryVector.size()][];
            byte[][] byteQueryVector = new byte[queryVector.size()][];
            boolean isFloat = false;
            int lastSize = -1;
            for (int i = 0; i < queryVector.size(); i++) {
                if (lastSize != -1 && lastSize != queryVector.get(i).size()) {
                    throw new IllegalArgumentException(
                        "The query vector contains inner vectors which have inconsistent number of dimensions."
                    );
                }
                lastSize = queryVector.get(i).size();
                floatQueryVector[i] = new float[queryVector.get(i).size()];
                if (isFloat == false) {
                    byteQueryVector[i] = new byte[queryVector.get(i).size()];
                }
                for (int j = 0; j < queryVector.get(i).size(); j++) {
                    Number number = queryVector.get(i).get(j);
                    floatQueryVector[i][j] = number.floatValue();
                    if (isFloat == false) {
                        byteQueryVector[i][j] = number.byteValue();
                    }
                    if (isFloat
                        || floatQueryVector[i][j] % 1.0f != 0.0f
                        || floatQueryVector[i][j] < Byte.MIN_VALUE
                        || floatQueryVector[i][j] > Byte.MAX_VALUE) {
                        isFloat = true;
                    }
                }
            }
            int fieldDims = field.get().getDims();
            if (isFloat) {
                this.floatQueryVector = floatQueryVector;
                this.byteQueryVector = null;
                if (fieldDims != floatQueryVector[0].length) {
                    throw new IllegalArgumentException(
                        "The query vector contains inner vectors which have incorrect number of dimensions. Must be ["
                            + fieldDims
                            + "] for float wise operations: provided ["
                            + floatQueryVector[0].length
                            + "]."
                    );
                }
            } else {
                this.floatQueryVector = null;
                this.byteQueryVector = byteQueryVector;
                if (fieldDims != byteQueryVector[0].length * Byte.SIZE && fieldDims != byteQueryVector[0].length) {
                    throw new IllegalArgumentException(
                        "The query vector contains inner vectors which have incorrect number of dimensions. Must be ["
                            + fieldDims / 8
                            + "] for bitwise operations, or ["
                            + fieldDims
                            + "] for byte wise operations: provided ["
                            + byteQueryVector[0].length
                            + "]."
                    );
                }
            }
        }

        @Override
        public double maxSimDotProduct() {
            setNextVector();
            return byteQueryVector != null ? field.get().maxSimDotProduct(byteQueryVector) : field.get().maxSimDotProduct(floatQueryVector);
        }
    }

    public static class MaxSimByteDotProduct extends ByteRankVectorsFunction implements MaxSimDotProductInterface {

        public MaxSimByteDotProduct(ScoreScript scoreScript, RankVectorsDocValuesField field, List<List<Number>> queryVector) {
            super(scoreScript, field, queryVector);
        }

        public MaxSimByteDotProduct(ScoreScript scoreScript, RankVectorsDocValuesField field, byte[][] queryVector) {
            super(scoreScript, field, queryVector);
        }

        public double maxSimDotProduct() {
            setNextVector();
            return field.get().maxSimDotProduct(queryVector);
        }
    }

    public static class MaxSimFloatDotProduct extends FloatRankVectorsFunction implements MaxSimDotProductInterface {

        public MaxSimFloatDotProduct(ScoreScript scoreScript, RankVectorsDocValuesField field, List<List<Number>> queryVector) {
            super(scoreScript, field, queryVector);
        }

        public double maxSimDotProduct() {
            setNextVector();
            return field.get().maxSimDotProduct(queryVector);
        }
    }

    public static final class MaxSimDotProduct {

        private final MaxSimDotProductInterface function;

        @SuppressWarnings("unchecked")
        public MaxSimDotProduct(ScoreScript scoreScript, Object queryVector, String fieldName) {
            RankVectorsDocValuesField field = (RankVectorsDocValuesField) scoreScript.field(fieldName);
            function = switch (field.getElementType()) {
                case BIT -> {
                    BytesOrList bytesOrList = parseBytes(queryVector);
                    if (bytesOrList.bytes != null) {
                        yield new MaxSimBitDotProduct(scoreScript, field, bytesOrList.bytes);
                    } else {
                        yield new MaxSimBitDotProduct(scoreScript, field, bytesOrList.list);
                    }
                }
                case BYTE -> {
                    BytesOrList bytesOrList = parseBytes(queryVector);
                    if (bytesOrList.bytes != null) {
                        yield new MaxSimByteDotProduct(scoreScript, field, bytesOrList.bytes);
                    } else {
                        yield new MaxSimByteDotProduct(scoreScript, field, bytesOrList.list);
                    }
                }
                case FLOAT -> {
                    if (queryVector instanceof List) {
                        yield new MaxSimFloatDotProduct(scoreScript, field, (List<List<Number>>) queryVector);
                    }
                    throw new IllegalArgumentException("Unsupported input object for float vectors: " + queryVector.getClass().getName());
                }
            };
        }

        public double maxSimDotProduct() {
            return function.maxSimDotProduct();
        }
    }
}
