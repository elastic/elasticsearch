/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.CompoundOutputFunction;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.Collections;
import java.util.Map;

/**
 * An evaluator that extracts compound output based on a {@link CompoundOutputFunction}.
 */
public class CompoundOutputEvaluator implements ColumnExtractOperator.Evaluator {

    /**
     * A map of output fields to use from the evaluating {@link CompoundOutputFunction}.
     * The actual output of the evaluating function may not fully match the required fields for the expected output as it is reflected
     * in {@link #computeRow}. This may happen if the actual execution occurs on a data node that has a different version from the
     * coordinating node (e.g. during cluster upgrade).
     */
    private final Map<String, DataType> functionOutputFields;

    private final CompoundOutputFunction function;
    private final DataType inputType;
    private final Warnings warnings;

    public CompoundOutputEvaluator(
        Map<String, DataType> functionOutputFields,
        CompoundOutputFunction function,
        DataType inputType,
        Warnings warnings
    ) {
        this.functionOutputFields = functionOutputFields;
        this.function = function;
        this.inputType = inputType;
        this.warnings = warnings;
    }

    /**
     * Executes the evaluation of the {@link CompoundOutputFunction} on the provided input.
     * The {@code target} output array must have the same size as {@link #functionOutputFields} and its elements must match the
     * {@link #functionOutputFields} entries in type and order. Otherwise, this method will throw an exception.
     * If an expected output field is missing from the actual output of the function, a null value will be appended to the corresponding
     * target block. If the actual output of the function contains an entry that is not expected, it will be ignored.
     * @param input the input to evaluate the function on
     * @param row row index in the input
     * @param target the output column blocks
     * @param spare the {@link BytesRef} to use for value retrieval
     * @throws EsqlIllegalArgumentException if the {@code target} array does not have the correct size or its elements do not match the
     *                                      expected output fields
     */
    @Override
    public void computeRow(BytesRefBlock input, int row, Block.Builder[] target, BytesRef spare) {
        if (target.length != functionOutputFields.size()) {
            throw new EsqlIllegalArgumentException("Incorrect number of target blocks for function [" + function + "]");
        }

        // if the input is null or invalid, we return nulls for all output fields

        Map<String, Object> result = Collections.emptyMap();
        if (input.isNull(row) == false) {
            try {
                BytesRef bytes = input.getBytesRef(input.getFirstValueIndex(row), spare);
                String inputAsString = getInputAsString(bytes, inputType);
                result = function.evaluate(inputAsString);
            } catch (Exception e) {
                warnings.registerException(e);
            }
        }

        int i = 0;
        for (Map.Entry<String, DataType> entry : functionOutputFields.entrySet()) {
            String relativeKey = entry.getKey();
            DataType dataType = entry.getValue();
            Object value = result.get(relativeKey);
            Block.Builder blockBuilder = target[i];

            if (value == null) {
                blockBuilder.appendNull();
            } else {
                switch (dataType) {
                    case KEYWORD:
                    case TEXT:
                        if (blockBuilder instanceof BytesRefBlock.Builder brbb) {
                            brbb.appendBytesRef(new BytesRef(value.toString()));
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case IP:
                        if (blockBuilder instanceof BytesRefBlock.Builder brbb) {
                            if (value instanceof BytesRef) {
                                brbb.appendBytesRef((BytesRef) value);
                            } else {
                                brbb.appendBytesRef(EsqlDataTypeConverter.stringToIP(value.toString()));
                            }
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case DOUBLE:
                        if (blockBuilder instanceof DoubleBlock.Builder dbb) {
                            dbb.appendDouble(((Number) value).doubleValue());
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case LONG:
                        if (blockBuilder instanceof LongBlock.Builder lbb) {
                            lbb.appendLong(((Number) value).longValue());
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case INTEGER:
                        if (blockBuilder instanceof IntBlock.Builder ibb) {
                            ibb.appendInt(((Number) value).intValue());
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case BOOLEAN:
                        if (blockBuilder instanceof BooleanBlock.Builder bbb) {
                            bbb.appendBoolean((Boolean) value);
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    case GEO_POINT:
                        if (blockBuilder instanceof BytesRefBlock.Builder brbb) {
                            if (value instanceof GeoPoint gp) {
                                brbb.appendBytesRef(EsqlDataTypeConverter.stringToGeo(gp.toWKT()));
                            } else {
                                throw new EsqlIllegalArgumentException(
                                    "Unsupported value type ["
                                        + value.getClass().getName()
                                        + "] for an output field of type ["
                                        + dataType
                                        + "]"
                                );
                            }
                        } else {
                            throw new EsqlIllegalArgumentException("Incorrect block builder for type [" + dataType + "]");
                        }
                        break;
                    default:
                        throw new EsqlIllegalArgumentException(
                            "Unsupported DataType [" + dataType + "] for GeoIP output field [" + relativeKey + "]"
                        );
                }
            }
            i++;
        }
    }

    private static String getInputAsString(BytesRef input, DataType inputType) {
        if (inputType == DataType.IP) {
            return EsqlDataTypeConverter.ipToString(input);
        } else if (DataType.isString(inputType)) {
            return input.utf8ToString();
        } else {
            throw new IllegalArgumentException("Unsupported input type [" + inputType + "]");
        }
    }
}
