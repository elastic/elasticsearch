/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;

import java.util.List;

/**
 * Types used by the code generator.
 */
public class Types {
    private static final String PACKAGE = "org.elasticsearch.compute";
    private static final String AGGREGATION_PACKAGE = PACKAGE + ".aggregation";
    private static final String OPERATOR_PACKAGE = PACKAGE + ".operator";
    private static final String DATA_PACKAGE = PACKAGE + ".data";

    static final TypeName LIST_INTEGER = ParameterizedTypeName.get(ClassName.get(List.class), TypeName.INT.box());

    static final ClassName PAGE = ClassName.get(DATA_PACKAGE, "Page");
    static final ClassName BLOCK = ClassName.get(DATA_PACKAGE, "Block");
    static final TypeName BLOCK_ARRAY = ArrayTypeName.of(BLOCK);
    static final ClassName VECTOR = ClassName.get(DATA_PACKAGE, "Vector");

    static final ClassName BIG_ARRAYS = ClassName.get("org.elasticsearch.common.util", "BigArrays");
    static final ClassName BYTES_REF_ARRAY = ClassName.get("org.elasticsearch.common.util", "BytesRefArray");

    static final ClassName BOOLEAN_BLOCK = ClassName.get(DATA_PACKAGE, "BooleanBlock");
    static final ClassName BYTES_REF_BLOCK = ClassName.get(DATA_PACKAGE, "BytesRefBlock");
    static final ClassName INT_BLOCK = ClassName.get(DATA_PACKAGE, "IntBlock");
    static final ClassName LONG_BLOCK = ClassName.get(DATA_PACKAGE, "LongBlock");
    static final ClassName DOUBLE_BLOCK = ClassName.get(DATA_PACKAGE, "DoubleBlock");

    static final ClassName BOOLEAN_BLOCK_BUILDER = BOOLEAN_BLOCK.nestedClass("Builder");
    static final ClassName BYTES_REF_BLOCK_BUILDER = BYTES_REF_BLOCK.nestedClass("Builder");
    static final ClassName INT_BLOCK_BUILDER = INT_BLOCK.nestedClass("Builder");
    static final ClassName LONG_BLOCK_BUILDER = LONG_BLOCK.nestedClass("Builder");
    static final ClassName DOUBLE_BLOCK_BUILDER = DOUBLE_BLOCK.nestedClass("Builder");

    static final ClassName ELEMENT_TYPE = ClassName.get(DATA_PACKAGE, "ElementType");

    static final ClassName BOOLEAN_VECTOR = ClassName.get(DATA_PACKAGE, "BooleanVector");
    static final ClassName BYTES_REF_VECTOR = ClassName.get(DATA_PACKAGE, "BytesRefVector");
    static final ClassName INT_VECTOR = ClassName.get(DATA_PACKAGE, "IntVector");
    static final ClassName LONG_VECTOR = ClassName.get(DATA_PACKAGE, "LongVector");
    static final ClassName DOUBLE_VECTOR = ClassName.get(DATA_PACKAGE, "DoubleVector");

    static final ClassName BOOLEAN_ARRAY_VECTOR = ClassName.get(DATA_PACKAGE, "BooleanArrayVector");
    static final ClassName BYTES_REF_ARRAY_VECTOR = ClassName.get(DATA_PACKAGE, "BytesRefArrayVector");
    static final ClassName INT_ARRAY_VECTOR = ClassName.get(DATA_PACKAGE, "IntArrayVector");
    static final ClassName LONG_ARRAY_VECTOR = ClassName.get(DATA_PACKAGE, "LongArrayVector");
    static final ClassName DOUBLE_ARRAY_VECTOR = ClassName.get(DATA_PACKAGE, "DoubleArrayVector");

    static final ClassName BOOLEAN_ARRAY_BLOCK = ClassName.get(DATA_PACKAGE, "BooleanArrayBlock");
    static final ClassName BYTES_REF_ARRAY_BLOCK = ClassName.get(DATA_PACKAGE, "BytesRefArrayBlock");
    static final ClassName INT_ARRAY_BLOCK = ClassName.get(DATA_PACKAGE, "IntArrayBlock");
    static final ClassName LONG_ARRAY_BLOCK = ClassName.get(DATA_PACKAGE, "LongArrayBlock");
    static final ClassName DOUBLE_ARRAY_BLOCK = ClassName.get(DATA_PACKAGE, "DoubleArrayBlock");

    static final ClassName BOOLEAN_CONSTANT_VECTOR = ClassName.get(DATA_PACKAGE, "ConstantBooleanVector");
    static final ClassName BYTES_REF_CONSTANT_VECTOR = ClassName.get(DATA_PACKAGE, "ConstantBytesRefVector");
    static final ClassName INT_CONSTANT_VECTOR = ClassName.get(DATA_PACKAGE, "ConstantIntVector");
    static final ClassName LONG_CONSTANT_VECTOR = ClassName.get(DATA_PACKAGE, "ConstantLongVector");
    static final ClassName DOUBLE_CONSTANT_VECTOR = ClassName.get(DATA_PACKAGE, "ConstantDoubleVector");

    static final ClassName AGGREGATOR_FUNCTION = ClassName.get(AGGREGATION_PACKAGE, "AggregatorFunction");
    static final ClassName AGGREGATOR_FUNCTION_SUPPLIER = ClassName.get(AGGREGATION_PACKAGE, "AggregatorFunctionSupplier");
    static final ClassName GROUPING_AGGREGATOR_FUNCTION = ClassName.get(AGGREGATION_PACKAGE, "GroupingAggregatorFunction");
    static final ClassName GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT = ClassName.get(
        AGGREGATION_PACKAGE,
        "GroupingAggregatorFunction",
        "AddInput"
    );

    static final ClassName INTERMEDIATE_STATE_DESC = ClassName.get(AGGREGATION_PACKAGE, "IntermediateStateDesc");
    static final TypeName LIST_AGG_FUNC_DESC = ParameterizedTypeName.get(ClassName.get(List.class), INTERMEDIATE_STATE_DESC);

    static final ClassName EXPRESSION_EVALUATOR = ClassName.get(OPERATOR_PACKAGE, "EvalOperator", "ExpressionEvaluator");
    static final ClassName ABSTRACT_MULTIVALUE_FUNCTION_EVALUATOR = ClassName.get(
        "org.elasticsearch.xpack.esql.expression.function.scalar.multivalue",
        "AbstractMultivalueFunction",
        "AbstractEvaluator"
    );
    static final ClassName ABSTRACT_CONVERT_FUNCTION_EVALUATOR = ClassName.get(
        "org.elasticsearch.xpack.esql.expression.function.scalar.convert",
        "AbstractConvertFunction",
        "AbstractEvaluator"
    );

    static final ClassName WARNINGS = ClassName.get("org.elasticsearch.xpack.esql.expression.function", "Warnings");

    static final ClassName SOURCE = ClassName.get("org.elasticsearch.xpack.ql.tree", "Source");

    static final ClassName BYTES_REF = ClassName.get("org.apache.lucene.util", "BytesRef");

    static ClassName blockType(TypeName elementType) {
        if (elementType.equals(TypeName.BOOLEAN)) {
            return BOOLEAN_BLOCK;
        }
        if (elementType.equals(BYTES_REF)) {
            return BYTES_REF_BLOCK;
        }
        if (elementType.equals(TypeName.INT)) {
            return INT_BLOCK;
        }
        if (elementType.equals(TypeName.LONG)) {
            return LONG_BLOCK;
        }
        if (elementType.equals(TypeName.DOUBLE)) {
            return DOUBLE_BLOCK;
        }
        throw new IllegalArgumentException("unknown block type for [" + elementType + "]");
    }

    static ClassName blockType(String elementType) {
        if (elementType.equalsIgnoreCase(TypeName.BOOLEAN.toString())) {
            return BOOLEAN_BLOCK;
        }
        if (elementType.equalsIgnoreCase("BYTES_REF")) {
            return BYTES_REF_BLOCK;
        }
        if (elementType.equalsIgnoreCase(TypeName.INT.toString())) {
            return INT_BLOCK;
        }
        if (elementType.equalsIgnoreCase(TypeName.LONG.toString())) {
            return LONG_BLOCK;
        }
        if (elementType.equalsIgnoreCase(TypeName.DOUBLE.toString())) {
            return DOUBLE_BLOCK;
        }
        throw new IllegalArgumentException("unknown vector type for [" + elementType + "]");
    }

    static ClassName vectorType(TypeName elementType) {
        if (elementType.equals(TypeName.BOOLEAN)) {
            return BOOLEAN_VECTOR;
        }
        if (elementType.equals(BYTES_REF)) {
            return BYTES_REF_VECTOR;
        }
        if (elementType.equals(TypeName.INT)) {
            return INT_VECTOR;
        }
        if (elementType.equals(TypeName.LONG)) {
            return LONG_VECTOR;
        }
        if (elementType.equals(TypeName.DOUBLE)) {
            return DOUBLE_VECTOR;
        }
        throw new IllegalArgumentException("unknown vector type for [" + elementType + "]");
    }

    static ClassName vectorType(String elementType) {
        if (elementType.equalsIgnoreCase(TypeName.BOOLEAN.toString())) {
            return BOOLEAN_VECTOR;
        }
        if (elementType.equalsIgnoreCase("BYTES_REF")) {
            return BYTES_REF_VECTOR;
        }
        if (elementType.equalsIgnoreCase(TypeName.INT.toString())) {
            return INT_VECTOR;
        }
        if (elementType.equalsIgnoreCase(TypeName.LONG.toString())) {
            return LONG_VECTOR;
        }
        if (elementType.equalsIgnoreCase(TypeName.DOUBLE.toString())) {
            return DOUBLE_VECTOR;
        }
        throw new IllegalArgumentException("unknown vector type for [" + elementType + "]");
    }

    static ClassName arrayVectorType(TypeName elementType) {
        if (elementType.equals(TypeName.BOOLEAN)) {
            return BOOLEAN_ARRAY_VECTOR;
        }
        if (elementType.equals(BYTES_REF)) {
            return BYTES_REF_ARRAY_VECTOR;
        }
        if (elementType.equals(TypeName.INT)) {
            return INT_ARRAY_VECTOR;
        }
        if (elementType.equals(TypeName.LONG)) {
            return LONG_ARRAY_VECTOR;
        }
        if (elementType.equals(TypeName.DOUBLE)) {
            return DOUBLE_ARRAY_VECTOR;
        }
        throw new IllegalArgumentException("unknown vector type for [" + elementType + "]");
    }

    static ClassName arrayBlockType(TypeName elementType) {
        if (elementType.equals(TypeName.BOOLEAN)) {
            return BOOLEAN_ARRAY_BLOCK;
        }
        if (elementType.equals(BYTES_REF)) {
            return BYTES_REF_ARRAY_BLOCK;
        }
        if (elementType.equals(TypeName.INT)) {
            return INT_ARRAY_BLOCK;
        }
        if (elementType.equals(TypeName.LONG)) {
            return LONG_ARRAY_BLOCK;
        }
        if (elementType.equals(TypeName.DOUBLE)) {
            return DOUBLE_ARRAY_BLOCK;
        }
        throw new IllegalArgumentException("unknown vector type for [" + elementType + "]");
    }

    static ClassName constantVectorType(TypeName elementType) {
        if (elementType.equals(TypeName.BOOLEAN)) {
            return BOOLEAN_CONSTANT_VECTOR;
        }
        if (elementType.equals(BYTES_REF)) {
            return BYTES_REF_CONSTANT_VECTOR;
        }
        if (elementType.equals(TypeName.INT)) {
            return INT_CONSTANT_VECTOR;
        }
        if (elementType.equals(TypeName.LONG)) {
            return LONG_CONSTANT_VECTOR;
        }
        if (elementType.equals(TypeName.DOUBLE)) {
            return DOUBLE_CONSTANT_VECTOR;
        }
        throw new IllegalArgumentException("unknown vector type for [" + elementType + "]");
    }

    static TypeName elementType(TypeName t) {
        if (t.equals(BOOLEAN_BLOCK) || t.equals(BOOLEAN_VECTOR) || t.equals(BOOLEAN_BLOCK_BUILDER)) {
            return TypeName.BOOLEAN;
        }
        if (t.equals(BYTES_REF_BLOCK) || t.equals(BYTES_REF_VECTOR) || t.equals(BYTES_REF_BLOCK_BUILDER)) {
            return BYTES_REF;
        }
        if (t.equals(INT_BLOCK) || t.equals(INT_VECTOR) || t.equals(INT_BLOCK_BUILDER)) {
            return TypeName.INT;
        }
        if (t.equals(LONG_BLOCK) || t.equals(LONG_VECTOR) || t.equals(LONG_BLOCK_BUILDER)) {
            return TypeName.LONG;
        }
        if (t.equals(DOUBLE_BLOCK) || t.equals(DOUBLE_VECTOR) || t.equals(DOUBLE_BLOCK_BUILDER)) {
            return TypeName.DOUBLE;
        }
        throw new IllegalArgumentException("unknown element type for [" + t + "]");
    }

}
