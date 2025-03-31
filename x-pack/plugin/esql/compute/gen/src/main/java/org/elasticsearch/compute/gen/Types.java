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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.lang.model.type.TypeMirror;

import static java.util.stream.Collectors.toUnmodifiableMap;

/**
 * Types used by the code generator.
 */
public class Types {
    private static final String PACKAGE = "org.elasticsearch.compute";
    private static final String AGGREGATION_PACKAGE = PACKAGE + ".aggregation";
    private static final String OPERATOR_PACKAGE = PACKAGE + ".operator";
    private static final String DATA_PACKAGE = PACKAGE + ".data";

    static final TypeName STRING = ClassName.get("java.lang", "String");

    static final TypeName LIST_INTEGER = ParameterizedTypeName.get(ClassName.get(List.class), TypeName.INT.box());

    static final ClassName PAGE = ClassName.get(DATA_PACKAGE, "Page");
    static final ClassName BLOCK = ClassName.get(DATA_PACKAGE, "Block");
    static final TypeName BLOCK_ARRAY = ArrayTypeName.of(BLOCK);
    static final ClassName VECTOR = ClassName.get(DATA_PACKAGE, "Vector");

    static final ClassName CIRCUIT_BREAKER = ClassName.get("org.elasticsearch.common.breaker", "CircuitBreaker");
    static final ClassName BIG_ARRAYS = ClassName.get("org.elasticsearch.common.util", "BigArrays");

    static final ClassName BOOLEAN_BLOCK = ClassName.get(DATA_PACKAGE, "BooleanBlock");
    static final ClassName BYTES_REF_BLOCK = ClassName.get(DATA_PACKAGE, "BytesRefBlock");
    static final ClassName INT_BLOCK = ClassName.get(DATA_PACKAGE, "IntBlock");
    static final ClassName LONG_BLOCK = ClassName.get(DATA_PACKAGE, "LongBlock");
    static final ClassName DOUBLE_BLOCK = ClassName.get(DATA_PACKAGE, "DoubleBlock");
    static final ClassName FLOAT_BLOCK = ClassName.get(DATA_PACKAGE, "FloatBlock");

    static final ClassName BOOLEAN_BLOCK_BUILDER = BOOLEAN_BLOCK.nestedClass("Builder");
    static final ClassName BYTES_REF_BLOCK_BUILDER = BYTES_REF_BLOCK.nestedClass("Builder");
    static final ClassName INT_BLOCK_BUILDER = INT_BLOCK.nestedClass("Builder");
    static final ClassName LONG_BLOCK_BUILDER = LONG_BLOCK.nestedClass("Builder");
    static final ClassName DOUBLE_BLOCK_BUILDER = DOUBLE_BLOCK.nestedClass("Builder");
    static final ClassName FLOAT_BLOCK_BUILDER = FLOAT_BLOCK.nestedClass("Builder");

    static final ClassName ELEMENT_TYPE = ClassName.get(DATA_PACKAGE, "ElementType");

    static final ClassName BOOLEAN_VECTOR = ClassName.get(DATA_PACKAGE, "BooleanVector");
    static final ClassName BYTES_REF_VECTOR = ClassName.get(DATA_PACKAGE, "BytesRefVector");
    static final ClassName ORDINALS_BYTES_REF_VECTOR = ClassName.get(DATA_PACKAGE, "OrdinalBytesRefVector");
    static final ClassName INT_VECTOR = ClassName.get(DATA_PACKAGE, "IntVector");
    static final ClassName LONG_VECTOR = ClassName.get(DATA_PACKAGE, "LongVector");
    static final ClassName DOUBLE_VECTOR = ClassName.get(DATA_PACKAGE, "DoubleVector");
    static final ClassName FLOAT_VECTOR = ClassName.get(DATA_PACKAGE, "FloatVector");

    static final ClassName BOOLEAN_VECTOR_BUILDER = ClassName.get(DATA_PACKAGE, "BooleanVector", "Builder");
    static final ClassName BYTES_REF_VECTOR_BUILDER = ClassName.get(DATA_PACKAGE, "BytesRefVector", "Builder");
    static final ClassName INT_VECTOR_BUILDER = ClassName.get(DATA_PACKAGE, "IntVector", "Builder");
    static final ClassName LONG_VECTOR_BUILDER = ClassName.get(DATA_PACKAGE, "LongVector", "Builder");
    static final ClassName DOUBLE_VECTOR_BUILDER = ClassName.get(DATA_PACKAGE, "DoubleVector", "Builder");
    static final ClassName FLOAT_VECTOR_BUILDER = ClassName.get(DATA_PACKAGE, "FloatVector", "Builder");

    static final ClassName BOOLEAN_VECTOR_FIXED_BUILDER = ClassName.get(DATA_PACKAGE, "BooleanVector", "FixedBuilder");
    static final ClassName INT_VECTOR_FIXED_BUILDER = ClassName.get(DATA_PACKAGE, "IntVector", "FixedBuilder");
    static final ClassName LONG_VECTOR_FIXED_BUILDER = ClassName.get(DATA_PACKAGE, "LongVector", "FixedBuilder");
    static final ClassName DOUBLE_VECTOR_FIXED_BUILDER = ClassName.get(DATA_PACKAGE, "DoubleVector", "FixedBuilder");
    static final ClassName FLOAT_VECTOR_FIXED_BUILDER = ClassName.get(DATA_PACKAGE, "FloatVector", "FixedBuilder");

    static final ClassName AGGREGATOR_STATE = ClassName.get(AGGREGATION_PACKAGE, "AggregatorState");
    static final ClassName GROUPING_AGGREGATOR_STATE = ClassName.get(AGGREGATION_PACKAGE, "GroupingAggregatorState");

    static final ClassName AGGREGATOR_FUNCTION = ClassName.get(AGGREGATION_PACKAGE, "AggregatorFunction");
    static final ClassName AGGREGATOR_FUNCTION_SUPPLIER = ClassName.get(AGGREGATION_PACKAGE, "AggregatorFunctionSupplier");
    static final ClassName GROUPING_AGGREGATOR_FUNCTION = ClassName.get(AGGREGATION_PACKAGE, "GroupingAggregatorFunction");
    static final ClassName GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT = ClassName.get(
        AGGREGATION_PACKAGE,
        "GroupingAggregatorFunction",
        "AddInput"
    );
    static final ClassName SEEN_GROUP_IDS = ClassName.get(AGGREGATION_PACKAGE, "SeenGroupIds");

    static final ClassName INTERMEDIATE_STATE_DESC = ClassName.get(AGGREGATION_PACKAGE, "IntermediateStateDesc");
    static final TypeName LIST_AGG_FUNC_DESC = ParameterizedTypeName.get(ClassName.get(List.class), INTERMEDIATE_STATE_DESC);

    static final ClassName DRIVER_CONTEXT = ClassName.get(OPERATOR_PACKAGE, "DriverContext");

    static final ClassName EXPRESSION_EVALUATOR = ClassName.get(OPERATOR_PACKAGE, "EvalOperator", "ExpressionEvaluator");
    static final ClassName EXPRESSION_EVALUATOR_FACTORY = ClassName.get(OPERATOR_PACKAGE, "EvalOperator", "ExpressionEvaluator", "Factory");
    static final ClassName ABSTRACT_MULTIVALUE_FUNCTION_EVALUATOR = ClassName.get(
        "org.elasticsearch.xpack.esql.expression.function.scalar.multivalue",
        "AbstractMultivalueFunction",
        "AbstractEvaluator"
    );
    static final ClassName ABSTRACT_NULLABLE_MULTIVALUE_FUNCTION_EVALUATOR = ClassName.get(
        "org.elasticsearch.xpack.esql.expression.function.scalar.multivalue",
        "AbstractMultivalueFunction",
        "AbstractNullableEvaluator"
    );
    static final ClassName ABSTRACT_CONVERT_FUNCTION_EVALUATOR = ClassName.get(
        "org.elasticsearch.xpack.esql.expression.function.scalar.convert",
        "AbstractConvertFunction",
        "AbstractEvaluator"
    );

    static final ClassName WARNINGS = ClassName.get("org.elasticsearch.compute.operator", "Warnings");

    static final ClassName SOURCE = ClassName.get("org.elasticsearch.xpack.esql.core.tree", "Source");

    static final ClassName BYTES_REF = ClassName.get("org.apache.lucene.util", "BytesRef");

    static final ClassName RELEASABLE = ClassName.get("org.elasticsearch.core", "Releasable");
    static final ClassName RELEASABLES = ClassName.get("org.elasticsearch.core", "Releasables");

    private record TypeDef(TypeName type, String alias, ClassName block, ClassName vector) {

        public static TypeDef of(TypeName type, String alias, String block, String vector) {
            return new TypeDef(type, alias, ClassName.get(DATA_PACKAGE, block), ClassName.get(DATA_PACKAGE, vector));
        }
    }

    private static final Map<String, TypeDef> TYPES = Stream.of(
        TypeDef.of(TypeName.BOOLEAN, "BOOLEAN", "BooleanBlock", "BooleanVector"),
        TypeDef.of(TypeName.INT, "INT", "IntBlock", "IntVector"),
        TypeDef.of(TypeName.LONG, "LONG", "LongBlock", "LongVector"),
        TypeDef.of(TypeName.FLOAT, "FLOAT", "FloatBlock", "FloatVector"),
        TypeDef.of(TypeName.DOUBLE, "DOUBLE", "DoubleBlock", "DoubleVector"),
        TypeDef.of(BYTES_REF, "BYTES_REF", "BytesRefBlock", "BytesRefVector")
    )
        .flatMap(def -> Stream.of(def.type.toString(), def.type + "[]", def.alias).map(alias -> Map.entry(alias, def)))
        .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    private static TypeDef findRequired(String name, String kind) {
        TypeDef typeDef = TYPES.get(name);
        if (typeDef == null) {
            throw new IllegalArgumentException("unknown " + kind + " type [" + name + "]");
        }
        return typeDef;
    }

    static TypeName fromString(String type) {
        return findRequired(type, "plain").type;
    }

    static ClassName blockType(TypeName elementType) {
        return blockType(elementType.toString());
    }

    static ClassName blockType(String elementType) {
        return findRequired(elementType, "block").block;
    }

    static ClassName vectorType(TypeName elementType) {
        return vectorType(elementType.toString());
    }

    static ClassName vectorType(String elementType) {
        return findRequired(elementType, "vector").vector;
    }

    static ClassName builderType(TypeName resultType) {
        if (resultType.equals(BOOLEAN_BLOCK)) {
            return BOOLEAN_BLOCK_BUILDER;
        }
        if (resultType.equals(BOOLEAN_VECTOR)) {
            return BOOLEAN_VECTOR_BUILDER;
        }
        if (resultType.equals(BYTES_REF_BLOCK)) {
            return BYTES_REF_BLOCK_BUILDER;
        }
        if (resultType.equals(BYTES_REF_VECTOR)) {
            return BYTES_REF_VECTOR_BUILDER;
        }
        if (resultType.equals(INT_BLOCK)) {
            return INT_BLOCK_BUILDER;
        }
        if (resultType.equals(INT_VECTOR)) {
            return INT_VECTOR_BUILDER;
        }
        if (resultType.equals(LONG_BLOCK)) {
            return LONG_BLOCK_BUILDER;
        }
        if (resultType.equals(LONG_VECTOR)) {
            return LONG_VECTOR_BUILDER;
        }
        if (resultType.equals(DOUBLE_BLOCK)) {
            return DOUBLE_BLOCK_BUILDER;
        }
        if (resultType.equals(DOUBLE_VECTOR)) {
            return DOUBLE_VECTOR_BUILDER;
        }
        if (resultType.equals(FLOAT_BLOCK)) {
            return FLOAT_BLOCK_BUILDER;
        }
        if (resultType.equals(FLOAT_VECTOR)) {
            return FLOAT_VECTOR_BUILDER;
        }
        throw new IllegalArgumentException("unknown builder type for [" + resultType + "]");
    }

    static ClassName vectorFixedBuilderType(TypeName elementType) {
        if (elementType.equals(TypeName.BOOLEAN)) {
            return BOOLEAN_VECTOR_FIXED_BUILDER;
        }
        if (elementType.equals(TypeName.INT)) {
            return INT_VECTOR_FIXED_BUILDER;
        }
        if (elementType.equals(TypeName.LONG)) {
            return LONG_VECTOR_FIXED_BUILDER;
        }
        if (elementType.equals(TypeName.DOUBLE)) {
            return DOUBLE_VECTOR_FIXED_BUILDER;
        }
        if (elementType.equals(TypeName.FLOAT)) {
            return FLOAT_VECTOR_FIXED_BUILDER;
        }
        throw new IllegalArgumentException("unknown vector fixed builder type for [" + elementType + "]");
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

    static boolean extendsSuper(javax.lang.model.util.Types types, TypeMirror c, String superName) {
        Deque<TypeMirror> mirrors = new ArrayDeque<>();
        mirrors.add(c);
        while (mirrors.isEmpty() == false) {
            TypeMirror m = mirrors.pop();
            if (m.toString().equals(superName)) {
                return true;
            }
            mirrors.addAll(types.directSupertypes(m));
        }
        return false;
    }
}
