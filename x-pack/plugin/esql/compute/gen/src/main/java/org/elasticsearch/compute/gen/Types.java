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
    private static final String EXPRESSION_PACKAGE = PACKAGE + ".expression";
    private static final String OPERATOR_PACKAGE = PACKAGE + ".operator";
    private static final String DATA_PACKAGE = PACKAGE + ".data";

    public static final TypeName STRING = ClassName.get("java.lang", "String");

    public static final TypeName LIST_INTEGER = ParameterizedTypeName.get(ClassName.get(List.class), TypeName.INT.box());

    static final ClassName PAGE = ClassName.get(DATA_PACKAGE, "Page");
    static final ClassName BLOCK = ClassName.get(DATA_PACKAGE, "Block");
    static final TypeName BLOCK_ARRAY = ArrayTypeName.of(BLOCK);
    static final ClassName VECTOR = ClassName.get(DATA_PACKAGE, "Vector");

    static final ClassName CIRCUIT_BREAKER = ClassName.get("org.elasticsearch.common.breaker", "CircuitBreaker");
    static final ClassName BIG_ARRAYS = ClassName.get("org.elasticsearch.common.util", "BigArrays");
    static final ClassName RAM_USAGE_ESIMATOR = ClassName.get("org.apache.lucene.util", "RamUsageEstimator");

    public static final ClassName BOOLEAN_BLOCK = ClassName.get(DATA_PACKAGE, "BooleanBlock");
    public static final ClassName BYTES_REF_BLOCK = ClassName.get(DATA_PACKAGE, "BytesRefBlock");
    public static final ClassName INT_BLOCK = ClassName.get(DATA_PACKAGE, "IntBlock");
    public static final ClassName INT_ARRAY_BLOCK = ClassName.get(DATA_PACKAGE, "IntArrayBlock");
    public static final ClassName INT_BIG_ARRAY_BLOCK = ClassName.get(DATA_PACKAGE, "IntBigArrayBlock");
    public static final ClassName LONG_BLOCK = ClassName.get(DATA_PACKAGE, "LongBlock");
    public static final ClassName DOUBLE_BLOCK = ClassName.get(DATA_PACKAGE, "DoubleBlock");
    public static final ClassName FLOAT_BLOCK = ClassName.get(DATA_PACKAGE, "FloatBlock");
    public static final ClassName EXPONENTIAL_HISTOGRAM_BLOCK = ClassName.get(DATA_PACKAGE, "ExponentialHistogramBlock");
    public static final ClassName EXPONENTIAL_HISTOGRAM_SCRATCH = ClassName.get(DATA_PACKAGE, "ExponentialHistogramScratch");
    public static final ClassName TDIGEST_BLOCK = ClassName.get(DATA_PACKAGE, "TDigestBlock");
    public static final ClassName LONG_RANGE_BLOCK = ClassName.get(DATA_PACKAGE, "LongRangeBlock");
    public static final ClassName DOUBLE_RANGE_BLOCK = ClassName.get(DATA_PACKAGE, "DoubleRangeBlock");

    static final ClassName BOOLEAN_BLOCK_BUILDER = BOOLEAN_BLOCK.nestedClass("Builder");
    static final ClassName BYTES_REF_BLOCK_BUILDER = BYTES_REF_BLOCK.nestedClass("Builder");
    static final ClassName INT_BLOCK_BUILDER = INT_BLOCK.nestedClass("Builder");
    static final ClassName LONG_BLOCK_BUILDER = LONG_BLOCK.nestedClass("Builder");
    static final ClassName DOUBLE_BLOCK_BUILDER = DOUBLE_BLOCK.nestedClass("Builder");
    static final ClassName FLOAT_BLOCK_BUILDER = FLOAT_BLOCK.nestedClass("Builder");
    static final ClassName EXPONENTIAL_HISTOGRAM_BLOCK_BUILDER = EXPONENTIAL_HISTOGRAM_BLOCK.nestedClass("Builder");
    static final ClassName TDIGEST_BLOCK_BUILDER = TDIGEST_BLOCK.nestedClass("Builder");
    static final ClassName LONG_RANGE_BLOCK_BUILDER = LONG_RANGE_BLOCK.nestedClass("Builder");
    static final ClassName DOUBLE_RANGE_BLOCK_BUILDER = DOUBLE_RANGE_BLOCK.nestedClass("Builder");

    static final ClassName ELEMENT_TYPE = ClassName.get(DATA_PACKAGE, "ElementType");

    static final ClassName BOOLEAN_VECTOR = ClassName.get(DATA_PACKAGE, "BooleanVector");
    static final ClassName BYTES_REF_VECTOR = ClassName.get(DATA_PACKAGE, "BytesRefVector");
    static final ClassName ORDINALS_BYTES_REF_VECTOR = ClassName.get(DATA_PACKAGE, "OrdinalBytesRefVector");
    static final ClassName INT_VECTOR = ClassName.get(DATA_PACKAGE, "IntVector");
    static final ClassName LONG_VECTOR = ClassName.get(DATA_PACKAGE, "LongVector");
    static final ClassName DOUBLE_VECTOR = ClassName.get(DATA_PACKAGE, "DoubleVector");
    static final ClassName FLOAT_VECTOR = ClassName.get(DATA_PACKAGE, "FloatVector");

    // Concrete Vector subtypes that the generated aggregator code dispatches on (see VECTOR_DISPATCH_SUBTYPES).
    private static final String ARROW_DATA_PACKAGE = DATA_PACKAGE + ".arrow";
    static final ClassName INT_ARRAY_VECTOR = ClassName.get(DATA_PACKAGE, "IntArrayVector");
    static final ClassName LONG_ARRAY_VECTOR = ClassName.get(DATA_PACKAGE, "LongArrayVector");
    static final ClassName DOUBLE_ARRAY_VECTOR = ClassName.get(DATA_PACKAGE, "DoubleArrayVector");
    static final ClassName FLOAT_ARRAY_VECTOR = ClassName.get(DATA_PACKAGE, "FloatArrayVector");
    static final ClassName CONSTANT_INT_VECTOR = ClassName.get(DATA_PACKAGE, "ConstantIntVector");
    static final ClassName CONSTANT_LONG_VECTOR = ClassName.get(DATA_PACKAGE, "ConstantLongVector");
    static final ClassName CONSTANT_DOUBLE_VECTOR = ClassName.get(DATA_PACKAGE, "ConstantDoubleVector");
    static final ClassName CONSTANT_FLOAT_VECTOR = ClassName.get(DATA_PACKAGE, "ConstantFloatVector");
    static final ClassName INT_ARROW_BUF_VECTOR = ClassName.get(ARROW_DATA_PACKAGE, "IntArrowBufVector");
    static final ClassName INT16_ARROW_BUF_VECTOR = ClassName.get(ARROW_DATA_PACKAGE, "Int16ArrowBufVector");
    static final ClassName INT8_ARROW_BUF_VECTOR = ClassName.get(ARROW_DATA_PACKAGE, "Int8ArrowBufVector");
    static final ClassName LONG_ARROW_BUF_VECTOR = ClassName.get(ARROW_DATA_PACKAGE, "LongArrowBufVector");
    static final ClassName DOUBLE_ARROW_BUF_VECTOR = ClassName.get(ARROW_DATA_PACKAGE, "DoubleArrowBufVector");
    static final ClassName FLOAT_ARROW_BUF_VECTOR = ClassName.get(ARROW_DATA_PACKAGE, "FloatArrowBufVector");

    /**
     * Concrete Vector subtypes the generated aggregator code dispatches on (one {@code getClass()} check
     * per block, then a monomorphic per-type body) so the per-element reads devirtualize. Keyed by the
     * abstract Vector interface; subtypes not listed fall through to a generic loop.
     */
    public static final Map<ClassName, List<ClassName>> VECTOR_DISPATCH_SUBTYPES = Map.ofEntries(
        Map.entry(
            INT_VECTOR,
            List.of(INT_ARRAY_VECTOR, INT_ARROW_BUF_VECTOR, INT16_ARROW_BUF_VECTOR, INT8_ARROW_BUF_VECTOR, CONSTANT_INT_VECTOR)
        ),
        Map.entry(LONG_VECTOR, List.of(LONG_ARRAY_VECTOR, LONG_ARROW_BUF_VECTOR, CONSTANT_LONG_VECTOR)),
        Map.entry(DOUBLE_VECTOR, List.of(DOUBLE_ARRAY_VECTOR, DOUBLE_ARROW_BUF_VECTOR, CONSTANT_DOUBLE_VECTOR)),
        Map.entry(FLOAT_VECTOR, List.of(FLOAT_ARRAY_VECTOR, FLOAT_ARROW_BUF_VECTOR, CONSTANT_FLOAT_VECTOR))
    );

    /**
     * For each Arrow-buffer Vector subtype in {@link #VECTOR_DISPATCH_SUBTYPES}, the FFM {@code ValueLayout}
     * field name describing how it stores each element off-heap. The bulk {@code MemorySegment} reduction
     * reads through this layout, and the stored value widens (signed) to the aggregator's element type.
     * <p>
     * Narrower-than-element widths ({@code Int16}/{@code Int8} feeding an {@code int} aggregator) are included
     * because the signed widening is value-preserving and the segment read is faster than the per-element
     * {@code ArrowBuf.getShort}/{@code getByte}. Unsigned Arrow widths are intentionally omitted: their
     * {@code getInt} masks the stored value, which a raw signed segment read would not reproduce, so they
     * fall through to the per-element loop.
     */
    public static final Map<ClassName, String> ARROW_VECTOR_VALUE_LAYOUT = Map.ofEntries(
        Map.entry(INT_ARROW_BUF_VECTOR, "JAVA_INT_UNALIGNED"),
        Map.entry(INT16_ARROW_BUF_VECTOR, "JAVA_SHORT_UNALIGNED"),
        Map.entry(INT8_ARROW_BUF_VECTOR, "JAVA_BYTE"),
        Map.entry(LONG_ARROW_BUF_VECTOR, "JAVA_LONG_UNALIGNED"),
        Map.entry(DOUBLE_ARROW_BUF_VECTOR, "JAVA_DOUBLE_UNALIGNED"),
        Map.entry(FLOAT_ARROW_BUF_VECTOR, "JAVA_FLOAT_UNALIGNED")
    );

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
    static final ClassName GROUPING_AGGREGATOR_FUNCTION_PREPARED_FOR_EVALUATION = ClassName.get(
        AGGREGATION_PACKAGE,
        "GroupingAggregatorFunction",
        "PreparedForEvaluation"
    );
    static final ClassName SEEN_GROUP_IDS = ClassName.get(AGGREGATION_PACKAGE, "SeenGroupIds");

    public static final ClassName INTERMEDIATE_STATE_DESC = ClassName.get(AGGREGATION_PACKAGE, "IntermediateStateDesc");
    public static final TypeName LIST_AGG_FUNC_DESC = ParameterizedTypeName.get(ClassName.get(List.class), INTERMEDIATE_STATE_DESC);

    public static final ClassName DRIVER_CONTEXT = ClassName.get(OPERATOR_PACKAGE, "DriverContext");
    public static final ClassName CONSTANT_METHOD_RESULT_SPECIALIZER = ClassName.get(OPERATOR_PACKAGE, "ConstantMethodResultSpecializer");
    public static final ClassName GROUPING_AGGREGATOR_EVALUATOR_CONTEXT = ClassName.get(
        AGGREGATION_PACKAGE,
        "GroupingAggregatorEvaluationContext"
    );

    public static final ClassName EXPRESSION_EVALUATOR = ClassName.get(EXPRESSION_PACKAGE, "ExpressionEvaluator");
    public static final ClassName EXPRESSION_EVALUATOR_FACTORY = ClassName.get(EXPRESSION_PACKAGE, "ExpressionEvaluator", "Factory");
    public static final ClassName ABSTRACT_MULTIVALUE_FUNCTION_EVALUATOR = ClassName.get(
        "org.elasticsearch.xpack.esql.expression.function.scalar.multivalue",
        "AbstractMultivalueFunction",
        "AbstractEvaluator"
    );
    public static final ClassName ABSTRACT_NULLABLE_MULTIVALUE_FUNCTION_EVALUATOR = ClassName.get(
        "org.elasticsearch.xpack.esql.expression.function.scalar.multivalue",
        "AbstractMultivalueFunction",
        "AbstractNullableEvaluator"
    );
    public static final ClassName ABSTRACT_CONVERT_FUNCTION_EVALUATOR = ClassName.get(
        "org.elasticsearch.xpack.esql.expression.function.scalar.convert",
        "AbstractConvertFunction",
        "AbstractEvaluator"
    );

    static final ClassName WARNINGS = ClassName.get("org.elasticsearch.compute.operator", "Warnings");
    static final ClassName WARNING_SOURCE_LOCATION = ClassName.get("org.elasticsearch.compute.operator", "WarningSourceLocation");

    static final ClassName SOURCE = ClassName.get("org.elasticsearch.xpack.esql.core.tree", "Source");

    public static final ClassName BYTES_REF = ClassName.get("org.apache.lucene.util", "BytesRef");
    public static final ClassName EXPONENTIAL_HISTOGRAM = ClassName.get("org.elasticsearch.exponentialhistogram", "ExponentialHistogram");
    public static final ClassName TDIGEST = ClassName.get("org.elasticsearch.compute.data", "TDigestHolder");
    public static final ClassName LONG_RANGE = ClassName.get(DATA_PACKAGE, "LongRangeBlockBuilder", "LongRange");
    public static final ClassName DOUBLE_RANGE = ClassName.get(DATA_PACKAGE, "DoubleRangeBlockBuilder", "DoubleRange");

    public static final ClassName RELEASABLE = ClassName.get("org.elasticsearch.core", "Releasable");
    public static final ClassName RELEASABLES = ClassName.get("org.elasticsearch.core", "Releasables");

    private record TypeDef(TypeName type, String alias, ClassName block, ClassName vector, ClassName scratch) {

        public static TypeDef of(TypeName type, String alias, String block, String vector, ClassName scratch) {
            return new TypeDef(
                type,
                alias,
                ClassName.get(DATA_PACKAGE, block),
                vector == null ? null : ClassName.get(DATA_PACKAGE, vector),
                scratch
            );
        }
    }

    private static final Map<String, TypeDef> TYPES = Stream.of(
        TypeDef.of(TypeName.BOOLEAN, "BOOLEAN", "BooleanBlock", "BooleanVector", null),
        TypeDef.of(TypeName.INT, "INT", "IntBlock", "IntVector", null),
        TypeDef.of(TypeName.LONG, "LONG", "LongBlock", "LongVector", null),
        TypeDef.of(TypeName.FLOAT, "FLOAT", "FloatBlock", "FloatVector", null),
        TypeDef.of(TypeName.DOUBLE, "DOUBLE", "DoubleBlock", "DoubleVector", null),
        TypeDef.of(BYTES_REF, "BYTES_REF", "BytesRefBlock", "BytesRefVector", BYTES_REF),
        TypeDef.of(EXPONENTIAL_HISTOGRAM, "EXPONENTIAL_HISTOGRAM", "ExponentialHistogramBlock", null, EXPONENTIAL_HISTOGRAM_SCRATCH),
        TypeDef.of(TDIGEST, "TDIGEST", "TDigestBlock", null, TDIGEST),
        TypeDef.of(LONG_RANGE, "LONG_RANGE", "LongRangeBlock", null, LONG_RANGE),
        TypeDef.of(DOUBLE_RANGE, "DOUBLE_RANGE", "DoubleRangeBlock", null, DOUBLE_RANGE)
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

    public static ClassName blockType(TypeName elementType) {
        return blockType(elementType.toString());
    }

    static ClassName blockType(String elementType) {
        return findRequired(elementType, "block").block;
    }

    public static ClassName vectorType(TypeName elementType) {
        return vectorType(elementType.toString());
    }

    static ClassName vectorType(String elementType) {
        return findRequired(elementType, "vector").vector;
    }

    public static ClassName scratchType(String elementType) {
        TypeDef typeDef = TYPES.get(elementType);
        if (typeDef != null) {
            return typeDef.scratch;
        }
        return null;
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
        if (resultType.equals(EXPONENTIAL_HISTOGRAM_BLOCK)) {
            return EXPONENTIAL_HISTOGRAM_BLOCK_BUILDER;
        }
        if (resultType.equals(TDIGEST_BLOCK)) {
            return TDIGEST_BLOCK_BUILDER;
        }
        if (resultType.equals(LONG_RANGE_BLOCK)) {
            return LONG_RANGE_BLOCK_BUILDER;
        }
        if (resultType.equals(DOUBLE_RANGE_BLOCK)) {
            return DOUBLE_RANGE_BLOCK_BUILDER;
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

    public static TypeName elementType(TypeName t) {
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
        if (t.equals(FLOAT_BLOCK) || t.equals(FLOAT_VECTOR) || t.equals(FLOAT_BLOCK_BUILDER)) {
            return TypeName.FLOAT;
        }
        if (t.equals(EXPONENTIAL_HISTOGRAM_BLOCK) || t.equals(EXPONENTIAL_HISTOGRAM_BLOCK_BUILDER)) {
            return EXPONENTIAL_HISTOGRAM;
        }
        if (t.equals(TDIGEST_BLOCK) || t.equals(TDIGEST_BLOCK_BUILDER)) {
            return TDIGEST;
        }
        if (t.equals(LONG_RANGE_BLOCK) || t.equals(LONG_RANGE_BLOCK_BUILDER)) {
            return LONG_RANGE;
        }
        if (t.equals(DOUBLE_RANGE_BLOCK) || t.equals(DOUBLE_RANGE_BLOCK_BUILDER)) {
            return DOUBLE_RANGE;
        }
        throw new IllegalArgumentException("unknown element type for [" + t + "]");
    }

    public static boolean extendsSuper(javax.lang.model.util.Types types, TypeMirror c, String superName) {
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
