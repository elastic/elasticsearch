/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.Signature;
import org.elasticsearch.xpack.esql.expression.function.scalar.Clamp;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;

/**
 * Clamps the input values to have an upper limit of max.
 */
public class ClampMax extends EsqlScalarFunction implements AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ClampMax", ClampMax::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ClampMax.class).binary(ClampMax::new).name("clamp_max");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        // PromQL values are doubles, and converting the field also lets an unsigned_long field be clamped by a numeric bound
        .binaryValueTransformation(
            PromqlFunctionDefinition.MAX_SCALAR,
            (source, field, max) -> new ClampMax(source, new ToDouble(source, field), max)
        )
        .description("Clamps the sample values of all elements to have an upper limit of max.")
        .example("clamp_max(http_requests_total, 100)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("clamp_max");

    private DataType resolvedType;

    @FunctionInfo(
        returnType = { "double", "integer", "long", "unsigned_long", "keyword", "ip", "boolean", "date", "date_nanos", "version" },
        signatures = {
            @Signature(params = { "boolean", "boolean" }, returnType = "boolean"),
            @Signature(params = { "date", "date" }, returnType = "date"),
            @Signature(params = { "date_nanos", "date_nanos" }, returnType = "date_nanos"),
            @Signature(params = { "integer", "integer" }, returnType = "integer"),
            @Signature(params = { "long", "integer|long" }, returnType = "long"),
            @Signature(params = { "integer", "long" }, returnType = "long"),
            @Signature(params = { "double", "integer|long|double" }, returnType = "double"),
            @Signature(params = { "integer|long", "double" }, returnType = "double"),
            @Signature(params = { "ip", "ip" }, returnType = "ip"),
            @Signature(params = { "STRING", "STRING" }, returnType = "keyword"),
            // unsigned_long can't be mixed with other numeric types, like in comparisons
            @Signature(params = { "unsigned_long", "unsigned_long" }, returnType = "unsigned_long"),
            @Signature(params = { "version", "version" }, returnType = "version") },
        briefSummary = "Clamps input values to an upper bound, reducing any value above max to max.",
        description = "Limits (or clamps) all input sample values to an upper bound of max. Any value above max is reduced to max.",
        examples = @Example(file = "k8s-timeseries-clamp", tag = "clamp-max"),
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") }
    )
    public ClampMax(
        Source source,
        @Param(
            name = "field",
            type = { "double", "integer", "long", "unsigned_long", "keyword", "text", "ip", "boolean", "date", "date_nanos", "version" },
            description = "Expression to clamp. If `null`, the function returns `null`."
        ) Expression field,
        @Param(
            name = "max",
            type = { "double", "integer", "long", "unsigned_long", "keyword", "text", "ip", "boolean", "date", "date_nanos", "version" },
            description = "The max value to clamp data into."
        ) Expression max
    ) {
        super(source, List.of(field, max));
    }

    private ClampMax(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeNamedWriteable(children().get(1));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        if (resolvedType == null && resolveType().resolved() == false) {
            throw new EsqlIllegalArgumentException("Unable to resolve data type for clamp_max");
        }
        return resolvedType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        var field = children().get(0);
        var max = children().get(1);
        var fieldDataType = field.dataType().noText();
        TypeResolution resolution = Clamp.resolveFieldType(field, sourceText(), "clamp_max");
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = Clamp.resolveBoundType(field, max, sourceText(), TypeResolutions.ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        if (fieldDataType.isNumeric() == false) {
            resolvedType = fieldDataType;
        } else if (fieldDataType.estimatedSize() == max.dataType().estimatedSize()) {
            // When the types are equally wide, prefer rational numbers
            resolvedType = fieldDataType.isRationalNumber() ? fieldDataType : max.dataType();
        } else {
            // Otherwise, prefer the wider type
            resolvedType = fieldDataType.estimatedSize() > max.dataType().estimatedSize() ? fieldDataType : max.dataType();
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ClampMax(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ClampMax::new, children().get(0), children().get(1));
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var outputType = PlannerUtils.toElementType(dataType());

        var fieldEval = PlannerUtils.toElementType(children().getFirst().dataType()) != outputType
            ? Cast.cast(source(), children().getFirst().dataType(), dataType(), toEvaluator.apply(children().get(0)))
            : toEvaluator.apply(children().getFirst());
        var maxEval = PlannerUtils.toElementType(children().get(1).dataType()) != outputType
            ? Cast.cast(source(), children().get(1).dataType(), dataType(), toEvaluator.apply(children().get(1)))
            : toEvaluator.apply(children().get(1));

        return switch (outputType) {
            case BOOLEAN -> new ClampMaxBooleanEvaluator.Factory(source(), fieldEval, maxEval);
            case DOUBLE -> new ClampMaxDoubleEvaluator.Factory(source(), fieldEval, maxEval);
            case INT -> new ClampMaxIntegerEvaluator.Factory(source(), fieldEval, maxEval);
            case LONG -> new ClampMaxLongEvaluator.Factory(source(), fieldEval, maxEval);
            case BYTES_REF -> new ClampMaxBytesRefEvaluator.Factory(source(), fieldEval, maxEval);
            default -> throw EsqlIllegalArgumentException.illegalDataType(dataType());
        };
    }

    @Evaluator(extraName = "Boolean")
    static boolean process(boolean field, boolean max) {
        if (max == false) {
            return false;
        } else {
            return field;
        }
    }

    @Evaluator(extraName = "BytesRef")
    static BytesRef process(BytesRef field, BytesRef max) {
        if (field.compareTo(max) > 0) {
            return max;
        } else {
            return field;
        }
    }

    @Evaluator(extraName = "Integer")
    static int process(int field, int max) {
        return Math.min(field, max);
    }

    @Evaluator(extraName = "Long")
    static long process(long field, long max) {
        return Math.min(field, max);
    }

    @Evaluator(extraName = "Double")
    static double process(double field, double max) {
        return Math.min(field, max);
    }
}
