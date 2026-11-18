/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.Signature;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

/**
 * Clamps the values of all samples to have a lower limit of min and an upper limit of max.
 */
public class Clamp extends EsqlScalarFunction implements OnlySurrogateExpression, AnyNullIsNull {
    private final Expression field;
    private final Expression min;
    private final Expression max;
    private DataType resolvedType;

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Clamp.class).ternary(Clamp::new).name("clamp");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        // Prometheus clamp returns an empty result (drops the series) when max < min, whereas ES|QL clamp would return
        // the bound. Emit NULL in that case so applyNullOutputFilter drops the row, matching Prometheus.
        .ternaryValueTransformation(
            PromqlFunctionDefinition.MIN_SCALAR,
            PromqlFunctionDefinition.MAX_SCALAR,
            // PromQL values are doubles, and converting the field also lets an unsigned_long field be clamped by numeric bounds
            (source, field, min, max) -> new Case(
                source,
                new LessThan(source, max, min),
                List.of(new Literal(source, null, NULL), new Clamp(source, new ToDouble(source, field), min, max))
            )
        )
        .description("Clamps the sample values of all elements to be within [min, max].")
        .example("clamp(http_requests_total, 0, 100)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .name("clamp");

    @FunctionInfo(
        returnType = { "double", "integer", "long", "unsigned_long", "keyword", "ip", "boolean", "date", "date_nanos", "version" },
        signatures = {
            @Signature(params = { "boolean", "boolean", "boolean" }, returnType = "boolean"),
            @Signature(params = { "date", "date", "date" }, returnType = "date"),
            @Signature(params = { "date_nanos", "date_nanos", "date_nanos" }, returnType = "date_nanos"),
            @Signature(params = { "integer", "integer", "integer" }, returnType = "integer"),
            @Signature(params = { "long", "integer|long", "integer|long" }, returnType = "long"),
            @Signature(params = { "integer", "long", "integer|long" }, returnType = "long"),
            @Signature(params = { "integer", "integer", "long" }, returnType = "long"),
            @Signature(params = { "double", "integer|long|double", "integer|long|double" }, returnType = "double"),
            @Signature(params = { "integer|long", "double", "integer|long|double" }, returnType = "double"),
            @Signature(params = { "integer|long", "integer|long", "double" }, returnType = "double"),
            @Signature(params = { "ip", "ip", "ip" }, returnType = "ip"),
            @Signature(params = { "STRING", "STRING", "STRING" }, returnType = "keyword"),
            // unsigned_long can't be mixed with other numeric types, like in comparisons
            @Signature(params = { "unsigned_long", "unsigned_long", "unsigned_long" }, returnType = "unsigned_long"),
            @Signature(params = { "version", "version", "version" }, returnType = "version") },
        briefSummary = "Clamps values to a specified minimum and maximum range.",
        description = "Limits (or clamps) the values of all samples to have a lower limit of min and an upper limit of max.",
        examples = { @Example(file = "k8s-timeseries-clamp", tag = "clamp") },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.3.0") }
    )
    public Clamp(
        Source source,
        @Param(
            name = "field",
            type = { "double", "integer", "long", "unsigned_long", "keyword", "text", "ip", "boolean", "date", "date_nanos", "version" },
            description = "Expression to clamp. If `null`, the function returns `null`."
        ) Expression field,
        @Param(
            name = "min",
            type = { "double", "integer", "long", "unsigned_long", "keyword", "text", "ip", "boolean", "date", "date_nanos", "version" },
            description = "The min value to clamp data into."
        ) Expression min,
        @Param(
            name = "max",
            type = { "double", "integer", "long", "unsigned_long", "keyword", "text", "ip", "boolean", "date", "date_nanos", "version" },
            description = "The max value to clamp data into."
        ) Expression max
    ) {
        super(source, List.of(field, min, max));
        this.field = field;
        this.min = min;
        this.max = max;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("Clamp does not support serialization.");
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        var field = children().get(0);
        var min = children().get(1);
        var max = children().get(2);
        var fieldDataType = field.dataType().noText();
        TypeResolution resolution = resolveFieldType(field, sourceText(), "clamp");
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = resolveBoundType(field, min, sourceText(), TypeResolutions.ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = resolveBoundType(field, max, sourceText(), TypeResolutions.ParamOrdinal.THIRD);
        if (resolution.unresolved()) {
            return resolution;
        }
        if (fieldDataType.isNumeric() == false) {
            resolvedType = fieldDataType;
        } else {
            // When the types are equally wide, prefer rational numbers
            resolvedType = Stream.of(fieldDataType, min.dataType(), max.dataType())
                .sorted(Comparator.comparingInt(DataType::estimatedSize).thenComparing(DataType::isRationalNumber))
                .toList()
                .getLast();
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * Resolves the type of the {@code field} of {@code CLAMP}, {@code CLAMP_MIN} and {@code CLAMP_MAX}, which must have
     * a total order.
     */
    public static TypeResolution resolveFieldType(Expression field, String sourceText, String functionName) {
        TypeResolution resolution = TypeResolutions.isType(
            field,
            t -> t.isNumeric() || t == DataType.BOOLEAN || t.isDate() || DataType.isString(t) || t == DataType.IP || t == DataType.VERSION,
            sourceText,
            TypeResolutions.ParamOrdinal.FIRST,
            "boolean",
            "date",
            "date_nanos",
            "ip",
            "numeric",
            "string",
            "version"
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        if (field.dataType() == NULL) {
            return new TypeResolution("'field' must not be null in " + functionName + "()");
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * Resolves the type of a bound of {@code CLAMP}, {@code CLAMP_MIN} and {@code CLAMP_MAX}, which must be comparable with
     * the already resolved {@code field}. Numeric types are converted to the widest one, except {@code unsigned_long},
     * which can only be compared with another {@code unsigned_long}.
     */
    public static TypeResolution resolveBoundType(
        Expression field,
        Expression bound,
        String sourceText,
        TypeResolutions.ParamOrdinal boundOrdinal
    ) {
        DataType fieldDataType = field.dataType().noText();
        String boundType = fieldDataType.isNumeric() ? "numeric" : DataType.isString(fieldDataType) ? "string" : fieldDataType.typeName();
        TypeResolution resolution = TypeResolutions.isType(
            bound,
            t -> t.isNumeric() ? fieldDataType.isNumeric() : t.noText() == fieldDataType,
            sourceText,
            boundOrdinal,
            boundType
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        DataType boundDataType = bound.dataType();
        if (boundDataType.isNumeric() && (fieldDataType == UNSIGNED_LONG) != (boundDataType == UNSIGNED_LONG)) {
            return new TypeResolution(
                format(
                    null,
                    "first argument of [{}] is [{}] and {} argument is [{}]. "
                        + "[unsigned_long] can only be operated on together with another [unsigned_long]",
                    sourceText,
                    fieldDataType.typeName(),
                    boundOrdinal.name().toLowerCase(Locale.ROOT),
                    boundDataType.typeName()
                )
            );
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        if (resolvedType == null && resolveType().resolved() == false) {
            throw new EsqlIllegalArgumentException("Unable to resolve data type for clamp");
        }
        return resolvedType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Clamp(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Clamp::new, field, children().get(1), children().get(2));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Clamp does not support serialization.");
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException(
            "Clamp should have been replaced by ClampMin and ClampMax. Something went wrong in the compute engine."
        );
    }

    @Override
    public Expression surrogate() {
        return new ClampMax(source(), new ClampMin(source(), field, min), max);
    }
}
