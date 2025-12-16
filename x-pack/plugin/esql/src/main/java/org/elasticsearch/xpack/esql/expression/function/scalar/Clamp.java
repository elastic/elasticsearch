/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMax;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.ClampMin;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

/**
 * Clamps the values of all samples to have a lower limit of min and an upper limit of max.
 */
public class Clamp extends EsqlScalarFunction implements SurrogateExpression {
    private final Expression field;
    private final Expression min;
    private final Expression max;
    private DataType resolvedType;

    @FunctionInfo(
        returnType = { "double", "integer", "long", "double", "unsigned_long", "keyword", "ip", "boolean", "date", "version" },
        description = "Clamps the values of all samples to have a lower limit of min and an upper limit of max.",
        examples = { @Example(file = "k8s-timeseries-clamp", tag = "clamp") }
    )
    public Clamp(
        Source source,
        @Param(
            name = "field",
            type = { "double", "integer", "long", "double", "unsigned_long", "keyword", "ip", "boolean", "date", "version" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression field,
        @Param(
            name = "min",
            type = { "double", "integer", "long", "double", "unsigned_long", "keyword", "ip", "boolean", "date", "version" },
            description = "The min value to clamp data into."
        ) Expression min,
        @Param(
            name = "max",
            type = { "double", "integer", "long", "double", "unsigned_long", "keyword", "ip", "boolean", "date", "version" },
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
        var max = children().get(1);
        var min = children().get(2);
        var fieldDataType = field.dataType().noText();
        TypeResolution resolution = TypeResolutions.isType(
            field,
            t -> t.isNumeric() || t == DataType.BOOLEAN || t.isDate() || DataType.isString(t) || t == DataType.IP || t == DataType.VERSION,
            sourceText(),
            TypeResolutions.ParamOrdinal.FIRST,
            fieldDataType.typeName()
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        if (fieldDataType == NULL) {
            return new TypeResolution("'field' must not be null in clamp()");
        }
        resolution = TypeResolutions.isType(
            max,
            t -> t.isNumeric() ? fieldDataType.isNumeric() : t.noText() == fieldDataType.noText(),
            sourceText(),
            TypeResolutions.ParamOrdinal.SECOND,
            fieldDataType.typeName()
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = TypeResolutions.isType(
            min,
            t -> t.isNumeric() ? fieldDataType.isNumeric() : t.noText() == fieldDataType.noText(),
            sourceText(),
            TypeResolutions.ParamOrdinal.THIRD,
            fieldDataType.typeName()
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        if (fieldDataType.isNumeric() == false) {
            resolvedType = fieldDataType;
        } else {
            // When the types are equally wide, prefer rational numbers
            resolvedType = Stream.of(fieldDataType, max.dataType(), min.dataType())
                .sorted(Comparator.comparingInt(DataType::estimatedSize).thenComparing(DataType::isRationalNumber))
                .toList()
                .getLast();
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        if (resolvedType == null && resolveType().resolved() == false) {
            throw new EsqlIllegalArgumentException("Unable to resolve data type for clamp_max");
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
