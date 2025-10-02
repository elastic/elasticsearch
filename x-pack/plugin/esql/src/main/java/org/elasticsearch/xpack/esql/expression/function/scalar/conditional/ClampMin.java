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
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

/**
 * Returns the input values clamped to have a lower limit of min.
 */
public class ClampMin extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ClampMin", ClampMin::new);

    private DataType dataType;

    @FunctionInfo(
        returnType = { "boolean", "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "version" },
        description = "Returns clamps the values of all input samples clamped to have a lower limit of min.",
        examples = @Example(file = "math", tag = "Clampmin")
    )
    public ClampMin(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "version" },
            description = "field to clamp."
        ) Expression field,
        @Param(
            name = "min",
            type = { "boolean", "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "version" },
            description = "The min value to clamp data into."
        ) Expression min
    ) {
        super(source, List.of(field, min));
    }

    private ClampMin(StreamInput in) throws IOException {
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
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        var field = children().get(0);
        var min = children().get(1);
        var fieldDataType = field.dataType();
        TypeResolution resolution = TypeResolutions.isType(
            field,
            t -> t.isNumeric() || t == DataType.BOOLEAN || t.isDate() || DataType.isString(t) || t == DataType.IP || t == DataType.VERSION,
            sourceText(),
            TypeResolutions.ParamOrdinal.FIRST,
            fieldDataType.typeName()
        );
        if (resolution.unresolved()) {
            dataType = NULL;
            return resolution;
        }
        if (fieldDataType == NULL) {
            dataType = NULL;
            return new TypeResolution("'field' must not be null in clamp()");
        }
        resolution = TypeResolutions.isType(
            min,
            t -> t.isNumeric() ? fieldDataType.isNumeric() : t.noText() == fieldDataType,
            sourceText(),
            TypeResolutions.ParamOrdinal.SECOND,
            fieldDataType.typeName()
        );
        if (resolution.unresolved()) {
            dataType = NULL;
            return resolution;
        }
        dataType = fieldDataType;
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ClampMin(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ClampMin::new, children().get(0), children().get(1));
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // force datatype initialization
        var outputType = dataType();

        var min = children().get(1);
        var minF = outputType != min.dataType()
            ? Cast.cast(source(), min.dataType(), outputType, toEvaluator.apply(min))
            : toEvaluator.apply(min);
        if (outputType == DataType.BOOLEAN) {
            return new ClampMinBooleanEvaluator.Factory(source(), toEvaluator.apply(children().get(0)), minF);
        }
        if (outputType == DataType.DOUBLE) {
            return new ClampMinDoubleEvaluator.Factory(source(), toEvaluator.apply(children().get(0)), minF);
        }
        if (outputType == DataType.INTEGER) {
            return new ClampMinIntegerEvaluator.Factory(source(), toEvaluator.apply(children().get(0)), minF);
        }
        if (outputType == DataType.UNSIGNED_LONG
            || outputType == DataType.LONG
            || outputType == DataType.DATETIME
            || outputType == DataType.DATE_NANOS) {
            return new ClampMinLongEvaluator.Factory(source(), toEvaluator.apply(children().get(0)), minF);
        }
        if (DataType.isString(outputType)
            || outputType == DataType.IP
            || outputType == DataType.VERSION
            || outputType == DataType.UNSUPPORTED) {

            return new ClampMinBytesRefEvaluator.Factory(source(), toEvaluator.apply(children().get(0)), minF);
        }
        throw EsqlIllegalArgumentException.illegalDataType(outputType);
    }

    @Evaluator(extraName = "Boolean")
    static boolean process(boolean field, boolean min) {
        if (min) {
            return true;
        } else {
            return field;
        }
    }

    @Evaluator(extraName = "BytesRef")
    static BytesRef process(BytesRef field, BytesRef min) {
        return field.compareTo(min) < 0 ? min : field;
    }

    @Evaluator(extraName = "Integer")
    static int process(int field, int min) {
        return Math.max(field, min);
    }

    @Evaluator(extraName = "Long")
    static long process(long field, long min) {
        return Math.max(field, min);
    }

    @Evaluator(extraName = "Double")
    static double process(double field, double min) {
        return Math.max(field, min);
    }
}
