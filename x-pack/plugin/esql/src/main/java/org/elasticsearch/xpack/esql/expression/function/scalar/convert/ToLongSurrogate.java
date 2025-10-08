/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;


import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Converts values to long.  Explain more here.
 * <p>
 *     Like ToIp, this doesn't extend from {@link AbstractConvertFunction} so that it
 *     can support an optional number base.  It rewrites itself into {@link ToLong}, ...
 *     which are all {@link AbstractConvertFunction} subclasses.
 *     This keeps the conversion code happy while still allowing us to expose a single method to users.
 * </p>
 */

public class ToLongSurrogate extends EsqlScalarFunction implements SurrogateExpression, OptionalArgument, ConvertFunction {

    private final Expression field;
    private final Expression base;

    @FunctionInfo(
        returnType = "long",
        description = """
            Converts an input value to a long value. If the input parameter is of a date type,
            its value will be interpreted as milliseconds since the {wikipedia}/Unix_time[Unix epoch], converted to long.
            Boolean `true` will be converted to long `1`, `false` to `0`.""",
        examples = @Example(file = "ints", tag = "to_long-str", explanation = """
            Note that in this example, the last conversion of the string isn’t possible.
            When this happens, the result is a `null` value. In this case a _Warning_ header is added to the response.
            The header will provide information on the source of the failure:

            `"Line 1:113: evaluation of [TO_LONG(str3)] failed, treating result as null. Only first 20 failures recorded."`

            A following header will contain the failure reason and the offending value:

            `"java.lang.NumberFormatException: For input string: \"foo\""`""")
    )
    public ToLongSurrogate(
        Source source,

        // ──────────────────────────────────────────────────── ToLong
        @Param(
            name = "field",
            type = {
                "boolean",
                "date",
                "date_nanos",
                "keyword",
                "text",
                "double",
                "long",
                "unsigned_long",
                "integer",
                "counter_integer",
                "counter_long",
                "geohash",
                "geotile",
                "geohex" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field,

        // ──────────────────────────────────────────────────── CountDistinctOverTime
        @Param(
            name = "base",
            type = { "integer", "long", "unsigned_long" },
            description = "(Optional) Radix or base used to convert the string. Defaults to base 10.",
            optional = true
        ) Expression base
    ) {
        super(source, base == null ? List.of(field) : List.of(field, base));
        this.field = field;
        this.base = base;
    }

    // ──────────────────────────────────────────────────────── ToIP
    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    // ──────────────────────────────────────────────────────── ToLong
    @Override
    public DataType dataType() {
        return LONG;
    }

    // ──────────────────────────────────────────────────────── ToIP
    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToLongSurrogate(source(), newChildren.get(0), newChildren.size() == 1 ? null : newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToLongSurrogate::new, field, base);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException("should be rewritten");
    }

    // SurrogateExpression ──────────────────────────────────── Max (aggregate)
    @Override
    public Expression surrogate() {
        if (base != null) {
            if (DataType.isString( field.dataType() ) == false) {
                throw new UnsupportedOperationException("may not specify base with non-string field");
            }
            if (base.dataType().isWholeNumber() == false) {
                throw new UnsupportedOperationException("base must be a whole number");
            }
            return new ToLongBase(source(), field, base);
        }
        return new ToLong(source(), field);
    }

    // ConvertFunction ──────────────────────────────────────── ToIP
    @Override
    public Expression field() {
        return field;
    }

    // ConvertFunction ──────────────────────────────────────── ToIP
    @Override
    public Set<DataType> supportedTypes() {
        return ToLong.EVALUATORS.keySet();
    }

}
