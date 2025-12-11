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
import org.elasticsearch.xpack.esql.core.expression.ExpressionContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
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

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * In a single-parameter mode, the function converts the first parameter to a long.
 * <ul>
 *     <li>TO_LONG(value) - value supports boolean, date, date_nanos, keyword, text,
 *         double, long, unsigned_long, integer, counter_integer, counter_long,
 *         geohash, geotile, geohex
 *     </li>
 * </ul>
 * <br/>
 * In two-parameter mode, the function parses the first string parameter into a long
 * using the second parameter as a base.
 * <ul>
 *     <li>TO_LONG(string, base) - base supports integer, long and unsigned_long
 *     </li>
 * </ul>
 */

public class ToLongSurrogate extends EsqlScalarFunction implements SurrogateExpression, OptionalArgument, ConvertFunction {

    private final Expression field;
    private final Expression base;

    @FunctionInfo(
        returnType = "long",
        description = """
            Converts the input value to a long.
            If the input parameter is of a date type, its value will be interpreted as milliseconds
            since the {wikipedia}/Unix_time[Unix epoch], converted to long.
            Boolean `true` will be converted to long `1`, `false` to `0`.""",

        detailedDescription = """
            When given two arguments, a string value and a whole number base,
            the string is parsed as a long in the given base.
            If parsing fails a warning is generated as described below and the result is null.
            A leading '0x' prefix is allowed for base 16.
            {applies_to}`stack: ga 9.3`
            """,

        examples = {
            @Example(file = "ints", tag = "to_long-str", explanation = """
                Note in this example the last conversion of the string isn’t possible.
                When this happens, the result is a `null` value.
                In this case a _Warning_ header is added to the response.
                The header will provide information on the source of the failure:

                `"Line 1:113: evaluation of [TO_LONG(str3)] failed, treating result as null. Only first 20 failures recorded."`

                A following header will contain the failure reason and the offending value:

                `"java.lang.NumberFormatException: For input string: \"foo\""`"""),

            @Example(
                file = "ints",
                tag = "to_long_base-str1",
                explanation = "" + "This example demonstrates parsing a base 16 value and a base 13 value. {applies_to}`stack: ga 9.3`"
            ),

            @Example(
                file = "ints",
                tag = "to_long_base-str2",
                explanation = ""
                    + "This example demonstrates parsing a string that is valid in base 36 but invalid in base 10."
                    + "Observe in the second case a warning is generated and null is returned. {applies_to}`stack: ga 9.3`"
            ) }
    )
    public ToLongSurrogate(
        Source source,

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

        @Param(
            optional = true,
            name = "base",
            type = { "integer", "long", "unsigned_long" },
            description = "(Optional) Radix or base used to convert the input value."
                + "When a base is specified the input type must be `keyword` or `text`."
                + "{applies_to}`stack: ga 9.3`"
        ) Expression base
    ) {
        super(source, base == null ? List.of(field) : List.of(field, base));
        this.field = field;
        this.base = base;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException("should be rewritten");
    }

    @Override
    public DataType dataType() {
        return LONG;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (base != null) {
            // two parameter TO_LONG(string, base) supports more restricted types

            TypeResolution resolution = TypeResolutions.isString(field, sourceText(), FIRST);
            if (resolution.resolved()) {
                resolution = TypeResolutions.isWholeNumber(base, sourceText(), SECOND);
            }
            return resolution;

        } else {

            // single parameter TO_LONG(field) supports many types
            return (new ToLong(source(), field)).resolveType();
        }
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToLongSurrogate(source(), newChildren.get(0), newChildren.size() == 1 ? null : newChildren.get(1));
    }

    @Override
    public Expression surrogate(ExpressionContext ctx) {
        if (base != null) {
            // two parameter TO_LONG(string, base)

            switch (field.dataType()) {
                case DataType.KEYWORD:
                case DataType.TEXT:
                case DataType.NULL:
                    break;
                default:
                    throw new UnsupportedOperationException("may not specify base with non-string field " + field.dataType());
            }

            switch (base.dataType()) {
                case DataType.INTEGER:
                case DataType.LONG:
                case DataType.UNSIGNED_LONG:
                case DataType.NULL:
                    break;
                default:
                    throw new UnsupportedOperationException("base must be a whole number, not " + base.dataType());
            }

            return new ToLongBase(source(), field, new ToInteger(source(), base));

        } else {

            // single parameter TO_LONG(field)
            return new ToLong(source(), field);
        }
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToLongSurrogate::new, field, base);
    }

    @Override
    public Expression field() {
        return field;
    }

    @Override
    public Set<DataType> supportedTypes() {
        return ToLong.EVALUATORS.keySet();
    }

}
