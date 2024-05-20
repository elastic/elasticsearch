/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToLong;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeDoubleToLong;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

public class ToLong extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(LONG, (fieldEval, source) -> fieldEval),
        Map.entry(DATETIME, (fieldEval, source) -> fieldEval),
        Map.entry(BOOLEAN, ToLongFromBooleanEvaluator.Factory::new),
        Map.entry(KEYWORD, ToLongFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToLongFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToLongFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToLongFromUnsignedLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToLongFromIntEvaluator.Factory::new), // CastIntToLongEvaluator would be a candidate, but not MV'd
        Map.entry(EsqlDataTypes.COUNTER_LONG, (field, source) -> field),
        Map.entry(EsqlDataTypes.COUNTER_INTEGER, ToLongFromIntEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "long",
        description = """
            Converts an input value to a long value. If the input parameter is of a date type,
            its value will be interpreted as milliseconds since the {wikipedia}/Unix_time[Unix epoch], converted to long.
            Boolean *true* will be converted to long *1*, *false* to *0*.""",
        examples = @Example(file = "ints", tag = "to_long-str", explanation = """
            Note that in this example, the last conversion of the string isn't possible.
            When this happens, the result is a *null* value. In this case a _Warning_ header is added to the response.
            The header will provide information on the source of the failure:

            `"Line 1:113: evaluation of [TO_LONG(str3)] failed, treating result as null. Only first 20 failures recorded."`

            A following header will contain the failure reason and the offending value:

            `"java.lang.NumberFormatException: For input string: \"foo\""`""")
    )
    public ToLong(
        Source source,
        @Param(
            name = "field",
            type = {
                "boolean",
                "date",
                "keyword",
                "text",
                "double",
                "long",
                "unsigned_long",
                "integer",
                "counter_integer",
                "counter_long" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return LONG;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToLong(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToLong::new, field());
    }

    @ConvertEvaluator(extraName = "FromBoolean")
    static long fromBoolean(boolean bool) {
        return bool ? 1L : 0L;
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { InvalidArgumentException.class })
    static long fromKeyword(BytesRef in) {
        return stringToLong(in.utf8ToString());
    }

    @ConvertEvaluator(extraName = "FromDouble", warnExceptions = { InvalidArgumentException.class })
    static long fromDouble(double dbl) {
        return safeDoubleToLong(dbl);
    }

    @ConvertEvaluator(extraName = "FromUnsignedLong", warnExceptions = { InvalidArgumentException.class })
    static long fromUnsignedLong(long ul) {
        return unsignedLongToLong(ul);
    }

    @ConvertEvaluator(extraName = "FromInt")
    static long fromInt(int i) {
        return i;
    }
}
