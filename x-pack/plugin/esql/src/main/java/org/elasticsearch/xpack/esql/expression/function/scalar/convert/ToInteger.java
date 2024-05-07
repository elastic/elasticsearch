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

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToInt;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToInt;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeToInt;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

public class ToInteger extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(INTEGER, (fieldEval, source) -> fieldEval),
        Map.entry(BOOLEAN, ToIntegerFromBooleanEvaluator.Factory::new),
        Map.entry(DATETIME, ToIntegerFromLongEvaluator.Factory::new),
        Map.entry(KEYWORD, ToIntegerFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToIntegerFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToIntegerFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToIntegerFromUnsignedLongEvaluator.Factory::new),
        Map.entry(LONG, ToIntegerFromLongEvaluator.Factory::new),
        Map.entry(EsqlDataTypes.COUNTER_INTEGER, (fieldEval, source) -> fieldEval)
    );

    @FunctionInfo(
        returnType = "integer",
        description = """
            Converts an input value to an integer value.
            If the input parameter is of a date type, its value will be interpreted as milliseconds
            since the {wikipedia}/Unix_time[Unix epoch], converted to integer.
            Boolean *true* will be converted to integer *1*, *false* to *0*.""",
        examples = @Example(file = "ints", tag = "to_int-long", explanation = """
            Note that in this example, the last value of the multi-valued field cannot be converted as an integer.
            When this happens, the result is a *null* value. In this case a _Warning_ header is added to the response.
            The header will provide information on the source of the failure:

            `"Line 1:61: evaluation of [TO_INTEGER(long)] failed, treating result as null. Only first 20 failures recorded."`

            A following header will contain the failure reason and the offending value:

            `"org.elasticsearch.xpack.ql.InvalidArgumentException: [501379200000] out of [integer] range"`""")
    )
    public ToInteger(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "keyword", "text", "double", "long", "unsigned_long", "integer", "counter_integer" },
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
        return INTEGER;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToInteger(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToInteger::new, field());
    }

    @ConvertEvaluator(extraName = "FromBoolean")
    static int fromBoolean(boolean bool) {
        return bool ? 1 : 0;
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { InvalidArgumentException.class })
    static int fromKeyword(BytesRef in) {
        return stringToInt(in.utf8ToString());
    }

    @ConvertEvaluator(extraName = "FromDouble", warnExceptions = { InvalidArgumentException.class })
    static int fromDouble(double dbl) {
        return safeToInt(dbl);
    }

    @ConvertEvaluator(extraName = "FromUnsignedLong", warnExceptions = { InvalidArgumentException.class })
    static int fromUnsignedLong(long ul) {
        return unsignedLongToInt(ul);
    }

    @ConvertEvaluator(extraName = "FromLong", warnExceptions = { InvalidArgumentException.class })
    static int fromLong(long lng) {
        return safeToInt(lng);
    }
}
