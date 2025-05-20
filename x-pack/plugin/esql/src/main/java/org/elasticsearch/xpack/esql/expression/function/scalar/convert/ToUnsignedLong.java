/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.booleanToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.doubleToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.intToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToUnsignedLong;

public class ToUnsignedLong extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToUnsignedLong",
        ToUnsignedLong::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(UNSIGNED_LONG, (source, fieldEval) -> fieldEval),
        Map.entry(DATETIME, ToUnsignedLongFromLongEvaluator.Factory::new),
        Map.entry(BOOLEAN, ToUnsignedLongFromBooleanEvaluator.Factory::new),
        Map.entry(KEYWORD, ToUnsignedLongFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToUnsignedLongFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToUnsignedLongFromDoubleEvaluator.Factory::new),
        Map.entry(LONG, ToUnsignedLongFromLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToUnsignedLongFromIntEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "unsigned_long",
        description = """
            Converts an input value to an unsigned long value. If the input parameter is of a date type,
            its value will be interpreted as milliseconds since the {wikipedia}/Unix_time[Unix epoch], converted to unsigned long.
            Boolean `true` will be converted to unsigned long `1`, `false` to `0`.""",
        examples = @Example(file = "ints", tag = "to_unsigned_long-str", explanation = """
            Note that in this example, the last conversion of the string isn’t possible.
            When this happens, the result is a `null` value. In this case a _Warning_ header is added to the response.
            The header will provide information on the source of the failure:

            `"Line 1:133: evaluation of [TO_UL(str3)] failed, treating result as null. Only first 20 failures recorded."`

            A following header will contain the failure reason and the offending value:

            `"java.lang.NumberFormatException: Character f is neither a decimal digit number, decimal point,
            + "nor \"e\" notation exponential mark."`""")
    )
    public ToUnsignedLong(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "keyword", "text", "double", "long", "unsigned_long", "integer" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToUnsignedLong(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return UNSIGNED_LONG;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToUnsignedLong(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToUnsignedLong::new, field());
    }

    @ConvertEvaluator(extraName = "FromBoolean")
    static long fromBoolean(boolean bool) {
        return booleanToUnsignedLong(bool);
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { InvalidArgumentException.class, NumberFormatException.class })
    static long fromKeyword(BytesRef in) {
        return stringToUnsignedLong(in.utf8ToString());
    }

    @ConvertEvaluator(extraName = "FromDouble", warnExceptions = { InvalidArgumentException.class })
    static long fromDouble(double dbl) {
        return doubleToUnsignedLong(dbl);
    }

    @ConvertEvaluator(extraName = "FromLong", warnExceptions = { InvalidArgumentException.class })
    static long fromLong(long lng) {
        return longToUnsignedLong(lng, false);
    }

    @ConvertEvaluator(extraName = "FromInt", warnExceptions = { InvalidArgumentException.class })
    static long fromInt(int i) {
        return intToUnsignedLong(i);
    }
}
