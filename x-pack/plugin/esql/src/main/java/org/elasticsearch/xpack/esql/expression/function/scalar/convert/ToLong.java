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
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeDoubleToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToLong;

public class ToLong extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToLong", ToLong::new);

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(LONG, (source, fieldEval) -> fieldEval),
        Map.entry(DATETIME, (source, fieldEval) -> fieldEval),
        Map.entry(DATE_NANOS, (source, fieldEval) -> fieldEval),
        Map.entry(BOOLEAN, ToLongFromBooleanEvaluator.Factory::new),
        Map.entry(KEYWORD, ToLongFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToLongFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToLongFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToLongFromUnsignedLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToLongFromIntEvaluator.Factory::new), // CastIntToLongEvaluator would be a candidate, but not MV'd
        Map.entry(DataType.COUNTER_LONG, (source, field) -> field),
        Map.entry(DataType.COUNTER_INTEGER, ToLongFromIntEvaluator.Factory::new)
    );

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
    public ToLong(
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
                "counter_long" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToLong(StreamInput in) throws IOException {
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
        throw new UnsupportedOperationException("SAFDSAF");
//        return safeDoubleToLong(dbl);
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
