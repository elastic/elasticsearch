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
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToDouble;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToDouble;

public class ToDouble extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToDouble", ToDouble::new);

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DOUBLE, (source, fieldEval) -> fieldEval),
        Map.entry(BOOLEAN, ToDoubleFromBooleanEvaluator.Factory::new),
        Map.entry(DATETIME, ToDoubleFromLongEvaluator.Factory::new), // CastLongToDoubleEvaluator would be a candidate, but not MV'd
        Map.entry(KEYWORD, ToDoubleFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToDoubleFromStringEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToDoubleFromUnsignedLongEvaluator.Factory::new),
        Map.entry(LONG, ToDoubleFromLongEvaluator.Factory::new), // CastLongToDoubleEvaluator would be a candidate, but not MV'd
        Map.entry(INTEGER, ToDoubleFromIntEvaluator.Factory::new), // CastIntToDoubleEvaluator would be a candidate, but not MV'd
        Map.entry(DataType.COUNTER_DOUBLE, (source, field) -> field),
        Map.entry(DataType.COUNTER_INTEGER, ToDoubleFromIntEvaluator.Factory::new),
        Map.entry(DataType.COUNTER_LONG, ToDoubleFromLongEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "double",
        description = """
            Converts an input value to a double value. If the input parameter is of a date type,
            its value will be interpreted as milliseconds since the {wikipedia}/Unix_time[Unix epoch],
            converted to double. Boolean *true* will be converted to double *1.0*, *false* to *0.0*.""",
        examples = @Example(file = "floats", tag = "to_double-str", explanation = """
            Note that in this example, the last conversion of the string isn't possible.
            When this happens, the result is a *null* value. In this case a _Warning_ header is added to the response.
            The header will provide information on the source of the failure:

            `"Line 1:115: evaluation of [TO_DOUBLE(str2)] failed, treating result as null. Only first 20 failures recorded."`

            A following header will contain the failure reason and the offending value:
            `"java.lang.NumberFormatException: For input string: \"foo\""`""")
    )
    public ToDouble(
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
                "counter_double",
                "counter_integer",
                "counter_long" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToDouble(StreamInput in) throws IOException {
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
        return DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDouble(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDouble::new, field());
    }

    @ConvertEvaluator(extraName = "FromBoolean")
    static double fromBoolean(boolean bool) {
        return bool ? 1d : 0d;
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { InvalidArgumentException.class })
    static double fromKeyword(BytesRef in) {
        return stringToDouble(in.utf8ToString());
    }

    @ConvertEvaluator(extraName = "FromUnsignedLong")
    static double fromUnsignedLong(long l) {
        return unsignedLongToDouble(l);
    }

    @ConvertEvaluator(extraName = "FromLong")
    static double fromLong(long l) {
        return l;
    }

    @ConvertEvaluator(extraName = "FromInt")
    static double fromInt(int i) {
        return i;
    }
}
