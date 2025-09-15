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
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToBoolean;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToBoolean;

public class ToBoolean extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToBoolean",
        ToBoolean::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(BOOLEAN, (source, field) -> field),
        Map.entry(KEYWORD, ToBooleanFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToBooleanFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToBooleanFromDoubleEvaluator.Factory::new),
        Map.entry(LONG, ToBooleanFromLongEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToBooleanFromUnsignedLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToBooleanFromIntEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "boolean",
        description = """
            Converts an input value to a boolean value.
            A string value of *true* will be case-insensitive converted to the Boolean *true*.
            For anything else, including the empty string, the function will return *false*.
            The numerical value of *0* will be converted to *false*, anything else will be converted to *true*.""",
        examples = @Example(file = "boolean", tag = "to_boolean")
    )
    public ToBoolean(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "keyword", "text", "double", "long", "unsigned_long", "integer" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private ToBoolean(StreamInput in) throws IOException {
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
        return BOOLEAN;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToBoolean(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToBoolean::new, field());
    }

    @ConvertEvaluator(extraName = "FromString")
    static boolean fromKeyword(BytesRef keyword) {
        return stringToBoolean(keyword.utf8ToString());
    }

    @ConvertEvaluator(extraName = "FromDouble")
    static boolean fromDouble(double d) {
        return d != 0;
    }

    @ConvertEvaluator(extraName = "FromLong")
    static boolean fromLong(long l) {
        return l != 0;
    }

    @ConvertEvaluator(extraName = "FromUnsignedLong")
    static boolean fromUnsignedLong(long ul) {
        return unsignedLongToBoolean(ul);
    }

    @ConvertEvaluator(extraName = "FromInt")
    static boolean fromInt(int i) {
        return fromLong(i);
    }
}
