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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToInt;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToInt;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToInt;

public class ToInteger extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToInteger",
        ToInteger::new
    );

    static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(INTEGER, (source, fieldEval) -> fieldEval),
        Map.entry(BOOLEAN, ToIntegerFromBooleanEvaluator.Factory::new),
        Map.entry(DATETIME, ToIntegerFromLongEvaluator.Factory::new),
        Map.entry(KEYWORD, ToIntegerFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToIntegerFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToIntegerFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToIntegerFromUnsignedLongEvaluator.Factory::new),
        Map.entry(LONG, ToIntegerFromLongEvaluator.Factory::new),
        Map.entry(COUNTER_INTEGER, (source, fieldEval) -> fieldEval)
    );

    public ToInteger(Source source, Expression field) {
        super(source, field);
    }

    private ToInteger(StreamInput in) throws IOException {
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
