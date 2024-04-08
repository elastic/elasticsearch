/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.booleanToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.doubleToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.intToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToUnsignedLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToUnsignedLong;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

public class ToUnsignedLong extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(UNSIGNED_LONG, (fieldEval, source) -> fieldEval),
        Map.entry(DATETIME, ToUnsignedLongFromLongEvaluator.Factory::new),
        Map.entry(BOOLEAN, ToUnsignedLongFromBooleanEvaluator.Factory::new),
        Map.entry(KEYWORD, ToUnsignedLongFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToUnsignedLongFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToUnsignedLongFromDoubleEvaluator.Factory::new),
        Map.entry(LONG, ToUnsignedLongFromLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToUnsignedLongFromIntEvaluator.Factory::new)
    );

    @FunctionInfo(returnType = "unsigned_long", description = "Converts an input value to an unsigned long value.")
    public ToUnsignedLong(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "keyword", "text", "double", "long", "unsigned_long", "integer" }
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
