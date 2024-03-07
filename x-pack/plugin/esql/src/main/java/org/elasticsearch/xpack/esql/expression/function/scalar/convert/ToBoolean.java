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
import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.tree.NodeInfo;
import org.elasticsearch.xpack.qlcore.tree.Source;
import org.elasticsearch.xpack.qlcore.type.DataType;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.qlcore.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.LONG;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.qlcore.util.NumericUtils.unsignedLongAsNumber;

public class ToBoolean extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(BOOLEAN, (field, source) -> field),
        Map.entry(KEYWORD, ToBooleanFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToBooleanFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToBooleanFromDoubleEvaluator.Factory::new),
        Map.entry(LONG, ToBooleanFromLongEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToBooleanFromUnsignedLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToBooleanFromIntEvaluator.Factory::new)
    );

    @FunctionInfo(returnType = "boolean", description = "Converts an input value to a boolean value.")
    public ToBoolean(
        Source source,
        @Param(name = "v", type = { "boolean", "keyword", "text", "double", "long", "unsigned_long", "integer" }) Expression field
    ) {
        super(source, field);
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
        return Boolean.parseBoolean(keyword.utf8ToString());
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
        Number n = unsignedLongAsNumber(ul);
        return n instanceof BigInteger || n.longValue() != 0;
    }

    @ConvertEvaluator(extraName = "FromInt")
    static boolean fromInt(int i) {
        return fromLong(i);
    }
}
