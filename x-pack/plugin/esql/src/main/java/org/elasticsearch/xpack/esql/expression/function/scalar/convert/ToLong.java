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
import org.elasticsearch.xpack.qlcore.InvalidArgumentException;
import org.elasticsearch.xpack.qlcore.expression.Expression;
import org.elasticsearch.xpack.qlcore.tree.NodeInfo;
import org.elasticsearch.xpack.qlcore.tree.Source;
import org.elasticsearch.xpack.qlcore.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.qlcore.type.DataTypeConverter.safeDoubleToLong;
import static org.elasticsearch.xpack.qlcore.type.DataTypeConverter.safeToLong;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.LONG;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.qlcore.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.qlcore.util.NumericUtils.unsignedLongAsNumber;

public class ToLong extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(LONG, (fieldEval, source) -> fieldEval),
        Map.entry(DATETIME, (fieldEval, source) -> fieldEval),
        Map.entry(BOOLEAN, ToLongFromBooleanEvaluator.Factory::new),
        Map.entry(KEYWORD, ToLongFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToLongFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToLongFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToLongFromUnsignedLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToLongFromIntEvaluator.Factory::new) // CastIntToLongEvaluator would be a candidate, but not MV'd
    );

    @FunctionInfo(returnType = "long", description = "Converts an input value to a long value.")
    public ToLong(
        Source source,
        @Param(name = "v", type = { "boolean", "date", "keyword", "text", "double", "long", "unsigned_long", "integer" }) Expression field
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

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { NumberFormatException.class })
    static long fromKeyword(BytesRef in) {
        String asString = in.utf8ToString();
        try {
            return Long.parseLong(asString);
        } catch (NumberFormatException nfe) {
            try {
                return fromDouble(Double.parseDouble(asString));
            } catch (Exception e) {
                throw nfe;
            }
        }
    }

    @ConvertEvaluator(extraName = "FromDouble", warnExceptions = { InvalidArgumentException.class })
    static long fromDouble(double dbl) {
        return safeDoubleToLong(dbl);
    }

    @ConvertEvaluator(extraName = "FromUnsignedLong", warnExceptions = { InvalidArgumentException.class })
    static long fromUnsignedLong(long ul) {
        return safeToLong(unsignedLongAsNumber(ul));
    }

    @ConvertEvaluator(extraName = "FromInt")
    static long fromInt(int i) {
        return i;
    }
}
