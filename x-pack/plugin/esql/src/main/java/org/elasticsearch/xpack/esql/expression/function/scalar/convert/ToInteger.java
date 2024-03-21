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

import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeToInt;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsNumber;

public class ToInteger extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(INTEGER, (fieldEval, source) -> fieldEval),
        Map.entry(BOOLEAN, ToIntegerFromBooleanEvaluator.Factory::new),
        Map.entry(DATETIME, ToIntegerFromLongEvaluator.Factory::new),
        Map.entry(KEYWORD, ToIntegerFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToIntegerFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToIntegerFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToIntegerFromUnsignedLongEvaluator.Factory::new),
        Map.entry(LONG, ToIntegerFromLongEvaluator.Factory::new)
    );

    @FunctionInfo(returnType = "integer", description = "Converts an input value to an integer value.")
    public ToInteger(
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

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { InvalidArgumentException.class, NumberFormatException.class })
    static int fromKeyword(BytesRef in) {
        String asString = in.utf8ToString();
        try {
            return Integer.parseInt(asString);
        } catch (NumberFormatException nfe) {
            try {
                return fromDouble(Double.parseDouble(asString));
            } catch (Exception e) {
                throw nfe;
            }
        }
    }

    @ConvertEvaluator(extraName = "FromDouble", warnExceptions = { InvalidArgumentException.class })
    static int fromDouble(double dbl) {
        return safeToInt(dbl);
    }

    @ConvertEvaluator(extraName = "FromUnsignedLong", warnExceptions = { InvalidArgumentException.class })
    static int fromUnsignedLong(long ul) {
        Number n = unsignedLongAsNumber(ul);
        int i = n.intValue();
        if (i != n.longValue()) {
            throw new InvalidArgumentException("[{}] out of [integer] range", n);
        }
        return i;
    }

    @ConvertEvaluator(extraName = "FromLong", warnExceptions = { InvalidArgumentException.class })
    static int fromLong(long lng) {
        return safeToInt(lng);
    }
}
