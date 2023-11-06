/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

public class ToDatetime extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DATETIME, (field, source) -> field),
        Map.entry(LONG, (field, source) -> field),
        Map.entry(KEYWORD, ToDatetimeFromStringEvaluator.Factory::new),
        Map.entry(DOUBLE, ToLongFromDoubleEvaluator.Factory::new),
        Map.entry(UNSIGNED_LONG, ToLongFromUnsignedLongEvaluator.Factory::new),
        Map.entry(INTEGER, ToLongFromIntEvaluator.Factory::new) // CastIntToLongEvaluator would be a candidate, but not MV'd
    );

    public ToDatetime(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return DATETIME;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDatetime(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDatetime::new, field());
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static long fromKeyword(BytesRef in) {
        return DateParse.process(in, DateParse.DEFAULT_FORMATTER);
    }
}
