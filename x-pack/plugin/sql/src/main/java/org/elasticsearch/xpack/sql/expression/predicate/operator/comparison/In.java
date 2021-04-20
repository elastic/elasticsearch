/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class In extends org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In {

    public In(Source source, Expression value, List<Expression> list) {
        super(source, value, list);
    }

    public In(Source source, Expression value, List<Expression> list, ZoneId zoneId) {
        super(source, value, list, zoneId);
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In> info() {
        return NodeInfo.create(this, In::new, value(), list(), zoneId());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new In(source(), newChildren.get(newChildren.size() - 1), newChildren.subList(0, newChildren.size() - 1), zoneId());
    }

    @Override
    protected List<Object> foldAndConvertListOfValues(List<Expression> list, DataType dataType) {
        List<Object> values = new ArrayList<>(list.size());
        for (Expression e : list) {
            values.add(SqlDataTypeConverter.convert(Foldables.valueOf(e), dataType));
        }
        return values;
    }

    @Override
    protected boolean areCompatible(DataType left, DataType right) {
        // "left" is the reference type that all other values in the "right" IN-set need to convert to
        return SqlDataTypeConverter.canConvert(right, left);
    }
}
