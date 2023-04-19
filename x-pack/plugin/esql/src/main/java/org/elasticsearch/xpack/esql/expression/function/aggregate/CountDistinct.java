/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

@Experimental
public class CountDistinct extends AggregateFunction {

    public CountDistinct(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<CountDistinct> info() {
        return NodeInfo.create(this, CountDistinct::new, field());
    }

    @Override
    public CountDistinct replaceChildren(List<Expression> newChildren) {
        return new CountDistinct(source(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        return DataTypes.LONG;
    }
}
