/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import java.util.List;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Sum all values of a field in matching documents.
 */
public class Sum extends NumericAggregate implements EnclosedAgg {

    public Sum(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Sum> info() {
        return NodeInfo.create(this, Sum::new, field());
    }

    @Override
    public Sum replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new Sum(location(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public String innerName() {
        return "sum";
    }
}
