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
 * Find the maximum value in matching documents.
 */
public class Max extends NumericAggregate implements EnclosedAgg {

    public Max(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Max> info() {
        return NodeInfo.create(this, Max::new, field());
    }

    @Override
    public Max replaceChildren(List<Expression> newChildren) {
        return new Max(location(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public String innerName() {
        return "max";
    }
}
