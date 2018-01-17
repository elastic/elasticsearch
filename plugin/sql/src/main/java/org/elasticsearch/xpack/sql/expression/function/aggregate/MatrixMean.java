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

public class MatrixMean extends NumericAggregate implements MatrixStatsEnclosed {

    public MatrixMean(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<MatrixMean> info() {
        return NodeInfo.create(this, MatrixMean::new, field());
    }

    @Override
    public MatrixMean replaceChildren(List<Expression> newChildren) {
        return new MatrixMean(location(), newChildren.get(0));
    }

    @Override
    public String innerName() {
        return "matrix_mean";
    }
}
