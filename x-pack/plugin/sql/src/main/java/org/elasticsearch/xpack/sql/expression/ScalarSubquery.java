/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

public class ScalarSubquery extends SubQueryExpression {

    public ScalarSubquery(Location location, LogicalPlan query) {
        this(location, query, null);
    }

    public ScalarSubquery(Location location, LogicalPlan query, ExpressionId id) {
        super(location, query, id);
    }

    @Override
    protected NodeInfo<ScalarSubquery> info() {
        return NodeInfo.create(this, ScalarSubquery::new, query(), id());
    }

    @Override
    protected ScalarSubquery clone(LogicalPlan newQuery) {
        return new ScalarSubquery(location(), newQuery);
    }

    @Override
    public DataType dataType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nullable() {
        return true;
    }
}
