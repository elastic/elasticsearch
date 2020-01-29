/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

public class ScalarSubquery extends SubQueryExpression {

    public ScalarSubquery(Source source, LogicalPlan query) {
        this(source, query, null);
    }

    public ScalarSubquery(Source source, LogicalPlan query, NameId id) {
        super(source, query, id);
    }

    @Override
    protected NodeInfo<ScalarSubquery> info() {
        return NodeInfo.create(this, ScalarSubquery::new, query(), id());
    }

    @Override
    protected ScalarSubquery clone(LogicalPlan newQuery) {
        return new ScalarSubquery(source(), newQuery);
    }

    @Override
    public DataType dataType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }
}
