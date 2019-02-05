/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

public class Exists extends SubQueryExpression {

    public Exists(Source source, LogicalPlan query) {
        this(source, query, null);
    }

    public Exists(Source source, LogicalPlan query, ExpressionId id) {
        super(source, query, id);
    }

    @Override
    protected NodeInfo<Exists> info() {
        return NodeInfo.create(this, Exists::new, query(), id());
    }

    @Override
    protected SubQueryExpression clone(LogicalPlan newQuery) {
        return new Exists(source(), newQuery);
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }
}
