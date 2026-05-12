/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * An anti join used to implement {@code WHERE field NOT IN (subquery)}.
 * <p>
 * Behaves identically to {@link SemiJoin} except it uses {@link JoinTypes#ANTI}.
 */
public class AntiJoin extends SemiJoin {

    public AntiJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config);
    }

    public AntiJoin(Source source, LogicalPlan left, LogicalPlan right, List<Attribute> leftFields, List<Attribute> rightFields) {
        super(source, left, right, JoinTypes.ANTI, leftFields, rightFields);
    }

    @Override
    protected NodeInfo<Join> info() {
        JoinConfig config = config();
        return NodeInfo.create(this, AntiJoin::new, left(), right(), config.leftFields(), config.rightFields());
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new AntiJoin(source(), left, right, config());
    }

    @Override
    public boolean isAntiJoin() {
        return true;
    }
}
