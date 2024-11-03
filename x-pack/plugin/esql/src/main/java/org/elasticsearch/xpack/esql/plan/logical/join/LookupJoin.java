/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;

/**
 * Lookup join - specialized LEFT (OUTER) JOIN between the main left side and a lookup index (index_mode = lookup) on the right.
 * In the future, as the join capabilities of the engine will evolve, a regular LEFT JOIN will be used instead, letting the planner decide on the
 * strategy at runtime.
 */
public class LookupJoin extends Join {

    public LookupJoin(StreamInput in) throws IOException {
        super(in);
    }

    public LookupJoin(Source source,
                      LogicalPlan left,
                      LogicalPlan right,
                      List<Attribute> joinFields) {
        super(source, left, right, new JoinConfig(JoinType.LEFT, joinFields, joinFields, joinFields));
    }
}
