/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

public final class ReplaceLookupWithJoin extends OptimizerRules.OptimizerRule<Lookup> {

    public ReplaceLookupWithJoin() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Lookup lookup) {
        // left join between the main relation and the local, lookup relation
        return new Join(lookup.source(), lookup.child(), lookup.localRelation(), lookup.joinConfig());
    }
}
