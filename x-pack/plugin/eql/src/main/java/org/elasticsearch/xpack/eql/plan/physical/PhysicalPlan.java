/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.plan.physical;

import org.elasticsearch.xpack.eql.session.Executable;
import org.elasticsearch.xpack.ql.plan.QueryPlan;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

/**
 * A PhysicalPlan is "how" a LogicalPlan (the "what") actually gets translated into one or more queries.
 *
 * LogicalPlan = I want to get from DEN to SFO
 * PhysicalPlan = take Delta, DEN to SJC, then SJC to SFO
 */
public abstract class PhysicalPlan extends QueryPlan<PhysicalPlan> implements Executable {

    public PhysicalPlan(Source source, List<PhysicalPlan> children) {
        super(source, children);
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);
}
