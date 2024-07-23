/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.plan.QueryPlan;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

/**
 * A PhysicalPlan is "how" a LogicalPlan (the "what") actually gets translated into one or more queries.
 *
 * LogicalPlan = I want to get from DEN to SFO
 * PhysicalPlan = take Delta, DEN to SJC, then SJC to SFO
 */
public abstract class PhysicalPlan extends QueryPlan<PhysicalPlan> {

    public PhysicalPlan(Source source, List<PhysicalPlan> children) {
        super(source, children);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO remove when all PhysicalPlans are migrated to NamedWriteable
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

}
