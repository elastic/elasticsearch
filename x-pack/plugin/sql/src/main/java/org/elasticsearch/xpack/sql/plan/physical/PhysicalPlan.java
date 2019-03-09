/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import java.util.List;

import org.elasticsearch.xpack.sql.plan.QueryPlan;
import org.elasticsearch.xpack.sql.session.Executable;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.Schema;

/**
 * A PhysicalPlan is "how" a LogicalPlan (the "what") actually gets translated into one or more queries.
 *
 * LogicalPlan = I want to get from DEN to SFO
 * PhysicalPlan = take Delta, DEN to SJC, then SJC to SFO
 */
public abstract class PhysicalPlan extends QueryPlan<PhysicalPlan> implements Executable {

    private Schema lazySchema;

    public PhysicalPlan(Source source, List<PhysicalPlan> children) {
        super(source, children);
    }

    public Schema schema() {
        if (lazySchema == null) {
            lazySchema = Rows.schema(output());
        }
        return lazySchema;
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);
}
