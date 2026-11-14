/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedReduction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

/** A row-preserving reduction boundary: child object rows never become pipeline rows. */
public final class NestedReduceExec extends EvalExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "NestedReduceExec",
        NestedReduceExec::new
    );

    public NestedReduceExec(Source source, PhysicalPlan child, List<Alias> fields) {
        super(source, child, fields);
    }

    private NestedReduceExec(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(PhysicalPlan.class), in.readCollectionAsList(Alias::new));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        NestedReduction.checkVersion(out);
        super.writeTo(out);
    }

    @Override
    public NestedReduceExec replaceChild(PhysicalPlan child) {
        return new NestedReduceExec(source(), child, fields());
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, NestedReduceExec::new, child(), fields());
    }
}
