/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Subquery extends UnaryPlan implements TelemetryAware, SortAgnostic {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Subquery", Subquery::new);
    private final String name;  // named subqueries are views (information useful for debugging planning)

    // subquery alias/qualifier could be added in the future if needed

    public Subquery(Source source, LogicalPlan subqueryPlan) {
        this(source, subqueryPlan, null);
    }

    public Subquery(Source source, LogicalPlan subqueryPlan, String name) {
        super(source, subqueryPlan);
        this.name = name;
    }

    private Subquery(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(LogicalPlan.class), null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        // View names are not needed on the data nodes, only on the coordinating node for debugging purposes
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Subquery> info() {
        return NodeInfo.create(this, Subquery::new, child(), name);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Subquery(source(), newChild, name);
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Subquery other = (Subquery) obj;
        return Objects.equals(child(), other.child());
    }

    @Override
    public String nodeString(NodeStringFormat format) {
        return nodeName() + "[" + (name == null ? "" : name) + "]";
    }

    public LogicalPlan plan() {
        return child();
    }
}
