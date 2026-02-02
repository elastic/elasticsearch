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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node for METRICS_INFO command.
 */
public class MetricsInfoExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "MetricsInfoExec",
        MetricsInfoExec::new
    );

    private final List<Attribute> outputAttrs;

    public MetricsInfoExec(Source source, PhysicalPlan child, List<Attribute> output) {
        super(source, child);
        this.outputAttrs = output;
    }

    private MetricsInfoExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(outputAttrs);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<MetricsInfoExec> info() {
        return NodeInfo.create(this, MetricsInfoExec::new, child(), outputAttrs);
    }

    @Override
    public MetricsInfoExec replaceChild(PhysicalPlan newChild) {
        return new MetricsInfoExec(source(), newChild, outputAttrs);
    }

    @Override
    public List<Attribute> output() {
        return outputAttrs;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), outputAttrs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MetricsInfoExec other = (MetricsInfoExec) obj;
        return Objects.equals(child(), other.child()) && Objects.equals(outputAttrs, other.outputAttrs);
    }
}
