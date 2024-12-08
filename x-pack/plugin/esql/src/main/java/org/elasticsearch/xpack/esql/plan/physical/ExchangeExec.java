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

import static java.util.Collections.emptyList;

public class ExchangeExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "ExchangeExec",
        ExchangeExec::new
    );

    private final List<Attribute> output;
    private final boolean inBetweenAggs;

    public ExchangeExec(Source source, PhysicalPlan child) {
        this(source, emptyList(), false, child);
    }

    public ExchangeExec(Source source, List<Attribute> output, boolean inBetweenAggs, PhysicalPlan child) {
        super(source, child);
        this.output = output;
        this.inBetweenAggs = inBetweenAggs;
    }

    private ExchangeExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readBoolean(),
            in.readNamedWriteable(PhysicalPlan.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(output);
        out.writeBoolean(inBetweenAggs());
        out.writeNamedWriteable(child());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public List<Attribute> output() {
        return output.isEmpty() ? super.output() : output;
    }

    public boolean inBetweenAggs() {
        return inBetweenAggs;
    }

    @Override
    protected AttributeSet computeReferences() {
        // ExchangeExec does no input referencing, it only outputs all synthetic attributes, "sourced" from remote exchanges.
        return AttributeSet.EMPTY;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new ExchangeExec(source(), output, inBetweenAggs, newChild);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ExchangeExec::new, output, inBetweenAggs, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        ExchangeExec other = (ExchangeExec) obj;
        return output.equals(other.output) && inBetweenAggs == other.inBetweenAggs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), output, inBetweenAggs);
    }
}
