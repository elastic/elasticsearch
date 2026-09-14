/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Coordinator-only boundary that fans several physical sources into one
 * data-to-coordinator exchange.
 */
public class SourceFanInExec extends PhysicalPlan {

    private final List<Attribute> output;
    private final boolean inBetweenAggs;

    /**
     * Creates a planning boundary whose producers feed a shared coordinator exchange.
     */
    public SourceFanInExec(Source source, List<PhysicalPlan> producers, List<Attribute> output, boolean inBetweenAggs) {
        super(source, producers);
        this.output = output;
        this.inBetweenAggs = inBetweenAggs;
    }

    /**
     * Returns the independently distributable source fragments.
     */
    public List<PhysicalPlan> producers() {
        return children();
    }

    /**
     * Returns whether producers emit intermediate aggregation state for final reduction.
     */
    public boolean inBetweenAggs() {
        return inBetweenAggs;
    }

    /**
     * Rebuilds this planning boundary while retaining its coordinator role.
     */
    public SourceFanInExec withProducers(List<PhysicalPlan> producers, List<Attribute> output, boolean inBetweenAggs) {
        return new SourceFanInExec(source(), producers, output, inBetweenAggs);
    }

    @Override
    public PhysicalPlan replaceChildren(List<PhysicalPlan> newChildren) {
        return new SourceFanInExec(source(), newChildren, output, inBetweenAggs);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, SourceFanInExec::new, producers(), output, inBetweenAggs);
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SourceFanInExec other = (SourceFanInExec) obj;
        return inBetweenAggs == other.inBetweenAggs && output.equals(other.output) && producers().equals(other.producers());
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, inBetweenAggs, producers());
    }
}
