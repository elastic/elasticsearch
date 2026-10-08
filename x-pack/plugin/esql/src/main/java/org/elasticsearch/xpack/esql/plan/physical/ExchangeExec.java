/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
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

    /**
     * Readers of this version know {@link Scope}. Shared with {@code DataType.DOC_REF} because both only appear in the
     * plans of the fetch phase.
     */
    private static final TransportVersion ESQL_FETCH_PHASE_PLAN = TransportVersion.fromName("esql_fetch_phase_plan");

    /**
     * Which boundary between drivers an exchange stands for.
     */
    public enum Scope {
        /**
         * Between the coordinator and the data nodes. The coordinator splits the plan at the first exchange of this
         * scope and rows cross the network, so they can never carry {@code _doc}.
         */
        CLUSTER,
        /**
         * Between the data drivers and the node reduce driver of one data node. The coordinator plans the node reduce
         * stage as part of the physical plan, and the data node splits it off without planning anything. Rows cross a
         * local exchange inside one node, so they may carry {@code _doc}. Always below a {@link #CLUSTER} exchange.
         */
        NODE
    }

    private final List<Attribute> output;
    private final boolean inBetweenAggs;
    private final Scope scope;

    public ExchangeExec(Source source, PhysicalPlan child) {
        this(source, emptyList(), false, child);
    }

    public ExchangeExec(Source source, List<Attribute> output, boolean inBetweenAggs, PhysicalPlan child) {
        this(source, output, inBetweenAggs, Scope.CLUSTER, child);
    }

    public ExchangeExec(Source source, List<Attribute> output, boolean inBetweenAggs, Scope scope, PhysicalPlan child) {
        super(source, child);
        this.output = output;
        this.inBetweenAggs = inBetweenAggs;
        this.scope = scope;
    }

    private ExchangeExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readBoolean(),
            in.getTransportVersion().supports(ESQL_FETCH_PHASE_PLAN) ? in.readEnum(Scope.class) : Scope.CLUSTER,
            in.readNamedWriteable(PhysicalPlan.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteableCollection(output);
        out.writeBoolean(inBetweenAggs());
        if (out.getTransportVersion().supports(ESQL_FETCH_PHASE_PLAN)) {
            out.writeEnum(scope);
        } else if (scope != Scope.CLUSTER) {
            // the planner checks the minimum version of the cluster before it plans a node scope, so this is a bug
            throw new IllegalStateException(
                "remote node at version [" + out.getTransportVersion() + "] doesn't understand " + scope + " exchanges"
            );
        }
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

    public Scope scope() {
        return scope;
    }

    @Override
    protected AttributeSet computeReferences() {
        // ExchangeExec does no input referencing, it only outputs all synthetic attributes, "sourced" from remote exchanges.
        return AttributeSet.EMPTY;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new ExchangeExec(source(), output, inBetweenAggs, scope, newChild);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ExchangeExec::new, output, inBetweenAggs, scope, child());
    }

    @Override
    public List<Object> nodeProperties() {
        // every exchange that predates the node scope is a cluster exchange, so plans without a node stage print as before
        return scope == Scope.CLUSTER ? List.of(output, inBetweenAggs, child()) : super.nodeProperties();
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        ExchangeExec other = (ExchangeExec) obj;
        return output.equals(other.output) && inBetweenAggs == other.inBetweenAggs && scope == other.scope;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), output, inBetweenAggs, scope);
    }
}
