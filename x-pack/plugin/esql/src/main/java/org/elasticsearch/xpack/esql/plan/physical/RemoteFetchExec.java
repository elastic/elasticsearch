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
import org.elasticsearch.xpack.esql.plan.logical.RemoteFetchSource;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Fetches deferred fields on the coordinator from remote shard owners using a transport-safe handle.
 * <p>
 * Remote fetch keeps two attribute lists because they represent different contracts:
 * <ul>
 *     <li>{@code attributesToFetch}: remote request schema (what the data node must load to execute fetch/pushdown)</li>
 *     <li>{@code fetchedOutputAttributes}: coordinator output schema (what this node appends to its child output)</li>
 * </ul>
 * <p>
 * The right-hand side of this {@link BinaryExec} is a {@link FragmentExec} that carries a {@link RemoteFetchSource}
 * logical plan. This follows the same architectural pattern as lookup planning: logical plans are serialized and
 * shipped, while physical planning remains local to the target node.
 */
public class RemoteFetchExec extends BinaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "RemoteFetchExec",
        RemoteFetchExec::new
    );

    private final Attribute handleAttribute;
    /**
     * Attributes requested from remote shard owners. This list drives fetch request construction.
     */
    private final List<Attribute> attributesToFetch;
    /**
     * Attributes appended to this node's output on the coordinator.
     */
    private final List<Attribute> fetchedOutputAttributes;
    private final PhysicalPlan fetchPlan;
    private List<Attribute> lazyOutput;

    public RemoteFetchExec(
        Source source,
        PhysicalPlan child,
        Attribute handleAttribute,
        List<Attribute> attributesToFetch,
        List<Attribute> fetchedOutputAttributes,
        PhysicalPlan fetchPlan
    ) {
        super(source, child, fetchPlan);
        this.fetchPlan = requireFetchPlan(fetchPlan);
        this.handleAttribute = handleAttribute;
        this.attributesToFetch = List.copyOf(attributesToFetch);
        this.fetchedOutputAttributes = List.copyOf(fetchedOutputAttributes);
    }

    private RemoteFetchExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(PhysicalPlan.class), in.readNamedWriteable(PhysicalPlan.class));
        this.fetchPlan = requireFetchPlan(right());
        this.handleAttribute = in.readNamedWriteable(Attribute.class);
        this.attributesToFetch = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.fetchedOutputAttributes = in.readNamedWriteableCollectionAsList(Attribute.class);
    }

    private static FragmentExec requireFetchPlan(PhysicalPlan plan) {
        if (plan instanceof FragmentExec fragmentExec && fragmentExec.fragment() instanceof RemoteFetchSource) {
            return fragmentExec;
        }
        throw new IllegalArgumentException("remote fetch plan must be a FragmentExec containing RemoteFetchSource");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(handleAttribute);
        out.writeNamedWriteableCollection(attributesToFetch);
        out.writeNamedWriteableCollection(fetchedOutputAttributes);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<RemoteFetchExec> info() {
        return NodeInfo.create(
            this,
            RemoteFetchExec::new,
            left(),
            handleAttribute,
            attributesToFetch,
            fetchedOutputAttributes,
            fetchPlan
        );
    }

    @Override
    public RemoteFetchExec replaceChildren(PhysicalPlan newLeft, PhysicalPlan newRight) {
        return new RemoteFetchExec(source(), newLeft, handleAttribute, attributesToFetch, fetchedOutputAttributes, newRight);
    }

    /**
     * Compatibility helper while this class migrates from {@link UnaryExec} to {@link BinaryExec}.
     */
    public PhysicalPlan child() {
        return left();
    }

    /**
     * Compatibility helper while this class migrates from {@link UnaryExec} to {@link BinaryExec}.
     */
    public RemoteFetchExec replaceChild(PhysicalPlan newChild) {
        return new RemoteFetchExec(source(), newChild, handleAttribute, attributesToFetch, fetchedOutputAttributes, fetchPlan);
    }

    @Override
    protected AttributeSet computeReferences() {
        return leftReferences();
    }

    @Override
    public AttributeSet inputSet() {
        return left().outputSet();
    }

    @Override
    public AttributeSet leftReferences() {
        return AttributeSet.of(handleAttribute);
    }

    @Override
    public AttributeSet rightReferences() {
        return AttributeSet.EMPTY;
    }

    public Attribute handleAttribute() {
        return handleAttribute;
    }

    public List<Attribute> attributesToFetch() {
        return attributesToFetch;
    }

    public PhysicalPlan pushdownPlan() {
        return fetchPlan;
    }

    public FragmentExec fetchPlan() {
        return (FragmentExec) fetchPlan;
    }

    public List<Attribute> fetchedOutputAttributes() {
        return fetchedOutputAttributes;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(fetchedOutputAttributes, left().output());
        }
        return lazyOutput;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(true, fetchedOutputAttributes);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), handleAttribute, attributesToFetch, fetchedOutputAttributes);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        RemoteFetchExec other = (RemoteFetchExec) obj;
        return Objects.equals(handleAttribute, other.handleAttribute)
            && Objects.equals(attributesToFetch, other.attributesToFetch)
            && Objects.equals(fetchedOutputAttributes, other.fetchedOutputAttributes);
    }
}
