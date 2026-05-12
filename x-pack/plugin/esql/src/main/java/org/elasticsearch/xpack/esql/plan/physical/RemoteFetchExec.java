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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Fetches deferred fields on the coordinator from remote shard owners using a transport-safe handle.
 */
public class RemoteFetchExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "RemoteFetchExec",
        RemoteFetchExec::new
    );

    private final Attribute handleAttribute;
    private final List<Attribute> attributesToFetch;
    private final List<Attribute> fetchedOutputAttributes;
    private final PhysicalPlan pushdownPlan;
    private List<Attribute> lazyOutput;

    public RemoteFetchExec(
        Source source,
        PhysicalPlan child,
        Attribute handleAttribute,
        List<Attribute> attributesToFetch,
        List<Attribute> fetchedOutputAttributes,
        PhysicalPlan pushdownPlan
    ) {
        super(source, child);
        this.handleAttribute = handleAttribute;
        this.attributesToFetch = List.copyOf(attributesToFetch);
        this.fetchedOutputAttributes = List.copyOf(fetchedOutputAttributes);
        this.pushdownPlan = pushdownPlan;
    }

    private RemoteFetchExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readOptionalNamedWriteable(PhysicalPlan.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(handleAttribute);
        out.writeNamedWriteableCollection(attributesToFetch);
        out.writeNamedWriteableCollection(fetchedOutputAttributes);
        out.writeOptionalNamedWriteable(pushdownPlan);
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
            child(),
            handleAttribute,
            attributesToFetch,
            fetchedOutputAttributes,
            pushdownPlan
        );
    }

    @Override
    public RemoteFetchExec replaceChild(PhysicalPlan newChild) {
        return new RemoteFetchExec(source(), newChild, handleAttribute, attributesToFetch, fetchedOutputAttributes, pushdownPlan);
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(handleAttribute);
    }

    public Attribute handleAttribute() {
        return handleAttribute;
    }

    public List<Attribute> attributesToFetch() {
        return attributesToFetch;
    }

    public PhysicalPlan pushdownPlan() {
        return pushdownPlan;
    }

    public List<Attribute> fetchedOutputAttributes() {
        return fetchedOutputAttributes;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<Attribute> childOutput = child().output();
            List<Attribute> fetchedOutput = fetchedOutputAttributes();
            lazyOutput = new ArrayList<>(childOutput.size() + fetchedOutput.size());
            lazyOutput.addAll(childOutput);
            lazyOutput.addAll(fetchedOutput);
        }
        return lazyOutput;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(true, attributesToFetch);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), handleAttribute, attributesToFetch, fetchedOutputAttributes, pushdownPlan);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RemoteFetchExec other = (RemoteFetchExec) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(handleAttribute, other.handleAttribute)
            && Objects.equals(attributesToFetch, other.attributesToFetch)
            && Objects.equals(fetchedOutputAttributes, other.fetchedOutputAttributes)
            && Objects.equals(pushdownPlan, other.pushdownPlan);
    }
}
