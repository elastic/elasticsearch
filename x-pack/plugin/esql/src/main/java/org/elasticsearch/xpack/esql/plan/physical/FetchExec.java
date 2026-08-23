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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.FetchSource;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.planner.FieldExtractionSpec;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Fetches deferred fields on the coordinator from remote shard owners using a transport-safe handle.
 * <p>
 * Fetch keeps two attribute lists because they represent different contracts:
 * <ul>
 *     <li>{@code attributesToFetch}: remote request schema (what the data node must load to execute fetch/pushdown)</li>
 *     <li>{@code fetchedOutputAttributes}: coordinator output schema (what this node appends to its child output)</li>
 * </ul>
 * <p>
 * The right-hand side of this {@link BinaryExec} is a {@link FragmentExec} that carries a constrained
 * {@link FetchSource}/{@link Eval}/{@link Filter}/{@link Project} logical plan. This follows the same
 * architectural pattern as lookup planning: logical plans are serialized and shipped, while physical planning remains
 * local to the target node.
 */
public class FetchExec extends BinaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "FetchExec",
        FetchExec::new
    );

    private final Attribute handleAttribute;
    /**
     * Attributes requested from remote shard owners. This list drives fetch request construction.
     */
    private final List<Attribute> attributesToFetch;
    /**
     * Complete extraction semantics for {@link #attributesToFetch}, in the same order.
     */
    private final List<FieldExtractionSpec> extractionSpecs;
    /**
     * Attributes appended to this node's output on the coordinator.
     */
    private final List<Attribute> fetchedOutputAttributes;
    private final PhysicalPlan fetchPlan;
    private List<Attribute> lazyOutput;

    public FetchExec(
        Source source,
        PhysicalPlan child,
        Attribute handleAttribute,
        List<Attribute> attributesToFetch,
        List<FieldExtractionSpec> extractionSpecs,
        List<Attribute> fetchedOutputAttributes,
        PhysicalPlan fetchPlan
    ) {
        super(source, child, fetchPlan);
        this.fetchPlan = requireFetchPlan(fetchPlan);
        this.handleAttribute = handleAttribute;
        this.attributesToFetch = List.copyOf(attributesToFetch);
        this.extractionSpecs = List.copyOf(extractionSpecs);
        this.fetchedOutputAttributes = List.copyOf(fetchedOutputAttributes);
        validateExtractionSpecs();
    }

    private void validateExtractionSpecs() {
        if (this.attributesToFetch.size() != this.extractionSpecs.size()) {
            throw new IllegalArgumentException(
                "fetch attributes ["
                    + this.attributesToFetch.size()
                    + "] must match extraction specifications ["
                    + this.extractionSpecs.size()
                    + "]"
            );
        }
        for (int i = 0; i < this.attributesToFetch.size(); i++) {
            if (this.attributesToFetch.get(i).dataType() != this.extractionSpecs.get(i).dataType()) {
                throw new IllegalArgumentException(
                    "fetch attribute ["
                        + this.attributesToFetch.get(i)
                        + "] has type ["
                        + this.attributesToFetch.get(i).dataType().typeName()
                        + "] but extraction specification has type ["
                        + this.extractionSpecs.get(i).dataType().typeName()
                        + "]"
                );
            }
        }
    }

    private FetchExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(PhysicalPlan.class), in.readNamedWriteable(PhysicalPlan.class));
        this.fetchPlan = requireFetchPlan(right());
        this.handleAttribute = in.readNamedWriteable(Attribute.class);
        this.attributesToFetch = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.extractionSpecs = in.readCollectionAsList(FieldExtractionSpec::new);
        this.fetchedOutputAttributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        validateExtractionSpecs();
    }

    private static FragmentExec requireFetchPlan(PhysicalPlan plan) {
        if ((plan instanceof FragmentExec) == false) {
            throw new IllegalArgumentException("fetch plan must be a FragmentExec");
        }
        FragmentExec fragmentExec = (FragmentExec) plan;
        LogicalPlan fragment = fragmentExec.fragment();
        if (fragment.anyMatch(FetchSource.class::isInstance) == false) {
            throw new IllegalArgumentException("fetch plan must contain FetchSource");
        }
        fragment.forEachDown(node -> {
            if (node instanceof FetchSource == false
                && node instanceof Eval == false
                && node instanceof Filter == false
                && node instanceof Project == false) {
                throw new IllegalArgumentException("unsupported fetch pushdown plan [" + node.nodeName() + "]");
            }
        });
        return fragmentExec;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(handleAttribute);
        out.writeNamedWriteableCollection(attributesToFetch);
        out.writeCollection(extractionSpecs);
        out.writeNamedWriteableCollection(fetchedOutputAttributes);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<FetchExec> info() {
        return NodeInfo.create(
            this,
            FetchExec::new,
            left(),
            handleAttribute,
            attributesToFetch,
            extractionSpecs,
            fetchedOutputAttributes,
            fetchPlan
        );
    }

    @Override
    public FetchExec replaceChildren(PhysicalPlan newLeft, PhysicalPlan newRight) {
        return new FetchExec(source(), newLeft, handleAttribute, attributesToFetch, extractionSpecs, fetchedOutputAttributes, newRight);
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
    public FetchExec replaceChild(PhysicalPlan newChild) {
        return new FetchExec(source(), newChild, handleAttribute, attributesToFetch, extractionSpecs, fetchedOutputAttributes, fetchPlan);
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

    /** Complete extraction specifications, in the same order as {@link #attributesToFetch()}. */
    public List<FieldExtractionSpec> extractionSpecs() {
        return extractionSpecs;
    }

    /**
     * The plan the fetch target must execute in addition to loading the fetched fields, or {@code null} when the fetch
     * plan is a bare {@link FetchSource} that only describes the fields to fetch.
     */
    @Nullable
    public PhysicalPlan pushdownPlan() {
        FragmentExec fragmentExec = fetchPlan();
        return fragmentExec.fragment() instanceof FetchSource ? null : fragmentExec;
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
        state.add(false, fetchedOutputAttributes);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), handleAttribute, attributesToFetch, extractionSpecs, fetchedOutputAttributes);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        FetchExec other = (FetchExec) obj;
        return Objects.equals(handleAttribute, other.handleAttribute)
            && Objects.equals(attributesToFetch, other.attributesToFetch)
            && Objects.equals(extractionSpecs, other.extractionSpecs)
            && Objects.equals(fetchedOutputAttributes, other.fetchedOutputAttributes);
    }
}
