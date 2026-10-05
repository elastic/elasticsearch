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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Loads the deferred columns of the rows that survived a cut from the nodes that own their documents.
 * <p>
 * The left child produces the rows: the coordinator half of the cut, reading the cluster exchange, with a
 * {@link DataType#DOC_REF} column next to the columns the cut needed. The right child is the fetch plan,
 * {@code ProjectExec(FieldExtractExec(FetchSourceExec))}, a physical plan the coordinator builds and every owning node
 * runs as it is for the documents it is asked about. Nothing is planned on those nodes. The right child never runs on
 * the coordinator.
 * <p>
 * The output is the left output followed by the fetched columns. No columns are merged by name: the fetched columns
 * and the left columns come from disjoint sets.
 */
public final class FetchExec extends BinaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "FetchExec",
        FetchExec::new
    );

    private final ReferenceAttribute docRef;
    private final List<Attribute> fetchedAttributes;
    private final int stage;
    private final String indexPattern;
    private final List<String> originalIndices;
    private final Integer estimatedRowSize;
    private List<Attribute> lazyOutput;

    /**
     * @param left              the rows to fetch columns for
     * @param fetchPlan         the plan each owning node runs, its output is {@code fetchedAttributes}
     * @param docRef            the {@link DataType#DOC_REF} column of {@code left}
     * @param fetchedAttributes the columns the fetch appends to each row
     * @param stage             1 for the first fetch above the source. A query fetches in several stages when the
     *                          columns are first needed after different cuts.
     * @param indexPattern      the index pattern of the relation the documents come from
     * @param originalIndices   the index expressions the user wrote for the local cluster. A fetch request carries
     *                          them so it is authorized like the query that selected the documents.
     */
    public FetchExec(
        Source source,
        PhysicalPlan left,
        PhysicalPlan fetchPlan,
        ReferenceAttribute docRef,
        List<Attribute> fetchedAttributes,
        int stage,
        String indexPattern,
        List<String> originalIndices,
        Integer estimatedRowSize
    ) {
        super(source, left, fetchPlan);
        this.docRef = docRef;
        this.fetchedAttributes = fetchedAttributes;
        this.stage = stage;
        this.indexPattern = indexPattern;
        this.originalIndices = originalIndices;
        this.estimatedRowSize = estimatedRowSize;
    }

    private FetchExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(PhysicalPlan.class),
            (ReferenceAttribute) in.readNamedWriteable(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readVInt(),
            in.readString(),
            in.readStringCollectionAsList(),
            in.readOptionalVInt()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(docRef);
        out.writeNamedWriteableCollection(fetchedAttributes);
        out.writeVInt(stage);
        out.writeString(indexPattern);
        out.writeStringCollection(originalIndices);
        out.writeOptionalVInt(estimatedRowSize);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    /** The plan each owning node runs for the documents of the rows. */
    public PhysicalPlan fetchPlan() {
        return right();
    }

    public ReferenceAttribute docRef() {
        return docRef;
    }

    public List<Attribute> fetchedAttributes() {
        return fetchedAttributes;
    }

    public int stage() {
        return stage;
    }

    public String indexPattern() {
        return indexPattern;
    }

    public List<String> originalIndices() {
        return originalIndices;
    }

    /**
     * Estimate of the bytes per row of the pages this node emits, set by {@link EstimatesRowSize}.
     */
    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<Attribute> leftOutput = left().output();
            List<Attribute> output = new ArrayList<>(leftOutput.size() + fetchedAttributes.size());
            output.addAll(leftOutput);
            output.addAll(fetchedAttributes);
            lazyOutput = output;
        }
        return lazyOutput;
    }

    /** Only the left side feeds this node on the coordinator. The fetch plan runs elsewhere, on its own inputs. */
    @Override
    public AttributeSet inputSet() {
        return left().outputSet();
    }

    @Override
    protected AttributeSet computeReferences() {
        return leftReferences();
    }

    @Override
    public AttributeSet leftReferences() {
        return AttributeSet.of(docRef);
    }

    @Override
    public AttributeSet rightReferences() {
        return AttributeSet.EMPTY;
    }

    /**
     * This node emits the pages that the fetched columns are added to, so it consumes the estimate. The cut below it
     * is then sized for its own narrow rows, the reference and the columns it needs, and the whole cut reaches this
     * node in one page instead of being split into pages sized for the fetched columns.
     */
    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, output());
        int size = Math.max(1, state.consumeAllFields(false));
        return Objects.equals(size, estimatedRowSize)
            ? this
            : new FetchExec(source(), left(), right(), docRef, fetchedAttributes, stage, indexPattern, originalIndices, size);
    }

    @Override
    protected FetchExec replaceChildren(PhysicalPlan newLeft, PhysicalPlan newRight) {
        return new FetchExec(
            source(),
            newLeft,
            newRight,
            docRef,
            fetchedAttributes,
            stage,
            indexPattern,
            originalIndices,
            estimatedRowSize
        );
    }

    @Override
    protected NodeInfo<FetchExec> info() {
        return NodeInfo.create(
            this,
            FetchExec::new,
            left(),
            right(),
            docRef,
            fetchedAttributes,
            stage,
            indexPattern,
            originalIndices,
            estimatedRowSize
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        FetchExec other = (FetchExec) obj;
        return docRef.equals(other.docRef)
            && fetchedAttributes.equals(other.fetchedAttributes)
            && stage == other.stage
            && indexPattern.equals(other.indexPattern)
            && originalIndices.equals(other.originalIndices)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), docRef, fetchedAttributes, stage, indexPattern, originalIndices, estimatedRowSize);
    }
}
