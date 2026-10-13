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
import org.elasticsearch.compute.data.DocVector;
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
 * Loads the fetched columns of the rows that survived a cut from the nodes that own their documents.
 * <p>
 * The child produces the rows: the coordinator half of the cut over the exchange, with the {@code _doc} of each row
 * next to the columns the cut needed. Those documents left the nodes that read them and carry their origins, see
 * {@link DocVector#origins()}. Like {@link FieldExtractExec}, the output is the child's output followed by the fetched
 * columns, so the plan above this node doesn't change.
 */
public final class FetchExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "FetchExec",
        FetchExec::new
    );

    private final Attribute doc;
    private final List<Attribute> fetchedAttributes;
    private final String indexPattern;
    private final List<String> originalIndices;
    private final Integer estimatedRowSize;
    private List<Attribute> lazyOutput;

    /**
     * @param doc               the {@code _doc} column of {@code child}
     * @param fetchedAttributes the columns the fetch appends to each row
     * @param indexPattern      the index pattern of the relation the documents come from
     * @param originalIndices   the index expressions the user wrote for the local cluster. A fetch request carries
     *                          them so it is authorized like the query that selected the documents.
     */
    public FetchExec(
        Source source,
        PhysicalPlan child,
        Attribute doc,
        List<Attribute> fetchedAttributes,
        String indexPattern,
        List<String> originalIndices,
        Integer estimatedRowSize
    ) {
        super(source, child);
        this.doc = doc;
        this.fetchedAttributes = fetchedAttributes;
        this.indexPattern = indexPattern;
        this.originalIndices = originalIndices;
        this.estimatedRowSize = estimatedRowSize;
    }

    private FetchExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readString(),
            in.readStringCollectionAsList(),
            in.readOptionalVInt()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(doc);
        out.writeNamedWriteableCollection(fetchedAttributes);
        out.writeString(indexPattern);
        out.writeStringCollection(originalIndices);
        out.writeOptionalVInt(estimatedRowSize);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Attribute doc() {
        return doc;
    }

    public List<Attribute> fetchedAttributes() {
        return fetchedAttributes;
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
            List<Attribute> childOutput = child().output();
            List<Attribute> output = new ArrayList<>(childOutput.size() + fetchedAttributes.size());
            output.addAll(childOutput);
            output.addAll(fetchedAttributes);
            lazyOutput = output;
        }
        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(doc);
    }

    /**
     * This node emits the pages that the fetched columns are added to, so it consumes the estimate. The cut below it
     * is then sized for its own narrow rows, the documents and the columns it needs, and the whole cut reaches this
     * node in one page instead of being split into pages sized for the fetched columns.
     */
    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, output());
        int size = Math.max(1, state.consumeAllFields(false));
        return Objects.equals(size, estimatedRowSize)
            ? this
            : new FetchExec(source(), child(), doc, fetchedAttributes, indexPattern, originalIndices, size);
    }

    @Override
    public FetchExec replaceChild(PhysicalPlan newChild) {
        return new FetchExec(source(), newChild, doc, fetchedAttributes, indexPattern, originalIndices, estimatedRowSize);
    }

    @Override
    protected NodeInfo<FetchExec> info() {
        return NodeInfo.create(this, FetchExec::new, child(), doc, fetchedAttributes, indexPattern, originalIndices, estimatedRowSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FetchExec other = (FetchExec) obj;
        return child().equals(other.child())
            && doc.equals(other.doc)
            && fetchedAttributes.equals(other.fetchedAttributes)
            && indexPattern.equals(other.indexPattern)
            && originalIndices.equals(other.originalIndices)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), doc, fetchedAttributes, indexPattern, originalIndices, estimatedRowSize);
    }
}
