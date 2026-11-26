/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Loads the fetched columns of the rows that survived a {@code LIMIT} or {@code TopN} from the documents they came from,
 * instead of loading them on the data nodes for every row the cut later drops.
 * <p>
 * The child is the cut, narrowed to the {@code _doc} of each row and the columns that cross the exchange as values. The
 * output is the child's output followed by the fetched columns. The fetched columns keep the attribute ids the relation
 * gave them, and the narrowing projection removed them below this node, so the plan above it doesn't change.
 * <p>
 * {@code PlanFetch} adds this node in the last batch of the logical optimizer, so no other logical rule meets it. The
 * mapper turns it into a {@link FetchExec} above the coordinator half of the cut. It doesn't escape the coordinator.
 */
public final class Fetch extends UnaryPlan implements ExecutesOn.Coordinator {

    private final Attribute doc;
    private final List<Attribute> fetchedAttributes;
    private final String indexPattern;
    private final List<String> originalIndices;
    private List<Attribute> lazyOutput;

    /**
     * @param doc               the {@code _doc} column of {@code child}
     * @param fetchedAttributes the columns the fetch appends to each row
     * @param indexPattern      the index pattern of the relation the documents come from
     * @param originalIndices   the index expressions the user wrote for the local cluster, so a fetch is authorized like
     *                          the query that selected the documents
     */
    public Fetch(
        Source source,
        LogicalPlan child,
        Attribute doc,
        List<Attribute> fetchedAttributes,
        String indexPattern,
        List<String> originalIndices
    ) {
        super(source, child);
        this.doc = doc;
        this.fetchedAttributes = fetchedAttributes;
        this.indexPattern = indexPattern;
        this.originalIndices = originalIndices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
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

    /** The fetched columns are produced here, not read from the child. */
    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(doc);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public Fetch replaceChild(LogicalPlan newChild) {
        return new Fetch(source(), newChild, doc, fetchedAttributes, indexPattern, originalIndices);
    }

    @Override
    protected NodeInfo<Fetch> info() {
        return NodeInfo.create(this, Fetch::new, child(), doc, fetchedAttributes, indexPattern, originalIndices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), doc, fetchedAttributes, indexPattern, originalIndices);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Fetch other = (Fetch) obj;
        return child().equals(other.child())
            && doc.equals(other.doc)
            && fetchedAttributes.equals(other.fetchedAttributes)
            && indexPattern.equals(other.indexPattern)
            && originalIndices.equals(other.originalIndices);
    }
}
