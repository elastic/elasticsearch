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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Local-only physical plan node that materializes deferred ("wide") columns for the rows that
 * survive a per-driver TopN above an {@link ExternalSourceExec}.
 * <p>
 * Inserted by {@code InsertExternalFieldExtraction} immediately above the TopN when the source's
 * format supports {@link org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor column
 * extraction}. The rule narrows the source to sort-key + predicate columns (plus a synthetic
 * {@code _rowPosition}); this node consumes the narrow output and loads the remaining columns
 * for the at-most-LIMIT rows the TopN selected. The {@code _rowPosition} channel is stripped
 * from the output so downstream operators see the same shape as if no late materialization had
 * happened.
 * <p>
 * <b>NOT serialized.</b> Created by the local physical optimizer on the data node after the
 * plan fragment arrives over the wire; consumed by {@code LocalExecutionPlanner} on the same
 * JVM. Same lifecycle as {@code ExternalSourceExec.pushedFilter} and
 * {@code ExternalSourceExec.pushedLimit}.
 */
public class ExternalFieldExtractExec extends UnaryExec {

    private final List<Attribute> attributesToExtract;
    private final Attribute rowPositionAttribute;

    private List<Attribute> lazyOutput;

    public ExternalFieldExtractExec(
        Source source,
        PhysicalPlan child,
        List<Attribute> attributesToExtract,
        Attribute rowPositionAttribute
    ) {
        super(source, child);
        if (attributesToExtract == null) {
            throw new IllegalArgumentException("attributesToExtract must not be null");
        }
        if (rowPositionAttribute == null) {
            throw new IllegalArgumentException("rowPositionAttribute must not be null");
        }
        this.attributesToExtract = attributesToExtract;
        this.rowPositionAttribute = rowPositionAttribute;
    }

    public List<Attribute> attributesToExtract() {
        return attributesToExtract;
    }

    public Attribute rowPositionAttribute() {
        return rowPositionAttribute;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<Attribute> childOutput = child().output();
            List<Attribute> out = new ArrayList<>(childOutput.size() - 1 + attributesToExtract.size());
            for (Attribute a : childOutput) {
                // Strip _rowPosition: it served as the late-materialization key and is not a user
                // column. Equality is by NameId so this is safe even if a user column happens to
                // be named "_rowPosition".
                if (a.equals(rowPositionAttribute) == false) {
                    out.add(a);
                }
            }
            out.addAll(attributesToExtract);
            lazyOutput = List.copyOf(out);
        }
        return lazyOutput;
    }

    @Override
    public ExternalFieldExtractExec replaceChild(PhysicalPlan newChild) {
        return new ExternalFieldExtractExec(source(), newChild, attributesToExtract, rowPositionAttribute);
    }

    /**
     * The only attribute this node requires from its child output is {@code _rowPosition} — the
     * key into the per-driver
     * {@link org.elasticsearch.xpack.esql.datasources.SourceExtractors source-extractor registry}.
     * {@code attributesToExtract} are <em>produced</em> by this node, not consumed from the
     * child; advertising them as references would mislead column-pruning rules into pinning them
     * on the (narrowed) source.
     */
    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(rowPositionAttribute);
    }

    @Override
    protected NodeInfo<? extends ExternalFieldExtractExec> info() {
        return NodeInfo.create(this, ExternalFieldExtractExec::new, child(), attributesToExtract, rowPositionAttribute);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("ExternalFieldExtractExec is local-only and not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("ExternalFieldExtractExec is local-only and not serialized");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), attributesToExtract, rowPositionAttribute);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        ExternalFieldExtractExec other = (ExternalFieldExtractExec) obj;
        return Objects.equals(attributesToExtract, other.attributesToExtract)
            && Objects.equals(rowPositionAttribute, other.rowPositionAttribute);
    }
}
