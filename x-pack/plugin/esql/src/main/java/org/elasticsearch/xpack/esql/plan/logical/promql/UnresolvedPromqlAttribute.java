/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A label of a translated PromQL plan, not yet bound to a column: a promoted label by name, or the _timeseries
 * metadata holding every label not promoted. Translation emits these so Bind can resolve without depending on
 * which columns the input already carries.
 * <p>
 * A metadata reference's unset is the transform its operator applies to input metadata: labels dropped from the
 * blob (without(...), label_replace destination, arith dropping __name__, …). PathTrace accumulates those into
 * the against-source view that names the column. Leaving labels out requires typed {@code _timeseries}
 * ({@code ts_unset}); keyword metadata does not support that.
 */
public final class UnresolvedPromqlAttribute extends Attribute implements Unresolvable {

    private final @Nullable Set<String> unset;

    /** A reference to the promoted label name. */
    public static UnresolvedPromqlAttribute label(Source source, String name) {
        return new UnresolvedPromqlAttribute(source, name, null, null);
    }

    /** Metadata of the operator's input after applying unset. */
    public static UnresolvedPromqlAttribute metadata(Source source, Set<String> unset) {
        return new UnresolvedPromqlAttribute(source, PromqlLabels.metadataLabelName(Set.of()), unset, null);
    }

    /** A reference to what an element of a tsIdentity stands for: its metadata, or one of its promoted labels. */
    public static UnresolvedPromqlAttribute of(Source source, Attribute element) {
        return element instanceof UnresolvedPromqlAttribute timeSeriesMetadata && timeSeriesMetadata.isMetadata()
            ? metadata(source, timeSeriesMetadata.unset())
            : label(source, PromqlLabels.labelName(element));
    }

    /**
     * References for a plan's {@code tsIdentity}: time-series metadata (if any), then each promoted label.
     * The tuple of those two is the series identity used for grouping, matching, and union dedup.
     */
    public static List<Attribute> tsIdentity(Source source, List<Attribute> tsIdentity) {
        var result = new ArrayList<Attribute>();
        UnresolvedPromqlAttribute timeSeriesMetadata = PromqlLabels.metadataLabel(tsIdentity);
        if (timeSeriesMetadata != null) {
            result.add(of(source, timeSeriesMetadata));
        }
        for (Attribute attribute : tsIdentity) {
            if (PromqlLabels.isMetadataLabel(attribute) == false) {
                result.add(of(source, attribute));
            }
        }
        return result;
    }

    public UnresolvedPromqlAttribute(Source source, String name, @Nullable Set<String> unset, @Nullable NameId id) {
        super(source, null, name, id);
        this.unset = unset == null ? null : Collections.unmodifiableSet(new LinkedHashSet<>(unset));
    }

    /**
     * Labels this metadata transform drops from its input blob (without, label_replace dst, …).
     * Null for a promoted label. After PathTrace, absolute against the source.
     */
    public @Nullable Set<String> unset() {
        return unset;
    }

    public boolean isMetadata() {
        return unset != null;
    }

    /** PathTrace result: unset absolute against the source series, named via metadataLabelName. */
    public UnresolvedPromqlAttribute againstSource(Set<String> unset) {
        assert isMetadata() : "only metadata leaves labels out [" + name() + "]";
        return new UnresolvedPromqlAttribute(source(), PromqlLabels.metadataLabelName(unset), unset, id());
    }

    /** True when both read the same column. */
    public boolean reads(UnresolvedPromqlAttribute other) {
        return name().equals(other.name()) && Objects.equals(unset, other.unset);
    }

    @Override
    public String unresolvedMessage() {
        return "unresolved PromQL label [" + name() + "]";
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected NodeInfo<UnresolvedPromqlAttribute> info() {
        return NodeInfo.create(this, UnresolvedPromqlAttribute::new, name(), unset, id());
    }

    @Override
    protected Attribute clone(
        Source source,
        String qualifier,
        String name,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        return new UnresolvedPromqlAttribute(source, name, unset, id);
    }

    @Override
    protected String label() {
        return "l";
    }

    @Override
    public boolean isDimension() {
        return false;
    }

    @Override
    public boolean isMetric() {
        return false;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the analyzer");
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("doesn't escape the analyzer");
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(ignoreIds), unset);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        UnresolvedPromqlAttribute other = (UnresolvedPromqlAttribute) o;
        return super.innerEquals(o, ignoreIds) && Objects.equals(unset, other.unset);
    }
}
