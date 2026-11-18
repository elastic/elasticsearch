/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Objects;

/**
 * A label in the declared output of a PromQL node, before translation: a label by name, or the tags label, the
 * {@code _timeseries} label container holding every label the node does not declare. The analyzer resolves the command's
 * output against these; translation reads and writes labels through the label container functions, which carry what a node
 * leaves out.
 */
public final class UnresolvedPromqlLabelAttribute extends Attribute implements Unresolvable {

    private final boolean tags;

    /** A label by name. */
    public static UnresolvedPromqlLabelAttribute label(Source source, String name) {
        return new UnresolvedPromqlLabelAttribute(source, name, false, null);
    }

    /** The tags label: the label container of the node's series. */
    public static UnresolvedPromqlLabelAttribute newTagsLabel(Source source) {
        return new UnresolvedPromqlLabelAttribute(source, MetadataAttribute.TIMESERIES, true, null);
    }

    public UnresolvedPromqlLabelAttribute(Source source, String name, boolean tags, NameId id) {
        super(source, null, name, id);
        this.tags = tags;
    }

    /** Whether this is the tags label rather than a label by name. */
    public boolean isTagsLabel() {
        return tags;
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
    protected NodeInfo<UnresolvedPromqlLabelAttribute> info() {
        return NodeInfo.create(this, UnresolvedPromqlLabelAttribute::new, name(), tags, id());
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
        return new UnresolvedPromqlLabelAttribute(source, name, tags, id);
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
        return Objects.hash(super.innerHashCode(ignoreIds), tags);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        return super.innerEquals(o, ignoreIds) && tags == ((UnresolvedPromqlLabelAttribute) o).tags;
    }
}
