/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;

/**
 * Attribute for referencing the TSDB metric temporality.
 * The name of the field is defined per index using the
 * {@link org.elasticsearch.index.IndexSettings#TIME_SERIES_TEMPORALITY_FIELD} index setting.
 * If an index does not have this setting, the temporality will always be null.
 */
public final class TemporalityAttribute extends TypedAttribute {

    public static final String NAME = "<temporality>";

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "TemporalityAttribute",
        TemporalityAttribute::readFrom
    );

    public TemporalityAttribute(Source source) {
        this(source, null);
    }

    private TemporalityAttribute(Source source, @Nullable NameId id) {
        super(source, NAME, DataType.KEYWORD, Nullability.TRUE, id, false);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (((PlanStreamOutput) out).writeAttributeCacheHeader(this)) {
            source().writeTo(out);
            id().writeTo(out);
        }
    }

    public static TemporalityAttribute readFrom(StreamInput in) throws IOException {
        return ((PlanStreamInput) in).readAttributeWithCache(TemporalityAttribute::innerReadFrom);
    }

    private static TemporalityAttribute innerReadFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        NameId id = NameId.readFrom((PlanStreamInput) in);
        return new TemporalityAttribute(source, id);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this);
    }

    @Override
    protected TemporalityAttribute clone(
        Source source,
        String qualifier,
        String name,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        // Ignore everything except for the source and id
        return new TemporalityAttribute(source, id);
    }

    @Override
    protected String label() {
        return "t";
    }

    @Override
    public boolean isDimension() {
        // temporality must be a dimension by definiton
        return true;
    }

    @Override
    public boolean isMetric() {
        return false;
    }
}
