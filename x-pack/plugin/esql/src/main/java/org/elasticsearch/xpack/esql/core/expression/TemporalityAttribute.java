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
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.Map;

/**
 * Attribute for referencing the TSDB metric temporality.
 * The name of the field is defined per index using the
 * {@link org.elasticsearch.index.IndexSettings#TIME_SERIES_TEMPORALITY_FIELD} index setting.
 * If an index does not have this setting, the temporality will always be null.
 */
public final class TemporalityAttribute extends FieldAttribute {

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "TemporalityAttribute",
        TemporalityAttribute::readFrom
    );

    /**
     * At the time the attribute is created, we don't know the field name yet.
     * Therefore, we use this name as a placeholder, which we'll resolve to the actual field block loader
     * (or a constant-null loader if no temporality field exists) during physical planning.
     */
    private static final String DUMMY_FIELD_NAME = "_temporality_placeholder";

    private static final EsField ES_FIELD = new EsField(
        DUMMY_FIELD_NAME,
        DataType.KEYWORD,
        Map.of(),
        false,
        EsField.TimeSeriesFieldType.DIMENSION
    );

    public TemporalityAttribute(Source source) {
        this(source, null);
    }

    private TemporalityAttribute(Source source, @Nullable NameId id) {
        super(source, null, null, DUMMY_FIELD_NAME, ES_FIELD, Nullability.TRUE, id, false);
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
}
