/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Map;

/**
 * Attribute for referencing the TSDB metric temporality.
 * The name of the field is defined per index using the {@code index.time_series.temporality_field} index setting.
 * If an index does not have this setting, the temporality will always be null.
 */
public final class TemporalityAttribute extends FieldAttribute {

    /**
     * At the time the attribute is created, we don't know the field name yet.
     * Therefore we use this field as a placeholder, which we'll resolve to the actual field block loader
     * (or a constant-null if non-existent) during physical planning.
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
        super(source, null, null, DUMMY_FIELD_NAME, ES_FIELD, Nullability.TRUE, null, false);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this);
    }
}
