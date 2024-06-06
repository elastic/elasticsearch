/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Information about a field in an ES index with the {@code keyword} type.
 */
public class KeywordEsField extends EsField {
    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        EsField.class,
        "KeywordEsField",
        KeywordEsField::new
    );

    private final int precision;
    private final boolean normalized;

    public KeywordEsField(String name) {
        this(name, Collections.emptyMap(), true, Short.MAX_VALUE, false);
    }

    public KeywordEsField(String name, Map<String, EsField> properties, boolean hasDocValues, int precision, boolean normalized) {
        this(name, properties, hasDocValues, precision, normalized, false);
    }

    public KeywordEsField(
        String name,
        Map<String, EsField> properties,
        boolean hasDocValues,
        int precision,
        boolean normalized,
        boolean isAlias
    ) {
        this(name, KEYWORD, properties, hasDocValues, precision, normalized, isAlias);
    }

    protected KeywordEsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean hasDocValues,
        int precision,
        boolean normalized,
        boolean isAlias
    ) {
        super(name, esDataType, properties, hasDocValues, isAlias);
        this.precision = precision;
        this.normalized = normalized;
    }

    private KeywordEsField(StreamInput in) throws IOException {
        this(
            in.readString(),
            KEYWORD,
            in.readMap(i -> i.readNamedWriteable(EsField.class)),
            in.readBoolean(),
            in.readInt(),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getName());
        out.writeMap(getProperties(), StreamOutput::writeNamedWriteable);
        out.writeBoolean(isAggregatable());
        out.writeInt(precision);
        out.writeBoolean(normalized);
        out.writeBoolean(isAlias());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public int getPrecision() {
        return precision;
    }

    public boolean getNormalized() {
        return normalized;
    }

    @Override
    public Exact getExactInfo() {
        return new Exact(normalized == false, "Normalized keyword field cannot be used for exact match operations");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        KeywordEsField that = (KeywordEsField) o;
        return precision == that.precision && normalized == that.normalized;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, normalized);
    }
}
