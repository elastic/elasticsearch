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
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Representation of field mapped differently across indices.
 * Used during mapping discovery only.
 */
public class InvalidMappedField extends EsField {
    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        EsField.class,
        "InvalidMappedField",
        InvalidMappedField::new
    );

    private final String errorMessage;

    public InvalidMappedField(String name, String errorMessage, Map<String, EsField> properties) {
        super(name, DataType.UNSUPPORTED, properties, false);
        this.errorMessage = errorMessage;
    }

    public InvalidMappedField(String name, String errorMessage) {
        this(name, errorMessage, new TreeMap<>());
    }

    public InvalidMappedField(String name) {
        this(name, StringUtils.EMPTY, new TreeMap<>());
    }

    private InvalidMappedField(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readImmutableMap(StreamInput::readString, i -> i.readNamedWriteable(EsField.class)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getName());
        out.writeString(errorMessage);
        out.writeMap(getProperties(), StreamOutput::writeNamedWriteable);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), errorMessage);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            InvalidMappedField other = (InvalidMappedField) obj;
            return Objects.equals(errorMessage, other.errorMessage);
        }

        return false;
    }

    @Override
    public EsField getExactField() {
        throw new QlIllegalArgumentException("Field [" + getName() + "] is invalid, cannot access it");

    }

    @Override
    public Exact getExactInfo() {
        return new Exact(false, "Field [" + getName() + "] is invalid, cannot access it");
    }
}
