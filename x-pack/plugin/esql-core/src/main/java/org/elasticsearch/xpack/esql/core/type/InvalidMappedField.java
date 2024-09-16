/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Representation of field mapped differently across indices.
 * Used during mapping discovery only.
 * Note that the field <code>typesToIndices</code> is not serialized because that information is
 * not required through the cluster, only surviving as long as the Analyser phase of query planning.
 * It is used specifically for the 'union types' feature in ES|QL.
 */
public class InvalidMappedField extends EsField {

    private final String errorMessage;
    private final Map<String, Set<String>> typesToIndices;

    public InvalidMappedField(String name, String errorMessage, Map<String, EsField> properties) {
        this(name, errorMessage, properties, Map.of());
    }

    public InvalidMappedField(String name, String errorMessage) {
        this(name, errorMessage, new TreeMap<>());
    }

    /**
     * Constructor supporting union types, used in ES|QL.
     */
    public InvalidMappedField(String name, Map<String, Set<String>> typesToIndices) {
        this(name, makeErrorMessage(typesToIndices), new TreeMap<>(), typesToIndices);
    }

    private InvalidMappedField(String name, String errorMessage, Map<String, EsField> properties, Map<String, Set<String>> typesToIndices) {
        super(name, DataType.UNSUPPORTED, properties, false);
        this.errorMessage = errorMessage;
        this.typesToIndices = typesToIndices;
    }

    protected InvalidMappedField(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readImmutableMap(StreamInput::readString, EsField::readFrom));
    }

    public Set<DataType> types() {
        return typesToIndices.keySet().stream().map(DataType::fromTypeName).collect(Collectors.toSet());
    }

    @Override
    protected void writeContent(StreamOutput out) throws IOException {
        out.writeString(getName());
        out.writeString(errorMessage);
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
    }

    public String getWriteableName() {
        return "InvalidMappedField";
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

    public Map<String, Set<String>> getTypesToIndices() {
        return typesToIndices;
    }

    private static String makeErrorMessage(Map<String, Set<String>> typesToIndices) {
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append("mapped as [");
        errorMessage.append(typesToIndices.size());
        errorMessage.append("] incompatible types: ");
        boolean first = true;
        for (Map.Entry<String, Set<String>> e : typesToIndices.entrySet()) {
            if (first) {
                first = false;
            } else {
                errorMessage.append(", ");
            }
            errorMessage.append("[");
            errorMessage.append(e.getKey());
            errorMessage.append("] in ");
            if (e.getValue().size() <= 3) {
                errorMessage.append(e.getValue());
            } else {
                errorMessage.append(e.getValue().stream().sorted().limit(3).collect(Collectors.toList()));
                errorMessage.append(" and [" + (e.getValue().size() - 3) + "] other ");
                errorMessage.append(e.getValue().size() == 4 ? "index" : "indices");
            }
        }
        return errorMessage.toString();
    }
}
