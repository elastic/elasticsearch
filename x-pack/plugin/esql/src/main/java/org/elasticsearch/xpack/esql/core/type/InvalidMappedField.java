/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

// FIXME(gal, NOCOMMIT) Redocument that this is for BWC
/**
 * Representation of field mapped differently across indices; or being potentially unmapped in some, in which case it is treated as
 * {@link DataType#KEYWORD} in the indices where it is unmapped.
 * Used during mapping discovery only.
 * Note that the fields <code>typesToIndices</code> and <code>isPotentiallyUnmapped</code> are not serialized because that information is
 * not required through the cluster, only surviving as long as the Analyser phase of query planning.
 * It is used specifically for the 'union types' and 'unmapped fields' feature in ES|QL.
 */
public final class InvalidMappedField extends EsField implements TypeConflictField {

    private final String errorMessage;
    private final Map<String, Set<String>> typesToIndices;
    private final boolean isPotentiallyUnmapped;

    public InvalidMappedField(String name, String errorMessage, Map<String, EsField> properties) {
        this(name, errorMessage, properties, Map.of(), false, TimeSeriesFieldType.UNKNOWN);
    }

    public InvalidMappedField(String name, String errorMessage) {
        this(name, errorMessage, new TreeMap<>());
    }

    public InvalidMappedField(String name, Map<String, Set<String>> typesToIndices) {
        this(
            name,
            TypeConflictField.makeErrorMessage(typesToIndices, false),
            new TreeMap<>(),
            typesToIndices,
            false,
            TimeSeriesFieldType.UNKNOWN
        );
    }

    /**
     * An {@link InvalidMappedField} is potentially unmapped if at least one index does not contain a mapping for the field, and the user
     * requested we load the values from {@code _source}. In that case, there is (possibly) an additional type conflict since we treat
     * unmapped fields as {@link DataType#KEYWORD}.
     */
    public static InvalidMappedField potentiallyUnmapped(String name, Map<String, Set<String>> typesToIndices) {
        return new InvalidMappedField(
            name,
            TypeConflictField.makeErrorMessage(typesToIndices, true),
            new TreeMap<>(),
            typesToIndices,
            true,
            TimeSeriesFieldType.UNKNOWN
        );
    }

    private InvalidMappedField(
        String name,
        String errorMessage,
        Map<String, EsField> properties,
        Map<String, Set<String>> typesToIndices,
        boolean isPotentiallyUnmapped,
        TimeSeriesFieldType type
    ) {
        super(name, DataType.UNSUPPORTED, properties, false, type);
        this.errorMessage = errorMessage;
        this.typesToIndices = typesToIndices;
        this.isPotentiallyUnmapped = isPotentiallyUnmapped;
    }

    protected InvalidMappedField(StreamInput in) throws IOException {
        this(
            ((PlanStreamInput) in).readCachedString(),
            in.readString(),
            in.readImmutableMap(StreamInput::readString, EsField::readFrom),
            Map.of(),
            false,
            readTimeSeriesFieldType(in)
        );
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        ((PlanStreamOutput) out).writeCachedString(getName());
        out.writeString(errorMessage);
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
        writeTimeSeriesFieldType(out);
    }

    public String getWriteableName(TransportVersion transportVersion) {
        return "InvalidMappedField";
    }

    @Override
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

    @Override
    public Map<String, Set<String>> getTypesToIndices() {
        return typesToIndices;
    }

    @Override
    public boolean isPotentiallyUnmapped() {
        return isPotentiallyUnmapped;
    }
}
