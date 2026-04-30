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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Representation of a field mapped differently across indices, or potentially unmapped in some, in which case it is treated as
 * {@link DataType#KEYWORD} in the indices where it is unmapped.
 * Used during mapping discovery only; the analyzer's {@code UnionTypesCleanup} converts any {@link InvalidMappedField} into an
 * {@link UnsupportedEsField}-backed attribute before the plan leaves the coordinator.
 * <p>
 * The two intrinsic pieces of state are {@link #getTypesToIndices()} and {@link #isPotentiallyUnmapped()}; together with the inherited
 * name/properties they fully determine the field's identity ({@link #equals(Object)} / {@link #hashCode()}).
 * <p>
 * The user-facing error message is derived lazily from those two fields on first access via {@link #errorMessage()} and is therefore not
 * part of equality/hashing.
 * <p>
 * {@link #writeContent(StreamOutput)} deliberately throws {@link UnsupportedOperationException}: serializing an
 * {@link InvalidMappedField} is a programming error because these objects must never be sent to data nodes. The {@link StreamInput}
 * constructor is kept solely for backward-compatibility deserialization of any data written before this restriction was enforced.
 * <p>
 * It is used specifically for the 'union types' and 'unmapped fields' feature in ES|QL.
 */
public class InvalidMappedField extends EsField {

    private final Map<String, Set<String>> typesToIndices;
    private final boolean isPotentiallyUnmapped;
    /**
     * Lazily derived from {@link #typesToIndices} and {@link #isPotentiallyUnmapped} on first access; not part of
     * {@link #equals(Object)} / {@link #hashCode()}. Pre-populated by {@link #InvalidMappedField(StreamInput)} so the message is
     * available immediately after deserialization without recomputing it.
     */
    private String cachedErrorMessage;

    public InvalidMappedField(String name, Map<String, Set<String>> typesToIndices) {
        // Use a mutable map: IndexResolver may add child fields into the properties of a conflicting parent field later.
        this(name, new HashMap<>(), typesToIndices, false, TimeSeriesFieldType.UNKNOWN, null);
    }

    public InvalidMappedField(String name, Map<String, Set<String>> typesToIndices, Map<String, EsField> properties) {
        this(name, properties, typesToIndices, false, TimeSeriesFieldType.UNKNOWN, null);
    }

    /**
     * An {@link InvalidMappedField} is potentially unmapped if at least one index does not contain a mapping for the field, and the user
     * requested we load the values from {@code _source}. In that case, there is (possibly) an additional type conflict since we treat
     * unmapped fields as {@link DataType#KEYWORD}.
     */
    public static InvalidMappedField potentiallyUnmapped(String name, Map<String, Set<String>> typesToIndices) {
        // Use a mutable map: IndexResolver may add child fields into the properties of a conflicting parent field later.
        return new InvalidMappedField(name, new HashMap<>(), typesToIndices, true, TimeSeriesFieldType.UNKNOWN, null);
    }

    private InvalidMappedField(
        String name,
        Map<String, EsField> properties,
        Map<String, Set<String>> typesToIndices,
        boolean isPotentiallyUnmapped,
        TimeSeriesFieldType type,
        String cachedErrorMessage
    ) {
        super(name, DataType.UNSUPPORTED, properties, false, type);
        this.typesToIndices = typesToIndices;
        this.isPotentiallyUnmapped = isPotentiallyUnmapped;
        this.cachedErrorMessage = cachedErrorMessage;
    }

    protected InvalidMappedField(StreamInput in) throws IOException {
        // Wire field order: name, errorMessage, properties, timeSeriesFieldType.
        // typesToIndices/isPotentiallyUnmapped are not on the wire; the error message is pre-populated so it survives the round-trip.
        this(
            ((PlanStreamInput) in).readCachedString(),
            in.readString(),
            in.readImmutableMap(StreamInput::readString, EsField::readFrom),
            readTimeSeriesFieldType(in)
        );
    }

    private InvalidMappedField(String name, String errorMessage, Map<String, EsField> properties, TimeSeriesFieldType type) {
        this(name, properties, Map.of(), false, type, errorMessage);
    }

    @Override
    public void writeContent(StreamOutput out) {
        throw new UnsupportedOperationException(
            "InvalidMappedField must be converted to UnsupportedEsField by UnionTypesCleanup before the plan is serialized"
        );
    }

    public String getWriteableName(TransportVersion transportVersion) {
        return "InvalidMappedField";
    }

    public Set<DataType> types() {
        return typesToIndices.keySet().stream().map(DataType::fromTypeName).collect(Collectors.toSet());
    }

    /**
     * Returns the error message describing why this field is invalid. The message is derived lazily from
     * {@link #getTypesToIndices()} and {@link #isPotentiallyUnmapped()} on first access. Not part of {@link #equals(Object)} /
     * {@link #hashCode()}.
     */
    public String errorMessage() {
        if (cachedErrorMessage == null) {
            cachedErrorMessage = makeErrorMessage(typesToIndices, isPotentiallyUnmapped);
        }
        return cachedErrorMessage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), typesToIndices, isPotentiallyUnmapped);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            InvalidMappedField other = (InvalidMappedField) obj;
            return Objects.equals(typesToIndices, other.typesToIndices) && isPotentiallyUnmapped == other.isPotentiallyUnmapped;
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

    public boolean isPotentiallyUnmapped() {
        return isPotentiallyUnmapped;
    }

    private static String makeErrorMessage(Map<String, Set<String>> typesToIndices, boolean includeInsistKeyword) {
        StringBuilder errorMessage = new StringBuilder();
        var isInsistKeywordOnlyKeyword = includeInsistKeyword && typesToIndices.containsKey(DataType.KEYWORD.typeName()) == false;
        errorMessage.append("mapped as [");
        errorMessage.append(typesToIndices.size() + (isInsistKeywordOnlyKeyword ? 1 : 0));
        errorMessage.append("] incompatible types: ");
        boolean first = true;
        if (isInsistKeywordOnlyKeyword) {
            first = false;
            errorMessage.append("[keyword] due to loading from _source");
        }
        for (Map.Entry<String, Set<String>> e : typesToIndices.entrySet()) {
            if (first) {
                first = false;
            } else {
                errorMessage.append(", ");
            }
            errorMessage.append("[");
            errorMessage.append(e.getKey());
            errorMessage.append("] ");
            if (e.getKey().equals(DataType.KEYWORD.typeName()) && includeInsistKeyword) {
                errorMessage.append("due to loading from _source and in ");
            } else {
                errorMessage.append("in ");
            }
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
