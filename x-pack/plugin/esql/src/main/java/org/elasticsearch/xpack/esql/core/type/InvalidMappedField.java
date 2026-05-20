/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Representation of a field mapped differently across indices. When {@code SET unmapped_fields="LOAD"} this also includes indices missing
 * the field in their mappings, in which case it is treated as {@link DataType#KEYWORD}.
 * <p>
 * Used during analysis only; the analyzer's {@code UnionTypesCleanup} converts any {@link InvalidMappedField}s before the plan leaves
 * the coordinator.
 */
public class InvalidMappedField extends EsField {

    private final Map<String, Set<String>> typesToIndices;
    private final boolean isPotentiallyUnmapped;
    /**
     * Lazily derived from {@link #typesToIndices} and {@link #isPotentiallyUnmapped} on first access; not part of
     * {@link #equals(Object)} / {@link #hashCode()}.
     */
    private String cachedErrorMessage;

    public InvalidMappedField(String name, Map<String, Set<String>> typesToIndices) {
        // Use a mutable map: IndexResolver may add child fields into the properties of a conflicting parent field later.
        this(name, new TreeMap<>(), typesToIndices, false, TimeSeriesFieldType.UNKNOWN);
    }

    public InvalidMappedField(String name, Map<String, Set<String>> typesToIndices, Map<String, EsField> properties) {
        this(name, properties, typesToIndices, false, TimeSeriesFieldType.UNKNOWN);
    }

    /**
     * An {@link InvalidMappedField} is potentially unmapped if at least one index does not contain a mapping for the field, and the user
     * requested we load the values from {@code _source}. In that case, there is (possibly) an additional type conflict since we treat
     * unmapped fields as {@link DataType#KEYWORD}.
     */
    public static InvalidMappedField potentiallyUnmapped(String name, Map<String, Set<String>> typesToIndices) {
        // Use a mutable map: IndexResolver may add child fields into the properties of a conflicting parent field later.
        return new InvalidMappedField(name, new TreeMap<>(), typesToIndices, true, TimeSeriesFieldType.UNKNOWN);
    }

    private InvalidMappedField(
        String name,
        Map<String, EsField> properties,
        Map<String, Set<String>> typesToIndices,
        boolean isPotentiallyUnmapped,
        TimeSeriesFieldType type
    ) {
        super(name, DataType.UNSUPPORTED, properties, false, type);
        this.typesToIndices = typesToIndices;
        this.isPotentiallyUnmapped = isPotentiallyUnmapped;
        this.cachedErrorMessage = null;
    }

    @Override
    public void writeContent(StreamOutput out) {
        throw new UnsupportedOperationException("InvalidMappedField must never leave the coordinator");
    }

    public Set<DataType> types() {
        return typesToIndices.keySet().stream().map(DataType::fromTypeName).collect(Collectors.toSet());
    }

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
            return isPotentiallyUnmapped == other.isPotentiallyUnmapped && Objects.equals(typesToIndices, other.typesToIndices);
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
