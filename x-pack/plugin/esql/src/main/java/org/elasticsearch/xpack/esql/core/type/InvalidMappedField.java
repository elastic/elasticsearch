/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * <p>
 * N.B.: This class exists only as a backward-compatible version of {@link CompactInvalidMappedField}.
 * </p>
 * Representation of a field mapped differently across indices. When {@code SET unmapped_fields="LOAD"} this also includes indices missing
 * the field in their mappings, in which case it is treated as {@link DataType#KEYWORD}.
 * Used during analysis only; the analyzer's {@code UnionTypesCleanup} converts any {@link InvalidMappedField}s before the plan leaves
 * the coordinator.
 */
public final class InvalidMappedField extends TypeConflictedField {

    /** How many index names per source type to spell out in {@link #errorMessage()} before collapsing the rest into "and [N] other". */
    private static final int MAX_INDICES_TO_DISPLAY = 3;

    private final Map<String, Set<String>> typesToIndices;
    private final boolean isPotentiallyUnmapped;

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
    }

    @Override
    Map<String, Sample> samples() {
        // Preserve typesToIndices' own iteration order so the rendered error lists types in the order they were discovered (which the
        // production resolver supplies as a TreeMap, but tests may supply in a deliberate insertion order).
        Map<String, Sample> samples = new LinkedHashMap<>();
        typesToIndices.forEach((type, indices) -> samples.put(type, toSample(indices)));
        return samples;
    }

    private static Sample toSample(Set<String> indices) {
        // At or below the display cap we show the full set in its original iteration order; above it we sort and take the first few so
        // the rendered list is deterministic.
        return indices.size() <= MAX_INDICES_TO_DISPLAY
            ? new Sample(indices, indices.size())
            : new Sample(indices.stream().sorted().limit(MAX_INDICES_TO_DISPLAY).toList(), indices.size());
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
    public Map<String, Set<String>> getTypesToIndices() {
        return typesToIndices;
    }

    @Override
    public boolean isPotentiallyUnmapped() {
        return isPotentiallyUnmapped;
    }

    @Override
    public Set<DataType> types() {
        return getTypesToIndices().keySet().stream().map(DataType::fromTypeName).collect(Collectors.toSet());
    }
}
