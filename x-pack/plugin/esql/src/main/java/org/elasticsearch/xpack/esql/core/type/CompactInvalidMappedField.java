/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Memory-frugal counterpart to {@link InvalidMappedField}: stores at most {@value #MAX_INDICES_PER_TYPE} concrete index names per source
 * type instead of the full per-type index list.
 */
public final class CompactInvalidMappedField extends TypeConflictedField {
    private static final int MAX_INDICES_PER_TYPE = 3;

    private final String errorMessage;
    private final Map<DataType, Set<String>> typesToIndices;
    private final boolean isPotentiallyUnmapped;

    public CompactInvalidMappedField(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache
    ) {
        this(name, buildErrorMessage(typesToIndices, false), truncate(typesToIndices), indexDedupCache, false);
    }

    public static CompactInvalidMappedField potentiallyUnmapped(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return new CompactInvalidMappedField(
            name,
            buildErrorMessage(typesToIndices, true),
            truncate(typesToIndices),
            indexDedupCache,
            true
        );
    }

    private CompactInvalidMappedField(
        String name,
        String errorMessage,
        Map<DataType, Set<String>> typesToIndices,
        Map<Set<String>, Set<String>> dedupCache,
        boolean isPotentiallyUnmapped
    ) {
        super(name, DataType.UNSUPPORTED, new TreeMap<>(), false, TimeSeriesFieldType.UNKNOWN);

        for (var entry : typesToIndices.entrySet()) {
            entry.setValue(dedupCache.computeIfAbsent(entry.getValue(), unused -> entry.getValue()));
        }
        this.errorMessage = errorMessage;
        this.typesToIndices = typesToIndices;
        this.isPotentiallyUnmapped = isPotentiallyUnmapped;
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("CompactInvalidMappedField shouldn't be transported");
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "InvalidMappedField";
    }

    @Override
    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public Map<String, Set<String>> getTypesToIndices() {
        Map<String, Set<String>> result = new TreeMap<>();
        typesToIndices.forEach((k, v) -> result.put(k.typeName(), v));
        return result;
    }

    @Override
    public boolean isPotentiallyUnmapped() {
        return isPotentiallyUnmapped;
    }

    @Override
    public Set<DataType> types() {
        return typesToIndices.keySet();
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), errorMessage);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        CompactInvalidMappedField other = (CompactInvalidMappedField) obj;
        return Objects.equals(errorMessage, other.errorMessage);
    }

    /** Cap each per-type index set at {@value #MAX_INDICES_PER_TYPE} entries. */
    private static Map<DataType, Set<String>> truncate(Map<DataType, Set<String>> typesToIndices) {
        Map<DataType, Set<String>> result = new TreeMap<>();
        for (Map.Entry<DataType, Set<String>> entry : typesToIndices.entrySet()) {
            Set<String> indices = entry.getValue();
            result.put(entry.getKey(), indices.size() <= MAX_INDICES_PER_TYPE ? Set.copyOf(indices) : truncate(indices));
        }
        return result;
    }

    private static @NonNull Set<String> truncate(Set<String> indices) {
        Set<String> truncated = new LinkedHashSet<>(MAX_INDICES_PER_TYPE + 1);
        indices.stream().sorted().limit(MAX_INDICES_PER_TYPE).forEach(truncated::add);
        truncated.add("...");
        return Collections.unmodifiableSet(truncated);
    }

    private static String buildErrorMessage(Map<DataType, ? extends Set<String>> typesToIndices, boolean includeInsistKeyword) {
        Map<String, Set<String>> stringKeyed = new TreeMap<>();
        typesToIndices.forEach((k, v) -> stringKeyed.put(k.typeName(), v));
        return TypeConflictedField.makeErrorMessage(stringKeyed, includeInsistKeyword);
    }
}
