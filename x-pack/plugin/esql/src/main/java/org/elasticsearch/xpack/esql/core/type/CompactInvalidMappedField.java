/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Memory-frugal counterpart to {@link InvalidMappedField}: stores at most {@value #MAX_INDICES_PER_TYPE} concrete index names per source
 * type alongside an integer count of the names that were dropped. Together they let {@link #errorMessage()} reconstruct the same
 * "and [N] other indices" rendering the full per-type list would have produced, without retaining the full list.
 */
public final class CompactInvalidMappedField extends TypeConflictedField {
    private static final int MAX_INDICES_PER_TYPE = 3;

    private record TruncatedIndices(Set<String> kept, int dropped) {}

    private final Map<DataType, TruncatedIndices> typesToIndices;
    private final boolean isPotentiallyUnmapped;

    public static CompactInvalidMappedField mappedEverywhere(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return new CompactInvalidMappedField(name, truncatedAndDedup(typesToIndices, indexDedupCache), false);
    }

    public static CompactInvalidMappedField potentiallyUnmapped(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return new CompactInvalidMappedField(name, truncatedAndDedup(typesToIndices, indexDedupCache), true);
    }

    private static Set<String> truncate(Set<String> indices) {
        Set<String> truncated = new LinkedHashSet<>(MAX_INDICES_PER_TYPE);
        indices.stream().sorted().limit(MAX_INDICES_PER_TYPE).forEach(truncated::add);
        return Collections.unmodifiableSet(truncated);
    }

    private CompactInvalidMappedField(String name, Map<DataType, TruncatedIndices> typesToIndices, boolean isPotentiallyUnmapped) {
        super(name, DataType.UNSUPPORTED, new TreeMap<>(), false, TimeSeriesFieldType.UNKNOWN);
        this.typesToIndices = typesToIndices;
        this.isPotentiallyUnmapped = isPotentiallyUnmapped;
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("CompactInvalidMappedField shouldn't be transported");
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "CompactInvalidMappedField";
    }

    @Override
    Map<String, Sample> samples() {
        Map<String, Sample> samples = new TreeMap<>();
        typesToIndices.forEach(
            (type, indices) -> samples.put(type.typeName(), new Sample(indices.kept(), indices.kept().size() + indices.dropped()))
        );
        return samples;
    }

    private Map<String, Set<String>> lazyTypesToIndices;

    @Override
    public Map<String, Set<String>> getTypesToIndices() {
        if (lazyTypesToIndices == null) {
            lazyTypesToIndices = new TreeMap<>();
            for (Map.Entry<DataType, TruncatedIndices> entry : typesToIndices.entrySet()) {
                TruncatedIndices indices = entry.getValue();
                Set<String> result = indices.dropped == 0 ? indices.kept : new LinkedHashSet<>(indices.kept);
                if (result != indices.kept) {
                    result.add("...");
                }
                lazyTypesToIndices.put(entry.getKey().typeName(), result);
            }
        }
        return lazyTypesToIndices;
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), typesToIndices, isPotentiallyUnmapped);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        CompactInvalidMappedField other = (CompactInvalidMappedField) obj;
        return isPotentiallyUnmapped == other.isPotentiallyUnmapped && Objects.equals(typesToIndices, other.typesToIndices);
    }

    private static Map<DataType, TruncatedIndices> truncatedAndDedup(
        Map<DataType, Set<String>> typesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return Maps.transformValues(typesToIndices, indices -> {
            // Use LinkedHashSet (vs. Set.copyOf) to keep the input's iteration order so the lazily-built error message renders the
            // same names in the same order the legacy InvalidMappedField path would have. Set equality is order-independent so dedup
            // still hits regardless of which insertion order won.
            Set<String> kept = indices.size() <= MAX_INDICES_PER_TYPE
                ? Collections.unmodifiableSet(new LinkedHashSet<>(indices))
                : truncate(indices);
            Set<String> deduped = indexDedupCache.computeIfAbsent(kept, unused -> kept);
            return new TruncatedIndices(deduped, Math.max(0, indices.size() - MAX_INDICES_PER_TYPE));
        });
    }
}
