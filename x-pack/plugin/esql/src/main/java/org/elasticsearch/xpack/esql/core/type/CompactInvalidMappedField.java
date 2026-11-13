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
    private final Map<String, TruncatedIndices> unsupportedTypesToIndices;
    private final boolean isPotentiallyUnmapped;

    public static CompactInvalidMappedField mappedEverywhere(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return mappedEverywhere(name, typesToIndices, Map.of(), indexDedupCache);
    }

    /**
     * A field mapped in every index, including indices that map it to types ES|QL doesn't support. See
     * {@link TypeConflictedField#getUnsupportedTypesToIndices()}.
     */
    public static CompactInvalidMappedField mappedEverywhere(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<String, Set<String>> unsupportedTypesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return new CompactInvalidMappedField(
            name,
            truncatedAndDedup(typesToIndices, indexDedupCache),
            truncatedAndDedup(unsupportedTypesToIndices, indexDedupCache),
            false
        );
    }

    public static CompactInvalidMappedField potentiallyUnmapped(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return potentiallyUnmapped(name, typesToIndices, Map.of(), indexDedupCache);
    }

    public static CompactInvalidMappedField potentiallyUnmapped(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<String, Set<String>> unsupportedTypesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return new CompactInvalidMappedField(
            name,
            truncatedAndDedup(typesToIndices, indexDedupCache),
            truncatedAndDedup(unsupportedTypesToIndices, indexDedupCache),
            true
        );
    }

    private static Set<String> truncate(Set<String> indices) {
        Set<String> truncated = new LinkedHashSet<>(MAX_INDICES_PER_TYPE);
        indices.stream().sorted().limit(MAX_INDICES_PER_TYPE).forEach(truncated::add);
        return Collections.unmodifiableSet(truncated);
    }

    private CompactInvalidMappedField(
        String name,
        Map<DataType, TruncatedIndices> typesToIndices,
        Map<String, TruncatedIndices> unsupportedTypesToIndices,
        boolean isPotentiallyUnmapped
    ) {
        super(name, DataType.UNSUPPORTED, new TreeMap<>(), false, TimeSeriesFieldType.UNKNOWN);
        this.typesToIndices = typesToIndices;
        this.unsupportedTypesToIndices = unsupportedTypesToIndices;
        this.isPotentiallyUnmapped = isPotentiallyUnmapped;
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("CompactInvalidMappedField [" + getName() + "] shouldn't be transported");
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "CompactInvalidMappedField";
    }

    @Override
    Map<String, Sample> samples() {
        Map<String, Sample> samples = new TreeMap<>();
        typesToIndices.forEach((type, indices) -> samples.put(type.typeName(), toSample(indices)));
        return samples;
    }

    @Override
    Map<String, Sample> unsupportedSamples() {
        Map<String, Sample> samples = new TreeMap<>();
        unsupportedTypesToIndices.forEach((type, indices) -> samples.put(type, toSample(indices)));
        return samples;
    }

    private static Sample toSample(TruncatedIndices indices) {
        return new Sample(indices.kept(), indices.kept().size() + indices.dropped());
    }

    private Map<String, Set<String>> lazyTypesToIndices;
    private Map<String, Set<String>> lazyUnsupportedTypesToIndices;

    @Override
    public Map<String, Set<String>> getTypesToIndices() {
        if (lazyTypesToIndices == null) {
            lazyTypesToIndices = new TreeMap<>();
            typesToIndices.forEach((type, indices) -> lazyTypesToIndices.put(type.typeName(), withSentinel(indices)));
        }
        return lazyTypesToIndices;
    }

    @Override
    public Map<String, Set<String>> getUnsupportedTypesToIndices() {
        if (lazyUnsupportedTypesToIndices == null) {
            lazyUnsupportedTypesToIndices = new TreeMap<>();
            unsupportedTypesToIndices.forEach((type, indices) -> lazyUnsupportedTypesToIndices.put(type, withSentinel(indices)));
        }
        return lazyUnsupportedTypesToIndices;
    }

    private static Set<String> withSentinel(TruncatedIndices indices) {
        if (indices.dropped == 0) {
            return indices.kept;
        }
        Set<String> result = new LinkedHashSet<>(indices.kept);
        result.add("...");
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), typesToIndices, unsupportedTypesToIndices, isPotentiallyUnmapped);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        CompactInvalidMappedField other = (CompactInvalidMappedField) obj;
        return isPotentiallyUnmapped == other.isPotentiallyUnmapped
            && Objects.equals(typesToIndices, other.typesToIndices)
            && Objects.equals(unsupportedTypesToIndices, other.unsupportedTypesToIndices);
    }

    private static <K> Map<K, TruncatedIndices> truncatedAndDedup(
        Map<K, Set<String>> typesToIndices,
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
