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
    private final Map<String, TruncatedIndices> unsupportedOriginalTypes;
    private final boolean isPotentiallyUnmapped;

    public static CompactInvalidMappedField mappedEverywhere(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        return mappedEverywhere(name, typesToIndices, Map.of(), indexDedupCache, timeSeriesFieldType);
    }

    /**
     * @param unsupportedOriginalTypes the types ES|QL doesn't support that are in {@code typesToIndices} as {@link DataType#UNSUPPORTED},
     *                                 each with its indices: see {@link #unsupportedOriginalTypesToIndices()}
     */
    public static CompactInvalidMappedField mappedEverywhere(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<String, Set<String>> unsupportedOriginalTypes,
        Map<Set<String>, Set<String>> indexDedupCache,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        return new CompactInvalidMappedField(
            name,
            truncatedAndDedup(typesToIndices, indexDedupCache),
            truncatedAndDedup(unsupportedOriginalTypes, indexDedupCache),
            false,
            timeSeriesFieldType
        );
    }

    public static CompactInvalidMappedField potentiallyUnmapped(
        String name,
        Map<DataType, Set<String>> typesToIndices,
        Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return new CompactInvalidMappedField(
            name,
            truncatedAndDedup(typesToIndices, indexDedupCache),
            Map.of(),
            true,
            TimeSeriesFieldType.UNKNOWN
        );
    }

    /**
     * This field, also unmapped in some indices. It keeps the counts of the indices of each type, which rebuilding it from
     * {@link #getTypesToIndices()} would lose: those are already truncated, with a {@code "..."} in place of the dropped indices.
     */
    public CompactInvalidMappedField asPotentiallyUnmapped() {
        return new CompactInvalidMappedField(getName(), typesToIndices, unsupportedOriginalTypes, true, TimeSeriesFieldType.UNKNOWN);
    }

    private static Set<String> truncate(Set<String> indices) {
        Set<String> truncated = new LinkedHashSet<>(MAX_INDICES_PER_TYPE);
        indices.stream().sorted().limit(MAX_INDICES_PER_TYPE).forEach(truncated::add);
        return Collections.unmodifiableSet(truncated);
    }

    private CompactInvalidMappedField(
        String name,
        Map<DataType, TruncatedIndices> typesToIndices,
        Map<String, TruncatedIndices> unsupportedOriginalTypes,
        boolean isPotentiallyUnmapped,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        super(name, DataType.UNSUPPORTED, new TreeMap<>(), false, timeSeriesFieldType);
        this.typesToIndices = typesToIndices;
        this.unsupportedOriginalTypes = unsupportedOriginalTypes;
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
        typesToIndices.forEach((type, indices) -> {
            if (type == DataType.UNSUPPORTED && unsupportedOriginalTypes.isEmpty() == false) {
                // The messages name the types that are unsupported, not the fact that they are.
                unsupportedOriginalTypes.forEach((original, originalIndices) -> samples.put(original, sample(originalIndices)));
            } else {
                samples.put(type.typeName(), sample(indices));
            }
        });
        return samples;
    }

    private static Sample sample(TruncatedIndices indices) {
        return new Sample(indices.kept(), indices.kept().size() + indices.dropped());
    }

    private volatile Map<String, Set<String>> lazyTypesToIndices;

    @Override
    public Map<String, Set<String>> getTypesToIndices() {
        Map<String, Set<String>> result = lazyTypesToIndices;
        if (result == null) {
            // Fill a local and publish it once: this field is shared by every thread that holds the plan.
            result = new TreeMap<>();
            for (Map.Entry<DataType, TruncatedIndices> entry : typesToIndices.entrySet()) {
                result.put(entry.getKey().typeName(), withDroppedSentinel(entry.getValue()));
            }
            lazyTypesToIndices = result;
        }
        return result;
    }

    @Override
    public Map<String, Set<String>> unsupportedOriginalTypesToIndices() {
        Map<String, Set<String>> result = new TreeMap<>();
        unsupportedOriginalTypes.forEach((original, indices) -> result.put(original, withDroppedSentinel(indices)));
        return result;
    }

    private static Set<String> withDroppedSentinel(TruncatedIndices indices) {
        if (indices.dropped == 0) {
            return indices.kept;
        }
        Set<String> names = new LinkedHashSet<>(indices.kept);
        names.add("...");
        return names;
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
        return Objects.hash(super.hashCode(), typesToIndices, unsupportedOriginalTypes, isPotentiallyUnmapped);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        CompactInvalidMappedField other = (CompactInvalidMappedField) obj;
        return isPotentiallyUnmapped == other.isPotentiallyUnmapped
            && Objects.equals(typesToIndices, other.typesToIndices)
            && Objects.equals(unsupportedOriginalTypes, other.unsupportedOriginalTypes);
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
