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
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Memory-frugal counterpart to {@link InvalidMappedField}: stores at most {@value #MAX_INDICES_PER_TYPE} concrete index names per source
 * type alongside an integer count of the names that were dropped. Together they let {@link #errorMessage()} reconstruct the same
 * "and [N] other indices" rendering the full per-type list would have produced, without retaining the full list.
 */
public final class CompactInvalidMappedField extends TypeConflictedField {
    private static final int MAX_INDICES_PER_TYPE = 3;

    /**
     * Per source-type view of the indices retained for the error message: at most {@link #MAX_INDICES_PER_TYPE} sample names plus the
     * count of further names dropped during construction. Carrying {@code dropped} explicitly (rather than baking a {@code "..."} sentinel
     * into {@link #kept}) lets {@link #errorMessage()} be computed lazily and keeps {@link #kept} eligible for dedup across fields.
     */
    public record TruncatedIndices(Set<String> kept, int dropped) {
        public TruncatedIndices {
            assert kept.size() <= MAX_INDICES_PER_TYPE : "kept size " + kept.size() + " > " + MAX_INDICES_PER_TYPE;
            assert dropped >= 0 : "negative dropped count " + dropped;
        }
    }

    private final Map<DataType, TruncatedIndices> typesToIndices;
    private final boolean isPotentiallyUnmapped;

    /**
     * Lazily built on first {@link #errorMessage()} call from {@link #typesToIndices} and {@link #isPotentiallyUnmapped}; not part of
     * {@link #equals(Object)} / {@link #hashCode()}.
     */
    private String cachedErrorMessage;

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

    private static Set<String> truncate(Set<String> indices) {
        Set<String> truncated = new LinkedHashSet<>(MAX_INDICES_PER_TYPE);
        indices.stream().sorted().limit(MAX_INDICES_PER_TYPE).forEach(truncated::add);
        return Collections.unmodifiableSet(truncated);
    }

    private CompactInvalidMappedField(String name, Map<DataType, TruncatedIndices> typesToIndices, boolean isPotentiallyUnmapped) {
        super(name, DataType.UNSUPPORTED, new TreeMap<>(), false, TimeSeriesFieldType.UNKNOWN);
        this.typesToIndices = typesToIndices;
        this.isPotentiallyUnmapped = isPotentiallyUnmapped;
        this.cachedErrorMessage = null;
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
        if (cachedErrorMessage == null) {
            cachedErrorMessage = buildErrorMessage();
        }
        return cachedErrorMessage;
    }

    @Override
    public Map<String, Set<String>> getTypesToIndices() {
        Map<String, Set<String>> result = new TreeMap<>();
        typesToIndices.forEach((type, indices) -> result.put(type.typeName(), withSentinelIfTruncated(indices)));
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

    /**
     * Produces the same rendering {@link TypeConflictedField#makeErrorMessage} would have produced on the original (pre-truncation)
     * input. Inlined here rather than calling {@code makeErrorMessage} because that helper expects to derive the per-type total from the
     * set size, whereas our totals come from {@link TruncatedIndices#dropped} plus {@link TruncatedIndices#kept}.
     */
    private String buildErrorMessage() {
        StringBuilder sb = new StringBuilder();
        boolean prependInsistKeyword = isPotentiallyUnmapped && typesToIndices.containsKey(DataType.KEYWORD) == false;
        sb.append("mapped as [");
        sb.append(typesToIndices.size() + (prependInsistKeyword ? 1 : 0));
        sb.append("] incompatible types: ");
        boolean first = true;
        if (prependInsistKeyword) {
            first = false;
            sb.append("[keyword] due to loading from _source");
        }
        // Sort by type name to match the iteration order InvalidMappedField gets from its TreeMap<String, ...>.
        Map<String, TruncatedIndices> byTypeName = new TreeMap<>();
        typesToIndices.forEach((type, indices) -> byTypeName.put(type.typeName(), indices));
        for (Map.Entry<String, TruncatedIndices> entry : byTypeName.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append("[").append(entry.getKey()).append("] ");
            sb.append(
                entry.getKey().equals(DataType.KEYWORD.typeName()) && isPotentiallyUnmapped ? "due to loading from _source and in " : "in "
            );
            TruncatedIndices indices = entry.getValue();
            if (indices.dropped() == 0) {
                sb.append(indices.kept());
            } else {
                int total = indices.kept().size() + indices.dropped();
                sb.append(indices.kept().stream().sorted().collect(Collectors.toList()));
                sb.append(" and [").append(indices.dropped()).append("] other ");
                sb.append(total == 4 ? "index" : "indices");
            }
        }
        return sb.toString();
    }

    private static Set<String> withSentinelIfTruncated(TruncatedIndices indices) {
        if (indices.dropped() == 0) {
            return indices.kept();
        }
        Set<String> withSentinel = new LinkedHashSet<>(indices.kept());
        withSentinel.add("...");
        return Collections.unmodifiableSet(withSentinel);
    }
}
