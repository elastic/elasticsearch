/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

// FIXME(gal, NOCOMMIT) Go over this javadocs
// FIXME(gal, NOCOMMIT) Reduce duplication with InvalidMappedField
/**
 * Memory-frugal variant of {@link InvalidMappedField}: stores at most {@value #MAX_INDICES_PER_TYPE} concrete index names per source type
 * (plus the {@value #ELLIPSIS} sentinel when more existed) instead of the full per-type index list. Wide union-typed fields routinely span
 * thousands of indices but the only consumers that need the full list are the legacy index-keyed conversion structures, and they aren't
 * used on transport versions that support {@link CompactMultiTypeEsField}. Truncating here lets the analyzed plan stay small while still
 * producing a good "[a, b, c, ...]" error message: the message itself is rendered from the full input map at construction time and then
 * stored as a string, so we lose only the post-construction ability to enumerate every index.
 *
 * <p>Just like its parent, the {@code typesToIndices} map is not sent over the wire (it only matters during analysis on the coordinator),
 * so {@code InvalidMappedField2} serializes identically to {@link InvalidMappedField}. Round-tripping through the wire therefore yields a
 * plain {@link InvalidMappedField} on the receiving side &mdash; that's fine because {@code typesToIndices} is empty after deserialization
 * anyway, so the truncation no longer matters.
 */
public class CompactInvalidMappedField extends InvalidMappedField {
    private static final String ELLIPSIS = "...";
    private static final int MAX_INDICES_PER_TYPE = 3;

    public CompactInvalidMappedField(String name, Map<String, Set<String>> typesToIndices) {
        super(name, makeErrorMessage(typesToIndices, false), new TreeMap<>(), truncate(typesToIndices), false, TimeSeriesFieldType.UNKNOWN);
    }

    public static CompactInvalidMappedField potentiallyUnmapped(String name, Map<String, Set<String>> typesToIndices) {
        return new CompactInvalidMappedField(
            name,
            makeErrorMessage(typesToIndices, true),
            new TreeMap<>(),
            truncate(typesToIndices),
            true,
            TimeSeriesFieldType.UNKNOWN
        );
    }

    private CompactInvalidMappedField(
        String name,
        String errorMessage,
        Map<String, EsField> properties,
        Map<String, Set<String>> typesToIndices,
        boolean isPotentiallyUnmapped,
        TimeSeriesFieldType type
    ) {
        super(name, errorMessage, properties, typesToIndices, isPotentiallyUnmapped, type);
    }

    /**
     * Cap each per-type index set at {@value #MAX_INDICES_PER_TYPE} entries, appending the {@value #ELLIPSIS} sentinel iff anything was
     * dropped. The retained 3 are picked by sorted order so that the (already truncated) error message and the stored set stay
     * consistent.
     */
    private static Map<String, Set<String>> truncate(Map<String, Set<String>> typesToIndices) {
        Map<String, Set<String>> result = new TreeMap<>();
        for (Map.Entry<String, Set<String>> entry : typesToIndices.entrySet()) {
            Set<String> indices = entry.getValue();
            if (indices.size() <= MAX_INDICES_PER_TYPE) {
                result.put(entry.getKey(), Set.copyOf(indices));
            } else {
                Set<String> truncated = new LinkedHashSet<>(MAX_INDICES_PER_TYPE + 1);
                indices.stream().sorted().limit(MAX_INDICES_PER_TYPE).forEach(truncated::add);
                truncated.add(ELLIPSIS);
                result.put(entry.getKey(), Set.copyOf(truncated));
            }
        }
        return result;
    }
}
