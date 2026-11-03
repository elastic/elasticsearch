/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Names and value lists for {@link MetricsInfoOperator} and {@link TsInfoOperator}, charged to the breaker.
 * <p>
 * Metric names, index abstraction names, dimension names and metadata lists repeat across many entries of these
 * operators. Each distinct string and each distinct shared list is kept once and charged once. Shared lists are
 * immutable, so entries replace them rather than mutate them.
 * <p>
 * Lists that keep growing, like dimension keys merged from many documents, come from {@link #unsharedUnion} instead.
 * Sharing them would keep every intermediate version alive. All lists are sorted and hold canonical strings.
 * <p>
 * Not thread-safe. Owned by one operator, which closes it to release the bytes charged here.
 */
final class MetadataValues implements Releasable {

    /**
     * Estimated bytes per {@link LinkedHashMap} entry, also used for {@link HashMap} entries. The node holds a hash
     * and five references. A hash map keeps its load between 0.375 and 0.75, so each entry also uses about two table slots.
     */
    static final long MAP_ENTRY_BYTES = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES + 5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
    ) + 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;

    /** Shallow size of the immutable list class that {@link List#copyOf} returns for three or more elements. */
    private static final long LIST_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(List.of("a", "b", "c").getClass());

    private final CircuitBreaker breaker;
    /** Names the owning operator in breaker messages. */
    private final String breakerLabel;
    private final Map<String, String> strings = new HashMap<>();
    private final Map<List<String>, List<String>> sharedLists = new HashMap<>();
    private long trackedBytes;

    MetadataValues(CircuitBreaker breaker, String breakerLabel) {
        this.breaker = breaker;
        this.breakerLabel = breakerLabel;
    }

    /** Returns the canonical instance of {@code value}. */
    String string(String value) {
        String canonical = strings.get(value);
        if (canonical == null) {
            charge(MAP_ENTRY_BYTES + RamUsageEstimator.sizeOf(value));
            strings.put(value, value);
            canonical = value;
        }
        return canonical;
    }

    /** Returns the shared list of {@code values}, which are sorted and distinct by type. */
    List<String> sharedList(SortedSet<String> values) {
        if (values.isEmpty()) {
            return List.of();
        }
        List<String> shared = sharedLists.get(List.copyOf(values));
        if (shared == null) {
            charge(MAP_ENTRY_BYTES + listBytes(values.size()));
            shared = canonicalCopy(values);
            sharedLists.put(shared, shared);
        }
        return shared;
    }

    /** Returns the shared list of {@code current} plus {@code value}. A null value adds nothing. */
    List<String> sharedWithValue(List<String> current, String value) {
        if (value == null || Collections.binarySearch(current, value) >= 0) {
            return current;
        }
        SortedSet<String> merged = new TreeSet<>(current);
        merged.add(value);
        return sharedList(merged);
    }

    /** Returns the shared union of two shared lists. */
    List<String> sharedUnion(List<String> current, List<String> added) {
        if (current == added || added.isEmpty()) {
            return current;
        }
        if (current.isEmpty()) {
            return added;
        }
        SortedSet<String> merged = new TreeSet<>(current);
        return merged.addAll(added) ? sharedList(merged) : current;
    }

    /**
     * Returns {@code current} plus {@code added} as a list owned by one entry, or {@code current} if nothing is new.
     * When {@code current} was itself owned, its bytes are released, because the entry drops it.
     */
    List<String> unsharedUnion(List<String> current, Collection<String> added) {
        if (current == added) {
            return current;
        }
        SortedSet<String> missing = null;
        for (String value : added) {
            if (Collections.binarySearch(current, value) < 0) {
                if (missing == null) {
                    missing = new TreeSet<>();
                }
                missing.add(value);
            }
        }
        if (missing == null) {
            return current;
        }
        charge(listBytes(current.size() + missing.size()));
        SortedSet<String> merged = new TreeSet<>(current);
        for (String value : missing) {
            merged.add(string(value));
        }
        if (current.isEmpty() == false && sharedLists.get(current) != current) {
            release(listBytes(current.size()));
        }
        return List.copyOf(merged);
    }

    private List<String> canonicalCopy(SortedSet<String> values) {
        String[] canonical = new String[values.size()];
        int i = 0;
        for (String value : values) {
            canonical[i++] = string(value);
        }
        return List.of(canonical);
    }

    /** Bytes of an immutable list of {@code size} references, excluding the strings, which are charged once. */
    private static long listBytes(int size) {
        return LIST_SHALLOW_SIZE + RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) size * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
    }

    private void charge(long bytes) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, breakerLabel);
        trackedBytes += bytes;
    }

    private void release(long bytes) {
        breaker.addWithoutBreaking(-bytes);
        trackedBytes -= bytes;
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-trackedBytes);
        trackedBytes = 0;
    }
}
