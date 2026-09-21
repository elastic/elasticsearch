/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.document;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lucene.RamUsageEstimates;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Conservative upper-bound estimator for the retained heap of a {@link DocumentField}, including
 * its values, ignored values, and lookup fields. Adds the JDK container overhead that
 * {@link RamUsageEstimator#shallowSizeOf(Object)} misses so the fetch-phase circuit breaker never
 * under-counts. Traversal is bounded by {@link #MAX_ESTIMATE_DEPTH}.
 */
public final class DocumentFieldRamUsageEstimator {

    private static final long DOCUMENT_FIELD_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(DocumentField.class);

    private static final int MAX_ESTIMATE_DEPTH = 20;

    private static final long DEPTH_CAP_PENALTY_BYTES = 1L << 20;

    private static final int REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    private static final int ARRAY_HEADER_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
    private static final int OBJECT_HEADER_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;

    /**
     * Caches the result of {@link RamUsageEstimator#shallowSizeOfInstance(Class)} per class.
     * Lucene's {@code shallowSizeOfInstance} walks the class hierarchy with {@code getDeclaredFields()}
     * on every call (uncached). When {@code fields} or {@code stored_fields} return many
     * {@code Double}/{@code Boolean}/{@code BytesRef}/date values per hit across thousands of hits,
     * repeating that reflective walk would add measurable fetch-phase CPU overhead.
     */
    private static final ClassValue<Long> SHALLOW_SIZES = new ClassValue<>() {
        @Override
        protected Long computeValue(Class<?> type) {
            return RamUsageEstimator.shallowSizeOfInstance(type);
        }
    };

    private static final long HASH_MAP_ENTRY_BYTES;
    private static final long LINKED_HASH_MAP_ENTRY_BYTES;
    private static final long TREE_MAP_ENTRY_BYTES;
    private static final long LINKED_LIST_NODE_BYTES;

    static {
        HashMap<Integer, Integer> hm = new HashMap<>();
        hm.put(0, 0);
        HASH_MAP_ENTRY_BYTES = RamUsageEstimator.shallowSizeOf(hm.entrySet().iterator().next());

        LinkedHashMap<Integer, Integer> lhm = new LinkedHashMap<>();
        lhm.put(0, 0);
        LINKED_HASH_MAP_ENTRY_BYTES = RamUsageEstimator.shallowSizeOf(lhm.entrySet().iterator().next());

        TreeMap<Integer, Integer> tm = new TreeMap<>();
        tm.put(0, 0);
        TREE_MAP_ENTRY_BYTES = RamUsageEstimator.shallowSizeOf(tm.entrySet().iterator().next());

        LINKED_LIST_NODE_BYTES = RamUsageEstimator.alignObjectSize(OBJECT_HEADER_BYTES + 3L * REF_BYTES);
    }

    private DocumentFieldRamUsageEstimator() {}

    /**
     * Returns the shallow retained heap of a leaf value (one that is not a {@link Map},
     * {@link Collection}, or {@code Object[]}). Replicates the fast paths of
     * {@link RamUsageEstimator#sizeOfObject(Object)} but routes the final fallthrough through
     * {@link #SHALLOW_SIZES} to avoid repeated reflective class-hierarchy walks.
     */
    private static long sizeOfLeaf(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Accountable a) {
            return a.ramBytesUsed();
        }
        if (value instanceof String s) {
            return RamUsageEstimator.sizeOf(s);
        }
        if (value instanceof byte[] a) {
            return RamUsageEstimator.sizeOf(a);
        }
        if (value instanceof char[] a) {
            return RamUsageEstimator.sizeOf(a);
        }
        if (value instanceof double[] a) {
            return RamUsageEstimator.sizeOf(a);
        }
        if (value instanceof float[] a) {
            return RamUsageEstimator.sizeOf(a);
        }
        if (value instanceof int[] a) {
            return RamUsageEstimator.sizeOf(a);
        }
        if (value instanceof long[] a) {
            return RamUsageEstimator.sizeOf(a);
        }
        if (value instanceof short[] a) {
            return RamUsageEstimator.sizeOf(a);
        }
        if (value instanceof boolean[] a) {
            return RamUsageEstimator.sizeOf(a);
        }
        if (value instanceof Integer i) {
            return RamUsageEstimator.sizeOf(i);
        }
        if (value instanceof Long l) {
            return RamUsageEstimator.sizeOf(l);
        }
        if (value instanceof String[] a) {
            return RamUsageEstimator.sizeOf(a);
        }
        // For any other type (Double, Float, Boolean, BytesRef, ZonedDateTime, GeoPoint, …),
        // use the ClassValue cache to avoid repeated getDeclaredFields() walks.
        return SHALLOW_SIZES.get(value.getClass());
    }

    /**
     * Returns a conservative upper bound on the retained heap of {@code field}.
     */
    public static long estimate(DocumentField field) {
        return DOCUMENT_FIELD_SHALLOW_SIZE + RamUsageEstimator.sizeOf(field.getName()) + estimateContainer(field.getValues())
            + estimateContainer(field.getIgnoredValues()) + estimateContainer(field.getLookupFields());
    }

    private static long estimateContainer(Collection<?> collection) {
        if (collection == null) {
            return 0L;
        }
        return estimateCollection(collection, 0);
    }

    private static long estimateValue(Object value, int depth) {
        if (value == null) {
            return 0L;
        }
        if (depth >= MAX_ESTIMATE_DEPTH) {
            if (value instanceof Map<?, ?> || value instanceof Collection<?> || value instanceof Object[]) {
                return DEPTH_CAP_PENALTY_BYTES;
            }
            return sizeOfLeaf(value);
        }
        if (value instanceof Map<?, ?> map) {
            return estimateMap(map, depth);
        }
        if (value instanceof Collection<?> collection) {
            return estimateCollection(collection, depth);
        }
        if (value instanceof Object[] array) {
            return estimateObjectArray(array, depth);
        }
        return sizeOfLeaf(value);
    }

    private static long estimateMap(Map<?, ?> map, int depth) {
        long size = RamUsageEstimator.shallowSizeOf(map);
        int entries = map.size();
        if (entries == 0) {
            return size;
        }

        if (map instanceof HashMap<?, ?>) {
            size += ARRAY_HEADER_BYTES + hashTableCapacity(entries) * (long) REF_BYTES;
        }

        size += entries * perEntryBytesFor(map);

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            size += estimateValue(entry.getKey(), depth + 1);
            size += estimateValue(entry.getValue(), depth + 1);
        }
        return size;
    }

    private static long perEntryBytesFor(Map<?, ?> map) {
        if (map instanceof LinkedHashMap<?, ?>) {
            return LINKED_HASH_MAP_ENTRY_BYTES;
        }
        if (map instanceof HashMap<?, ?>) {
            return HASH_MAP_ENTRY_BYTES;
        }
        if (map instanceof TreeMap<?, ?>) {
            return TREE_MAP_ENTRY_BYTES;
        }
        return Math.max(LINKED_HASH_MAP_ENTRY_BYTES, TREE_MAP_ENTRY_BYTES);
    }

    private static long hashTableCapacity(int size) {
        long minSlots = Math.max(16L, (long) Math.ceil(size / 0.75f));
        long capacity = 16L;
        while (capacity < minSlots) {
            capacity <<= 1;
        }
        return capacity;
    }

    private static long estimateCollection(Collection<?> collection, int depth) {
        long size = RamUsageEstimator.shallowSizeOf(collection);
        int n = collection.size();

        if (collection instanceof LinkedList<?>) {
            size += n * LINKED_LIST_NODE_BYTES;
        } else if (collection instanceof ArrayList<?>) {
            // Over-approximate ArrayList backing array: JDK grows by ~50% so ceil(n * 1.5) is always >= actual
            long capacity = Math.max(10L, (long) Math.ceil(n * 1.5));
            size += ARRAY_HEADER_BYTES + capacity * REF_BYTES;
        } else if (collection instanceof ArrayDeque<?>) {
            long capacity = Math.max(16L, Long.highestOneBit((long) n * 2L - 1L) << 1);
            size += ARRAY_HEADER_BYTES + capacity * REF_BYTES;
        } else if (collection instanceof HashSet<?> || collection instanceof LinkedHashSet<?>) {
            size += collection instanceof LinkedHashSet<?>
                ? RamUsageEstimates.LINKED_HASH_MAP_SHALLOW_SIZE
                : RamUsageEstimates.HASH_MAP_SHALLOW_SIZE;
            size += ARRAY_HEADER_BYTES + hashTableCapacity(n) * (long) REF_BYTES;
            size += n * (collection instanceof LinkedHashSet<?> ? LINKED_HASH_MAP_ENTRY_BYTES : HASH_MAP_ENTRY_BYTES);
        } else if (collection instanceof TreeSet<?>) {
            size += RamUsageEstimates.TREE_MAP_SHALLOW_SIZE;
            size += n * TREE_MAP_ENTRY_BYTES;
        } else if (n > 0) {
            long capacity = Math.max(10L, (long) Math.ceil(n * 1.5));
            size += ARRAY_HEADER_BYTES + capacity * REF_BYTES;
        }

        for (Object o : collection) {
            size += estimateValue(o, depth + 1);
        }
        return size;
    }

    private static long estimateObjectArray(Object[] array, int depth) {
        long size = RamUsageEstimator.alignObjectSize(ARRAY_HEADER_BYTES + (long) array.length * REF_BYTES);
        for (Object o : array) {
            size += estimateValue(o, depth + 1);
        }
        return size;
    }
}
