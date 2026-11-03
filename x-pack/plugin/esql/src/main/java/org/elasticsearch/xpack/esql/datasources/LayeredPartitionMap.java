/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Read-only partition map whose directory-constant keys live in one shared tuple and whose per-file keys live in a
 * small overlay. The two key sets are disjoint: {@code get} checks the overlay, then the shared tuple.
 * {@code entrySet} walks those maps in Hive column order, then {@link FileMetadataColumns} order, and does not copy
 * their entries. {@code equals} and {@code hashCode} match a flat map with those entries.
 * <p>
 * Not a {@link java.util.LinkedHashMap}, so {@code writeGenericMap} writes it as an unordered map — the same wire
 * type as the unmodifiable wrapper discovery already freezes. A copy in {@link FileSplit} would drop the sharing.
 */
final class LayeredPartitionMap extends AbstractMap<String, Object> {

    private final Map<String, Object> shared;
    private final Map<String, Object> overlay;
    private final int size;

    /** The interned directory tuple. Siblings that layer per-file keys over it return the same instance. */
    Map<String, Object> sharedTuple() {
        return shared;
    }

    LayeredPartitionMap(Map<String, Object> shared, Map<String, Object> overlay) {
        // Overlap would make get (overlay wins) disagree with entrySet (shared wins for metadata names).
        assert Collections.disjoint(shared.keySet(), overlay.keySet());
        this.shared = shared;
        this.overlay = overlay;
        this.size = countEntries(shared, overlay);
    }

    @Override
    public Object get(Object key) {
        if (overlay.containsKey(key)) {
            return overlay.get(key);
        }
        return shared.get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        return overlay.containsKey(key) || shared.containsKey(key);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return new EntrySet();
    }

    private static int countEntries(Map<String, Object> shared, Map<String, Object> overlay) {
        int count = 0;
        for (String key : shared.keySet()) {
            if (FileMetadataColumns.isFileMetadataColumn(key) == false) {
                count++;
            }
        }
        for (String name : FileMetadataColumns.NAMES) {
            if (name.equals(FileMetadataColumns.RECORD_REF)) {
                continue;
            }
            if (shared.containsKey(name) || overlay.containsKey(name)) {
                count++;
            }
        }
        return count;
    }

    /**
     * The entry object already owned by {@code map}. Scanning avoids a per-file copy of the shared Hive keys.
     */
    private static Entry<String, Object> liveEntry(Map<String, Object> map, String name) {
        if (map.containsKey(name) == false) {
            return null;
        }
        for (Entry<String, Object> entry : map.entrySet()) {
            if (name.equals(entry.getKey())) {
                return entry;
            }
        }
        return null;
    }

    private final class EntrySet extends AbstractSet<Entry<String, Object>> {
        @Override
        public Iterator<Entry<String, Object>> iterator() {
            return new EntryIterator();
        }

        @Override
        public int size() {
            return size;
        }
    }

    private final class EntryIterator implements Iterator<Entry<String, Object>> {
        private final Iterator<Entry<String, Object>> hive = shared.entrySet().iterator();
        private final Iterator<String> metadataNames = FileMetadataColumns.NAMES.iterator();
        private Entry<String, Object> nextHive;
        private Entry<String, Object> nextMetadata;
        private boolean metadataStarted;

        private EntryIterator() {
            nextHive = pullHive();
        }

        @Override
        public boolean hasNext() {
            if (nextHive != null) {
                return true;
            }
            if (metadataStarted == false) {
                nextMetadata = pullMetadata();
                metadataStarted = true;
            }
            return nextMetadata != null;
        }

        @Override
        public Entry<String, Object> next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            if (nextHive != null) {
                Entry<String, Object> entry = nextHive;
                nextHive = pullHive();
                return entry;
            }
            Entry<String, Object> entry = nextMetadata;
            nextMetadata = pullMetadata();
            return entry;
        }

        private Entry<String, Object> pullHive() {
            while (hive.hasNext()) {
                Entry<String, Object> entry = hive.next();
                if (FileMetadataColumns.isFileMetadataColumn(entry.getKey()) == false) {
                    return entry;
                }
            }
            return null;
        }

        private Entry<String, Object> pullMetadata() {
            while (metadataNames.hasNext()) {
                String name = metadataNames.next();
                if (name.equals(FileMetadataColumns.RECORD_REF)) {
                    continue;
                }
                Entry<String, Object> entry = liveEntry(shared, name);
                if (entry == null) {
                    entry = liveEntry(overlay, name);
                }
                if (entry != null) {
                    return entry;
                }
            }
            return null;
        }
    }
}
