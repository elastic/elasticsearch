/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Read-only partition map whose directory-constant keys live in one shared tuple and whose per-file keys live in a
 * small overlay. {@code get} checks the overlay, then the shared tuple. {@code entrySet} is Hive column order, then
 * {@link FileMetadataColumns} order. {@code equals} and {@code hashCode} match a flat map with those entries.
 * <p>
 * Not a {@link java.util.LinkedHashMap}, so {@code writeGenericMap} writes it as an unordered map — the same wire
 * type as the unmodifiable wrapper discovery already freezes. A copy in {@link FileSplit} would drop the sharing.
 */
final class LayeredPartitionMap extends AbstractMap<String, Object> {

    private final Map<String, Object> shared;
    private final Map<String, Object> overlay;
    private final List<Entry<String, Object>> entries;

    LayeredPartitionMap(Map<String, Object> shared, Map<String, Object> overlay) {
        this.shared = shared;
        this.overlay = overlay;
        this.entries = buildEntries(shared, overlay);
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
        return entries.size();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return new EntrySet();
    }

    private static List<Entry<String, Object>> buildEntries(Map<String, Object> shared, Map<String, Object> overlay) {
        List<Entry<String, Object>> built = new ArrayList<>(shared.size() + overlay.size());
        for (Entry<String, Object> entry : shared.entrySet()) {
            if (FileMetadataColumns.isFileMetadataColumn(entry.getKey()) == false) {
                built.add(new SimpleImmutableEntry<>(entry.getKey(), entry.getValue()));
            }
        }
        for (String name : FileMetadataColumns.NAMES) {
            if (name.equals(FileMetadataColumns.RECORD_REF)) {
                continue;
            }
            if (shared.containsKey(name)) {
                built.add(new SimpleImmutableEntry<>(name, shared.get(name)));
            } else if (overlay.containsKey(name)) {
                built.add(new SimpleImmutableEntry<>(name, overlay.get(name)));
            }
        }
        return List.copyOf(built);
    }

    private final class EntrySet extends AbstractSet<Entry<String, Object>> {
        @Override
        public Iterator<Entry<String, Object>> iterator() {
            return new Iterator<>() {
                private int index;

                @Override
                public boolean hasNext() {
                    return index < entries.size();
                }

                @Override
                public Entry<String, Object> next() {
                    if (hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    return entries.get(index++);
                }
            };
        }

        @Override
        public int size() {
            return entries.size();
        }
    }
}
