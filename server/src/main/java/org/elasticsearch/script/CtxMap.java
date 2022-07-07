/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CtxMap extends AbstractMap<String, Object> {
    protected Metadata metadata;
    protected Map<String, Object> source;
    protected Set<Map.Entry<String, Object>> entrySet;

    public CtxMap(Metadata metadata, Map<String, Object> source) {
        this.metadata = metadata;
        this.source = source;
    }

    /**
     * Returns an entrySet that respects the validators of the map.
     */
    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        if (entrySet == null) {
            entrySet = new EntrySet(source.entrySet(), metadata.keys());
        }
        return entrySet;
    }

    /**
     * Associate a key with a value.  If the key has a validator, it is applied before association.
     * @throws IllegalArgumentException if value does not pass validation for the given key
     */
    @Override
    public Object put(String key, Object value) {
        if (metadata.isMetadata(key)) {
            return metadata.put(key, value);
        }
        return source.put(key, value);
    }

    /**
     * Remove the mapping of key.  If the key has a validator, it is checked before key removal.
     * @throws IllegalArgumentException if the validator does not allow the key to be removed
     */
    @Override
    public Object remove(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (metadata.isMetadata(key)) {
            return metadata.remove(key);
        }
        return source.remove(key);
    }

    /**
     * Clear entire map.  For each key in the map with a validator, that validator is checked as in {@link #remove(Object)}.
     * @throws IllegalArgumentException if any validator does not allow the key to be removed, in this case the map is unmodified
     */
    @Override
    public void clear() {
        // AbstractMap uses entrySet().clear(), it should be quicker to run through the metadata keys, then call the wrapped maps clear
        for (String key : metadata.keys()) {
            metadata.remove(key);
        }
        source.clear();
    }

    @Override
    public int size() {
        // uses map directly to avoid creating an EntrySet via AbstractMaps implementation, which returns entrySet().size()
        return source.size() + metadata.size();
    }

    @Override
    public boolean containsValue(Object value) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (source.containsValue(value)) {
            return true;
        }
        for (String key : metadata.keys()) {
            if (Objects.equals(value, metadata.get(key))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        return metadata.exists(key) || source.containsKey(key);
    }

    @Override
    public Object get(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (metadata.isMetadata(key)) {
            return metadata.get(key);
        }
        return source.get(key);
    }

    /**
     * Set of entries of the wrapped map that calls the appropriate validator before changing an entries value or removing an entry.
     *
     * Inherits {@link AbstractSet#removeAll(Collection)}, which calls the overridden {@link #remove(Object)} which performs validation.
     *
     * Inherits {@link AbstractCollection#retainAll(Collection)} and {@link AbstractCollection#clear()}, which both use
     * {@link EntrySetIterator#remove()} for removal.
     */
    class EntrySet extends AbstractSet<Map.Entry<String, Object>> {
        Set<Map.Entry<String, Object>> sourceSet;
        List<String> metadataKeys;

        EntrySet(Set<Map.Entry<String, Object>> sourceSet, List<String> metadataKeys) {
            this.sourceSet = sourceSet;
            this.metadataKeys = metadataKeys;
        }

        @Override
        public Iterator<Map.Entry<String, Object>> iterator() {
            return new EntrySetIterator(sourceSet.iterator(), metadataKeys.iterator());
        }

        @Override
        public int size() {
            return sourceSet.size() + metadataKeys.size();
        }

        @Override
        public boolean remove(Object o) {
            if (o instanceof Map.Entry<?, ?> entry) {
                if (entry.getKey()instanceof String key) {
                    if (metadata.isMetadata(key)) {
                        if (Objects.equals(entry.getValue(), metadata.get(key)) && metadata.exists(key)) {
                            metadata.remove(key);
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
            }
            return sourceSet.remove(o);
        }
    }

    /**
     * Iterator over the wrapped map that returns a validating {@link Entry} on {@link #next()} and validates on {@link #remove()}.
     *
     * {@link #remove()} is called by remove in {@link AbstractMap#values()}, {@link AbstractMap#keySet()}, {@link AbstractMap#clear()} via
     * {@link AbstractSet#clear()}
     */
    class EntrySetIterator implements Iterator<Map.Entry<String, Object>> {
        final Iterator<Map.Entry<String, Object>> sourceIter;
        final Iterator<String> metadataIter;

        boolean sourceCur = true;
        Map.Entry<String, Object> cur;

        EntrySetIterator(Iterator<Map.Entry<String, Object>> sourceIter, Iterator<String> metadataIter) {
            this.sourceIter = sourceIter;
            this.metadataIter = metadataIter;
        }

        @Override
        public boolean hasNext() {
            return sourceIter.hasNext() || metadataIter.hasNext();
        }

        @Override
        public Map.Entry<String, Object> next() {
            sourceCur = sourceIter.hasNext();
            return cur = (sourceCur ? sourceIter.next() : new MetadataEntry(metadataIter.next()));
        }

        /**
         * Remove current entry from the backing Map.  Checks the Entry's key's validator, if one exists, before removal.
         * @throws IllegalArgumentException if the validator does not allow the Entry to be removed
         * @throws IllegalStateException if remove is called before {@link #next()}
         */
        @Override
        public void remove() {
            if (cur == null) {
                throw new IllegalStateException();
            }
            if (sourceCur) {
                sourceIter.remove();
            } else {
                metadata.remove(cur.getKey());
            }
        }
    }

    class MetadataEntry implements Map.Entry<String, Object> {
        final String key;

        MetadataEntry(String key) {
            this.key = key;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            return metadata.get(key);
        }

        @Override
        public Object setValue(Object value) {
            return metadata.put(key, value);
        }
    }
}
