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

/**
 * Implements a Map interface, including {@link Map.Entry} and {@link Iterator<Map.Entry>} for classes that
 * implement {@link MapWrappable}.
 */
class MapWrapper extends AbstractMap<String, Object> {
    private EntrySet entrySet; // cache to avoid recreation
    protected final MapWrappable wrapped;

    MapWrapper(MapWrappable wrapped) {
        this.wrapped = wrapped;
    }

    /**
     * Returns an entrySet that respects the validators of the map.
     */
    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        if (entrySet == null) {
            entrySet = new EntrySet(wrapped.keys());
        }
        return entrySet;
    }

    /**
     * Associate a key with a value.  If the key has a validator, it is applied before association.
     * @throws IllegalArgumentException if value does not pass validation for the given key
     */
    @Override
    public Object put(String key, Object value) {
        return wrapped.put(key, value);
    }

    /**
     * Remove the mapping of key.  If the key has a validator, it is checked before key removal.
     * @throws IllegalArgumentException if the validator does not allow the key to be removed
     */
    @Override
    public Object remove(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (key instanceof String str) {
            return wrapped.remove(str);
        }
        return null;
    }

    /**
     * Clear entire map.  For each key in the map with a validator, that validator is checked as in {@link #remove(Object)}.
     * @throws IllegalArgumentException if any validator does not allow the key to be removed, in this case the map is may have been
     *                                  modified
     */
    @Override
    public void clear() {
        // AbstractMap uses entrySet().clear(), it should be quicker to run through the metadata keys, then call the wrapped maps clear
        for (String key : wrapped.keys()) {
            wrapped.remove(key);
        }
    }

    @Override
    public int size() {
        // uses map directly to avoid creating an EntrySet via AbstractMaps implementation, which returns entrySet().size()
        return wrapped.size();
    }

    @Override
    public boolean containsValue(Object value) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        return wrapped.containsValue(value);
    }

    @Override
    public boolean containsKey(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (key instanceof String str) {
            return wrapped.containsKey(str);
        }
        return false;
    }

    @Override
    public Object get(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (key instanceof String str) {
            return wrapped.get(str);
        }
        return null;
    }

    /**
     * Set of entries of the wrapped map that calls the appropriate validator before changing an entries value or removing an entry.
     *
     * Inherits {@link AbstractSet#removeAll(Collection)}, which calls the overridden {@link #remove(Object)} which performs validation.
     *
     * Inherits {@link AbstractCollection#retainAll(Collection)} and {@link AbstractCollection#clear()}, which both use
     * {@link CtxMap.EntrySetIterator#remove()} for removal.
     */
    class EntrySet extends AbstractSet<Map.Entry<String, Object>> {
        List<String> wrappedKeys;

        EntrySet(List<String> wrappedKeys) {
            this.wrappedKeys = wrappedKeys;
        }

        @Override
        public Iterator<Map.Entry<String, Object>> iterator() {
            return new EntrySetIterator(wrappedKeys.iterator());
        }

        @Override
        public int size() {
            return wrappedKeys.size();
        }

        @Override
        public boolean remove(Object o) {
            if (o instanceof Map.Entry<?, ?> entry) {
                if (entry.getKey()instanceof String key) {
                    if (wrapped.containsKey(key)) {
                        if (Objects.equals(entry.getValue(), wrapped.get(key))) {
                            wrapped.remove(key);
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }

    /**
     * Iterator over the wrapped map that returns a validating {@link Entry} on {@link #next()} and validates on {@link #remove()}.
     *
     * {@link #remove()} is called by remove in {@link AbstractMap#values()}, {@link AbstractMap#keySet()}, {@link AbstractMap#clear()} via
     * {@link AbstractSet#clear()}
     */
    class EntrySetIterator implements Iterator<Map.Entry<String, Object>> {
        final Iterator<String> wrappedIter;
        Map.Entry<String, Object> cur;

        EntrySetIterator(Iterator<String> wrappedIter) {
            this.wrappedIter = wrappedIter;
        }

        @Override
        public boolean hasNext() {
            return wrappedIter.hasNext();
        }

        @Override
        public Map.Entry<String, Object> next() {
            return cur = new Entry(wrappedIter.next());
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
            wrapped.remove(cur.getKey());
        }
    }

    class Entry implements Map.Entry<String, Object> {
        final String key;

        Entry(String key) {
            this.key = key;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            return wrapped.get(key);
        }

        @Override
        public Object setValue(Object value) {
            return wrapped.put(key, value);
        }
    }
}
