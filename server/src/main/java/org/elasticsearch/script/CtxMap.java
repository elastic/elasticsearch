/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.common.util.set.Sets;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A scripting ctx map with metadata for write ingest contexts.  Delegates all metadata updates to metadata and
 * all other updates to source.  Implements the {@link Map} interface for backwards compatibility while performing
 * validation via {@link Metadata}.
 */
public class CtxMap<T extends Metadata> extends AbstractMap<String, Object> {
    protected static final String SOURCE = "_source";
    protected Map<String, Object> source;
    protected final T metadata;

    /**
     * Create CtxMap from a source and metadata
     *
     * @param source the source document map
     * @param metadata the metadata map
     */
    public CtxMap(Map<String, Object> source, T metadata) {
        this.source = source;
        this.metadata = metadata;
        Set<String> badKeys = Sets.intersection(this.metadata.keySet(), this.source.keySet());
        if (badKeys.size() > 0) {
            throw new IllegalArgumentException(
                "unexpected metadata ["
                    + badKeys.stream().sorted().map(k -> k + ":" + this.source.get(k)).collect(Collectors.joining(", "))
                    + "] in source"
            );
        }
    }

    /**
     * Does this access to the internal {@link #source} map occur directly via ctx? ie {@code ctx['myField']}.
     * Or does it occur via the {@link #SOURCE} key? ie {@code ctx['_source']['myField']}.
     *
     * Defaults to indirect, {@code ctx['_source']}
     */
    protected boolean directSourceAccess() {
        return false;
    }

    /**
     * get the source map, if externally modified then the guarantees of this class are not enforced
     */
    public final Map<String, Object> getSource() {
        return source;
    }

    /**
     * get the metadata map, if externally modified then the guarantees of this class are not enforced
     */
    public T getMetadata() {
        return metadata;
    }

    /**
     * Returns an entrySet that respects the validators of the map.
     */
    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        // Make a copy of the Metadata.keySet() to avoid a ConcurrentModificationException when removing a value from the iterator
        Set<Map.Entry<String, Object>> sourceEntries = directSourceAccess() ? source.entrySet() : Set.of(new IndirectSourceEntry());
        return new EntrySet(sourceEntries, new HashSet<>(metadata.keySet()));
    }

    /**
     * Associate a key with a value.  If the key has a validator, it is applied before association.
     * @throws IllegalArgumentException if value does not pass validation for the given key
     */
    @Override
    public Object put(String key, Object value) {
        if (metadata.isAvailable(key)) {
            return metadata.put(key, value);
        }
        if (directSourceAccess()) {
            return source.put(key, value);
        } else if (SOURCE.equals(key)) {
            return replaceSource(value);
        }
        throw new IllegalArgumentException("Cannot put key [" + key + "] with value [" + value + "] into ctx");
    }

    private Object replaceSource(Object value) {
        if (value instanceof Map == false) {
            throw new IllegalArgumentException(
                "Expected ["
                    + SOURCE
                    + "] to be a Map, not ["
                    + value
                    + "]"
                    + (value != null ? " with type [" + value.getClass().getName() + "]" : "")
            );

        }
        var oldSource = source;
        source = castSourceMap(value);
        return oldSource;
    }

    @SuppressWarnings({ "unchecked", "raw" })
    private static Map<String, Object> castSourceMap(Object value) {
        return (Map<String, Object>) value;
    }

    /**
     * Remove the mapping of key.  If the key has a validator, it is checked before key removal.
     * @throws IllegalArgumentException if the validator does not allow the key to be removed
     */
    @Override
    public Object remove(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (key instanceof String str) {
            if (metadata.isAvailable(str)) {
                return metadata.remove(str);
            }
        }
        if (directSourceAccess()) {
            return source.remove(key);
        } else {
            throw new UnsupportedOperationException("Cannot remove key " + key + " from ctx");
        }
    }

    /**
     * Clear entire map.  For each key in the map with a validator, that validator is checked as in {@link #remove(Object)}.
     * @throws IllegalArgumentException if any validator does not allow the key to be removed, in this case the map is unmodified
     */
    @Override
    public void clear() {
        // AbstractMap uses entrySet().clear(), it should be quicker to run through the validators, then call the wrapped maps clear
        for (String key : new ArrayList<>(metadata.keySet())) { // copy the key set to get around the ConcurrentModificationException
            metadata.remove(key);
        }
        // note: this is actually bogus in the general case, though! for this to work there must be some Metadata or subclass of Metadata
        // for which all the FieldPoperty properties of the metadata are nullable and therefore could have been removed in the previous
        // loop -- does such a class even exist? (that is, is there any *real* CtxMap for which the previous loop didn't throw?)
        source.clear();
    }

    @Override
    public int size() {
        // uses map directly to avoid creating an EntrySet via AbstractMaps implementation, which returns entrySet().size()
        final int sourceSize = directSourceAccess() ? source.size() : 1;
        return sourceSize + metadata.size();
    }

    @Override
    public boolean containsValue(Object value) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        return metadata.containsValue(value) || (directSourceAccess() ? source.containsValue(value) : source.equals(value));
    }

    @Override
    public boolean containsKey(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (key instanceof String str) {
            if (metadata.isAvailable(str)) {
                return metadata.containsKey(str);
            }
            return directSourceAccess() ? source.containsKey(key) : SOURCE.equals(key);
        }
        return false;
    }

    @Override
    public Object get(Object key) {
        // uses map directly to avoid AbstractMaps linear time implementation using entrySet()
        if (key instanceof String str) {
            if (metadata.isAvailable(str)) {
                return metadata.get(str);
            }
        }
        return directSourceAccess() ? source.get(key) : (SOURCE.equals(key) ? source : null);
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        // uses map directly to avoid Map's implementation that is just get and then containsKey and so could require two isAvailable calls
        if (key instanceof String str) {
            if (metadata.isAvailable(str)) {
                return metadata.getOrDefault(str, defaultValue);
            }
            return directSourceAccess() ? source.getOrDefault(key, defaultValue) : (SOURCE.equals(key) ? source : defaultValue);
        }
        return defaultValue;
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
        Set<String> metadataKeys;

        EntrySet(Set<Map.Entry<String, Object>> sourceSet, Set<String> metadataKeys) {
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
                if (entry.getKey() instanceof String key) {
                    if (metadata.containsKey(key)) {
                        if (Objects.equals(entry.getValue(), metadata.get(key))) {
                            metadata.remove(key);
                            return true;
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
        final Iterator<String> metadataKeyIter;

        boolean sourceCur = true;
        Map.Entry<String, Object> cur;

        EntrySetIterator(Iterator<Map.Entry<String, Object>> sourceIter, Iterator<String> metadataKeyIter) {
            this.sourceIter = sourceIter;
            this.metadataKeyIter = metadataKeyIter;
        }

        @Override
        public boolean hasNext() {
            return sourceIter.hasNext() || metadataKeyIter.hasNext();
        }

        @Override
        public Map.Entry<String, Object> next() {
            sourceCur = sourceIter.hasNext();
            return cur = sourceCur ? sourceIter.next() : new Entry(metadataKeyIter.next());
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
                try {
                    sourceIter.remove();
                } catch (UnsupportedOperationException e) {
                    // UnsupportedOperationException's message is "remove", rethrowing with more helpful message
                    throw new UnsupportedOperationException("Cannot remove key [" + cur.getKey() + "] from ctx");
                }

            } else {
                metadata.remove(cur.getKey());
            }
        }
    }

    private class IndirectSourceEntry implements Map.Entry<String, Object> {

        @Override
        public String getKey() {
            return SOURCE;
        }

        @Override
        public Object getValue() {
            return source;
        }

        @Override
        public Object setValue(Object value) {
            return replaceSource(value);
        }
    }

    /**
     * Map.Entry that stores metadata key and calls into {@link #metadata} for {@link #setValue}
     */
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
            return metadata.get(key);
        }

        @Override
        public Object setValue(Object value) {
            return metadata.put(key, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof CtxMap) == false) return false;
        if (super.equals(o) == false) return false;
        CtxMap<?> ctxMap = (CtxMap<?>) o;
        return source.equals(ctxMap.source) && metadata.equals(ctxMap.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, metadata);
    }
}
