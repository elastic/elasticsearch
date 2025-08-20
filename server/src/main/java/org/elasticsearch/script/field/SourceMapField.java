/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Provides an immutable view of a field in a source map.
 */
public class SourceMapField implements Field<Object> {
    protected String path;
    protected Supplier<Map<String, Object>> rootSupplier;

    protected Map<String, Object> container;
    protected String leaf;

    protected static final Object MISSING = new Object();

    public SourceMapField(String path, Supplier<Map<String, Object>> rootSupplier) {
        this.path = path;
        this.rootSupplier = rootSupplier;
        resolveDepthFlat();
    }

    // Path Read

    /**
     * Get the path represented by this Field
     */
    public String getName() {
        return path;
    }

    /**
     * Does the path exist?
     */
    public boolean exists() {
        return leaf != null && container.containsKey(leaf);
    }

    /**
     * Is this path associated with any values?
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * How many elements are at the leaf of this path?
     */
    @Override
    public int size() {
        if (leaf == null) {
            return 0;
        }

        Object value = container.getOrDefault(leaf, MISSING);
        if (value == MISSING) {
            return 0;
        }

        if (value instanceof List<?> list) {
            return list.size();
        }
        return 1;
    }

    /**
     * Iterate through all elements of this path with an iterator that cannot mutate the underlying map.
     */
    @Override
    public Iterator<Object> iterator() {
        if (leaf == null) {
            return Collections.emptyIterator();
        }

        Object value = container.getOrDefault(leaf, MISSING);
        if (value == MISSING) {
            return Collections.emptyIterator();
        }

        if (value instanceof List<?> list) {
            return getListIterator(list);
        }
        return Collections.singleton(value).iterator();
    }

    /**
     * Get an iterator for the given list that cannot mutate the underlying list. Subclasses can override this method to allow for
     * mutating iterators.
     * @param list the list to get an iterator for
     * @return an iterator that cannot mutate the underlying list
     */
    @SuppressWarnings("unchecked")
    protected Iterator<Object> getListIterator(List<?> list) {
        return (Iterator<Object>) Collections.unmodifiableList(list).iterator();
    }

    /**
     * Get the value at this path, if there is no value then get the provided {@param defaultValue}
     */
    public Object get(Object defaultValue) {
        if (leaf == null) {
            return defaultValue;
        }

        return container.getOrDefault(leaf, defaultValue);
    }

    /**
     * Get the value at the given index at this path or {@param defaultValue} if there is no such value.
     */
    public Object get(int index, Object defaultValue) {
        if (leaf == null) {
            return defaultValue;
        }

        Object value = container.getOrDefault(leaf, MISSING);
        if (value instanceof List<?> list) {
            if (index < list.size()) {
                return list.get(index);
            }
        } else if (value != MISSING && index == 0) {
            return value;
        }

        return defaultValue;
    }

    /**
     * Is there any value matching {@param predicate} at this path?
     */
    public boolean hasValue(Predicate<Object> predicate) {
        if (leaf == null) {
            return false;
        }

        Object value = container.getOrDefault(leaf, MISSING);
        if (value == MISSING) {
            return false;
        }

        if (value instanceof List<?> list) {
            return list.stream().anyMatch(predicate);
        }

        return predicate.test(value);
    }

    /**
     * Change the path and clear the existing resolution by setting {@link #leaf} and {@link #container} to null.
     * Caller needs to re-resolve after this call.
     */
    protected void setPath(String path) {
        this.path = path;
        this.leaf = null;
        this.container = null;
    }

    /**
     * Resolve {@link #path} from the root.
     *
     * Tries to resolve the path one segment at a time, if the segment is not mapped to a Java Map, then
     * treats that segment and the rest as the leaf if it resolves.
     *
     * a.b.c could be resolved as
     *  I)   ['a']['b']['c'] if 'a' is a Map at the root and 'b' is a Map in 'a', 'c' need not exist in 'b'.
     *  II)  ['a']['b.c'] if 'a' is a Map at the root and 'b' does not exist in 'a's Map but 'b.c' does.
     *  III) ['a.b.c'] if 'a' doesn't exist at the root but 'a.b.c' does.
     *
     * {@link #container} and {@link #leaf} and non-null if resolved.
     */
    @SuppressWarnings("unchecked")
    protected final void resolveDepthFlat() {
        container = rootSupplier.get();

        int index = path.indexOf('.');
        int lastIndex = 0;
        String segment;

        while (index != -1) {
            segment = path.substring(lastIndex, index);
            Object value = container.get(segment);
            if (value instanceof Map<?, ?> map) {
                container = (Map<String, Object>) map;
                lastIndex = index + 1;
                index = path.indexOf('.', lastIndex);
            } else {
                // Check rest of segments as a single key
                String rest = path.substring(lastIndex);
                if (container.containsKey(rest)) {
                    leaf = rest;
                } else {
                    leaf = null;
                }
                return;
            }
        }
        leaf = path.substring(lastIndex);
    }
}
