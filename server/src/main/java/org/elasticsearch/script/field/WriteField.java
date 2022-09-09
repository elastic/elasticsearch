/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.util.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class WriteField implements Field<Object> {
    protected String path;
    protected final Supplier<Map<String, Object>> rootSupplier;

    protected Map<String, Object> container;
    protected String leaf;

    private static final Object MISSING = new Object();

    public WriteField(String path, Supplier<Map<String, Object>> rootSupplier) {
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

    // Path Update

    /**
     * Move this path to another path in the map.
     *
     * @throws IllegalArgumentException if the other path has contents
     */
    public WriteField move(String path) {
        WriteField dest = new WriteField(path, rootSupplier);
        if (dest.isEmpty() == false) {
            throw new IllegalArgumentException("Cannot move to non-empty destination [" + path + "]");
        }
        return overwrite(path);
    }

    /**
     * Move this path to another path in the map, overwriting the destination path if it exists.
     *
     * If this Field has no value, the value at {@param path} is removed.
     */
    public WriteField overwrite(String path) {
        Object value = get(MISSING);
        remove();
        setPath(path);
        if (value == MISSING) {
            // The source has a missing value, remove the value, if it exists, at the destination
            // to match the missing value at the source.
            remove();
        } else {
            setLeaf();
            set(value);
        }
        return this;
    }

    // Path Delete

    /**
     * Removes this path from the map.
     */
    public void remove() {
        resolveDepthFlat();
        if (leaf == null) {
            return;
        }
        container.remove(leaf);
    }

    // Value Create

    /**
     * Sets the value for this path.  Creates nested path if necessary.
     */
    public WriteField set(Object value) {
        setLeaf();
        container.put(leaf, value);
        return this;
    }

    /**
     * Appends a value to this path.  Creates the path and the List at the leaf if necessary.
     */
    @SuppressWarnings("unchecked")
    public WriteField append(Object value) {
        setLeaf();

        container.compute(leaf, (k, v) -> {
            List<Object> values;
            if (v == null) {
                values = new ArrayList<>(4);
            } else if (v instanceof List<?> list) {
                values = (List<Object>) list;
            } else {
                values = new ArrayList<>(4);
                values.add(v);
            }
            values.add(value);
            return values;
        });
        return this;
    }

    // Value Read

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
     * Iterate through all elements of this path
     */
    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Object> iterator() {
        if (leaf == null) {
            return Collections.emptyIterator();
        }

        Object value = container.getOrDefault(leaf, MISSING);
        if (value == MISSING) {
            return Collections.emptyIterator();
        }

        if (value instanceof List<?> list) {
            return (Iterator<Object>) list.iterator();
        }
        return Collections.singleton(value).iterator();
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

    // Value Update

    /**
     * Update each value at this path with the {@param transformer} {@link Function}.
     */
    @SuppressWarnings("unchecked")
    public WriteField transform(Function<Object, Object> transformer) {
        if (leaf == null) {
            return this;
        }

        Object value = container.getOrDefault(leaf, MISSING);
        if (value == MISSING) {
            return this;
        }

        if (value instanceof List<?> list) {
            ((List<Object>) list).replaceAll(transformer::apply);
        } else {
            container.put(leaf, transformer.apply(value));
        }

        return this;
    }

    // Value Delete

    /**
     * Remove all duplicate values from this path.  List order is not preserved.
     */
    @SuppressWarnings("unchecked")
    public WriteField deduplicate() {
        if (leaf == null) {
            return this;
        }

        Object value = container.getOrDefault(leaf, MISSING);
        if (value == MISSING) {
            return this;
        }

        if (value instanceof List<?> list) {
            // Assume modifiable list
            Set<Object> set = new HashSet<>(list);
            list.clear();
            ((List<Object>) list).addAll(set);
        }

        return this;
    }

    /**
     * Remove all values at this path that match {@param filter}.  If there is only one value and it matches {@param filter},
     * the mapping is removed, however empty Lists are retained.
     */
    public WriteField removeValuesIf(Predicate<Object> filter) {
        if (leaf == null) {
            return this;
        }

        Object value = container.getOrDefault(leaf, MISSING);
        if (value == MISSING) {
            return this;
        }

        if (value instanceof List<?> list) {
            list.removeIf(filter);
        } else if (filter.test(value)) {
            container.remove(leaf);
        }

        return this;
    }

    /**
     * Remove the value at {@param index}, if it exists.  If there is only one value and {@param index} is zero, remove the
     * mapping.
     */
    public WriteField removeValue(int index) {
        if (leaf == null) {
            return this;
        }

        Object value = container.getOrDefault(leaf, MISSING);
        if (value == MISSING) {
            return this;
        }

        if (value instanceof List<?> list) {
            if (index < list.size()) {
                list.remove(index);
            }
        } else if (index == 0) {
            container.remove(leaf);
        }

        return this;
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
     * Get the path to a leaf or create it if one does not exist.
     */
    protected void setLeaf() {
        if (leaf == null) {
            resolveDepthFlat();
        }
        if (leaf == null) {
            createDepth();
        }
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
    protected void resolveDepthFlat() {
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

    /**
     * Create a new Map for each segment in path, if that segment is unmapped or mapped to null.
     *
     * @throws IllegalArgumentException if a non-leaf segment maps to a non-Map Object.
     */
    @SuppressWarnings("unchecked")
    protected void createDepth() {
        container = rootSupplier.get();

        String[] segments = path.split("\\.");
        for (int i = 0; i < segments.length - 1; i++) {
            String segment = segments[i];
            Object value = container.get(segment);
            if (value instanceof Map<?, ?> map) {
                container = (Map<String, Object>) map;
            } else if (value == null) {
                Map<String, Object> next = Maps.newHashMapWithExpectedSize(4);
                container.put(segment, next);
                container = next;
            } else {
                throw new IllegalArgumentException(
                    "Segment [" + i + ":'" + segment + "'] has value [" + value + "] of type [" + value.getClass().getName() + "]"
                );
            }
        }
        leaf = segments[segments.length - 1];
    }
}
