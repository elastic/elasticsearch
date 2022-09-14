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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class WriteField implements Field<Object> {
    protected String path;
    protected Supplier<Map<String, Object>> rootSupplier;

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
     * Move this path to the path of the given {@code WriteField}, using that {@code WriteField}s root,
     * which may be a {@link NestedDocument}.
     *
     * @throws IllegalArgumentException if the other path has contents
     */
    public WriteField move(WriteField path) {
        if (path.isEmpty() == false) {
            throw new IllegalArgumentException("Cannot move to non-empty destination [" + path.path + "]");
        }
        return overwrite(path);
    }

    /**
     * The painless API for {@code move}, delegates to {@link #move(String)} or {@link #move(WriteField)},
     * throws an {@link IllegalArgumentException} if {@param path} is neither a {@link String} nor a {@link WriteField}.
     *
     * This is necessary because Painless does not support method overloading, only arity overloading.
     */
    public WriteField move(Object path) {
        if (path instanceof String str) {
            return move(str);
        } else if (path instanceof WriteField field) {
            return move(field);
        }

        throw new IllegalArgumentException(
            "Cannot call move with ["
                + path
                + "], must be String or WriteField, not ["
                + ((path != null) ? path.getClass().getName() : "null")
                + "]"
        );
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

    /**
     * Move this path to the path represented by another {@code WriteField}, using that {@code WriteField}s root,
     * which may be a {@link NestedDocument}.  Overwrites the destination path if it exists.
     *
     * If this Field has no value, the value at {@param path} is removed.
     */
    public WriteField overwrite(WriteField path) {
        Object value = get(MISSING);
        remove();
        setPath(path.path);
        this.rootSupplier = path.rootSupplier;
        if (value == MISSING) {
            // The source has a missing value, remove the value, if it exists, at the destination
            // to match the missing value at the source.
            remove();
        } else {
            setLeaf();
            set(value);
        }
        // refresh argument field
        path.resolveDepthFlat();
        return this;
    }

    /**
     * The painless API for {@code overwrite}, delegates to {@link #overwrite(String)} or {@link #overwrite(WriteField)},
     * throws an {@link IllegalArgumentException} if {@param path} is neither a {@link String} nor a {@link WriteField}.
     *
     * This is necessary because Painless does not support method overloading, only arity overloading.
     */
    public WriteField overwrite(Object path) {
        if (path instanceof String str) {
            return overwrite(str);
        } else if (path instanceof WriteField field) {
            return overwrite(field);
        }

        throw new IllegalArgumentException(
            "Cannot call overwrite with ["
                + path
                + "], must be String or WriteField, not ["
                + ((path != null) ? path.getClass().getName() : "null")
                + "]"
        );
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
        if (value instanceof NestedDocument doc) {
            throw new IllegalArgumentException("cannot set NestedDocument [" + doc.getDoc() + "] as path [" + path + "]");
        }
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
            if (value instanceof NestedDocument doc) {
                throw new IllegalArgumentException("cannot append NestedDocument [" + doc.getDoc() + "] to path [" + path + "]");
            } else {
                values.add(value);
            }
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
     * Removes the {@param o} if this WriteField contains {@param o} using reference equality.
     */
    void remove(Object o) {
        Object value = container.getOrDefault(leaf, MISSING);
        if (value == MISSING) {
            return;
        }

        if (value instanceof List<?> list) {
            Iterator<?> it = list.iterator();
            // List.remove(Object) uses Objects.equals which will check content equality for Maps.
            while (it.hasNext()) {
                if (it.next() == o) {
                    it.remove();
                    return;
                }
            }
        } else if (container.get(leaf) == value) {
            container.remove(leaf);
        }
    }

    /**
     * Append a {@link NestedDocument} to this field and return it.
     */
    public NestedDocument doc() {
        List<Map<String, Object>> docs = getDocsAsList();

        if (docs == null) {
            NestedDocument doc = new NestedDocument(this, new HashMap<>());
            set(doc.getDoc());
            return doc;
        }

        NestedDocument doc = new NestedDocument(this, new HashMap<>());
        docs.add(doc.getDoc());
        return doc;
    }

    /**
     *  Returns a {@link NestedDocument} at the index, if index is beyond the end of the List, creates empty
     *  NestedDocument through the end of the array to the index.
     */
    public NestedDocument doc(int index) {
        List<Map<String, Object>> docs = getDocsAsList();
        if (docs == null) {
            if (index == 0) {
                NestedDocument doc = new NestedDocument(this, new HashMap<>());
                set(doc.getDoc());
                return doc;
            }
            docs = new ArrayList<>(index + 1);
            set(docs);
        }

        if (index < docs.size()) {
            return new NestedDocument(this, docs.get(index));
        }

        for (int i = docs.size(); i < index; i++) {
            docs.add(new HashMap<>());
        }

        NestedDocument doc = new NestedDocument(this, new HashMap<>());
        docs.add(doc.getDoc());
        return doc;
    }

    /**
     * Fetch the value at the current location, changing a Map into a single element list.  The new list is written back to the leaf.
     * If there is no value, return {@code null}.
     * If there is a value that is not a List or a Map, {@throws IllegalStateException}.
     */
    @SuppressWarnings("unchecked")
    protected List<Map<String, Object>> getDocsAsList() {
        Object value = get(MISSING);
        if (value == MISSING) {
            return null;
        }

        List<Map<String, Object>> docs;
        if (value instanceof List<?> list) {
            docs = (List<Map<String, Object>>) list;

        } else if (value instanceof Map<?, ?> map) {
            docs = new ArrayList<>(4);
            docs.add((Map<String, Object>) map);
            set(docs);

        } else {
            throw new IllegalStateException(
                "Unexpected value [" + value + "] of type [" + typeName(value) + "] at path [" + path + "], expected Map or List of Map"
            );
        }
        return docs;
    }

    /**
     * Iterable over all {@link NestedDocument}s in this field.
     */
    @SuppressWarnings("unchecked")
    public Iterable<NestedDocument> docs() {
        Object value = get(MISSING);
        if (value == MISSING) {
            return Collections.emptyList();
        }

        if (value instanceof Map<?, ?> map) {
            return () -> new Iterator<>() {
                private boolean done = false;

                @Override
                public boolean hasNext() {
                    return done == false;
                }

                @Override
                public NestedDocument next() {
                    if (done) {
                        throw new NoSuchElementException();
                    }
                    done = true;
                    return new NestedDocument(WriteField.this, (Map<String, Object>) map);
                }

                @Override
                public void remove() {
                    WriteField.this.remove();
                }
            };

        } else if (value instanceof List<?> list) {
            return () -> new Iterator<>() {
                private final Iterator<?> it = list.iterator();
                private int index;

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                @SuppressWarnings("unchecked")
                public NestedDocument next() {
                    Object value = it.next();
                    if (value instanceof Map<?, ?> map) {
                        index++;
                        return new NestedDocument(WriteField.this, (Map<String, Object>) map);
                    }

                    throw new IllegalStateException(
                        "Unexpected value ["
                            + value
                            + "] of type ["
                            + typeName(value)
                            + "] at ["
                            + path
                            + "] and index ["
                            + index
                            + "] for docs() iterator"
                    );
                }

                @Override
                public void remove() {
                    it.remove();
                }
            };
        }

        throw new IllegalStateException("Unexpected value [" + value + "] of type [" + typeName(value) + "] at [" + path + "] for docs()");
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
            Object value = container.getOrDefault(segment, MISSING);
            if (value instanceof Map<?, ?> map) {
                container = (Map<String, Object>) map;
            } else if (value == MISSING) {
                Map<String, Object> next = Maps.newHashMapWithExpectedSize(4);
                container.put(segment, next);
                container = next;
            } else {
                throw new IllegalArgumentException(
                    "Segment [" + i + ":'" + segment + "'] has value [" + value + "] of type [" + typeName(value) + "]"
                );
            }
        }
        leaf = segments[segments.length - 1];
    }

    protected String typeName(Object value) {
        return value != null ? value.getClass().getName() : "null";
    }
}
