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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    public String getName() {
        return path;
    }

    public boolean isExists() {
        return container != null && container.containsKey(leaf);
    }

    // Path Update
    public void move(String path) {
        throw new UnsupportedOperationException("unimplemented");
    }

    public void overwrite(String path) {
        throw new UnsupportedOperationException("unimplemented");
    }

    // Path Delete
    public void remove() {
        throw new UnsupportedOperationException("unimplemented");
    }

    // Value Create
    public WriteField set(Object value) {
        container.put(getOrCreateLeaf(), value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public WriteField append(Object value) {
        getOrCreateLeaf();
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
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException("unimplemented");
    }

    public Object get(Object defaultValue) {
        if (leaf == null) {
            return defaultValue;
        }
        return container.getOrDefault(leaf, defaultValue);
    }

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
    @SuppressWarnings("unchecked")
    public WriteField dedupe() {
        if (leaf == null) {
            return this;
        }
        Object value = container.getOrDefault(leaf, MISSING);
        if (value == MISSING) {
            return this;
        }
        if (value instanceof List<?> list) {
            container.put(leaf, new HashSet<>((List<Object>) list).stream().toList());
        }
        return this;
    }

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

    protected String getOrCreateLeaf() {
        if (leaf == null) {
            resolveDepthFlat();
        }
        if (leaf == null) {
            createDepth();
        }
        return leaf;
    }

    @SuppressWarnings("unchecked")
    protected void resolveDepthFlat() {
        container = rootSupplier.get();

        int idx = path.indexOf('.');
        int lastIdx = 0;
        String parent;

        while (idx != -1) {
            parent = path.substring(lastIdx, idx);
            Object value = container.get(parent);
            if (value instanceof Map<?, ?> map) {
                container = (Map<String, Object>) map;
                lastIdx = idx + 1;
                idx = path.indexOf('.', lastIdx);
            } else {
                String rest = path.substring(lastIdx);
                if (container.containsKey(rest)) {
                    leaf = rest;
                } else {
                    leaf = null;
                }
                return;
            }
        }
        leaf = path.substring(lastIdx);
    }

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
