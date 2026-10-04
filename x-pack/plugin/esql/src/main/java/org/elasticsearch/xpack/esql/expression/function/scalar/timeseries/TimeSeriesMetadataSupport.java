/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The one place that knows how a {@code _timeseries} value - the time-series metadata identifying a series - is encoded. The
 * time-series metadata operations ({@link TimeSeriesUnset}) read and write values only through this class: each prepares its
 * edit here once ({@link #unset}) and applies it to every value. A dedicated encoding replaces this class's internals; an
 * operation only names it and the type of its prepared edit, and no plan sees it.
 * <p>
 * Today a value is the JSON object of dimension values the source loads for a series, nested the way the document stores
 * them. Every value an operation writes is canonical: object keys sorted at every level, so two values carrying the same
 * dimensions compare equal however the source ordered them.
 */
final class TimeSeriesMetadataSupport {

    private TimeSeriesMetadataSupport() {}

    /**
     * Prepares unsetting {@code dimensions}, each named by field name. The document may store a dotted field name as one dotted
     * key, as nested objects, or as any mix of both, so a dimension is removed at every split of its dots.
     */
    static Unset unset(List<String> dimensions) {
        var paths = new LinkedHashSet<List<String>>();
        for (String dimension : new TreeSet<>(dimensions)) {
            paths.addAll(splits(dimension));
        }
        return new Unset(List.copyOf(dimensions), List.copyOf(paths));
    }

    /** Unsetting a fixed set of dimensions, its member paths computed once for every value it applies to. */
    record Unset(List<String> dimensions, List<List<String>> paths) {
        BytesRef apply(BytesRef value) throws IOException {
            Map<String, Object> object = read(value);
            for (List<String> path : paths) {
                remove(object, path);
            }
            return write(object);
        }

        @Override
        public String toString() {
            return "dimensions=" + dimensions;
        }
    }

    /** Every way to split {@code name} at its dots into member keys: {@code a.b} is {@code [a.b]} and {@code [a, b]}. */
    static List<List<String>> splits(String name) {
        String[] parts = name.split("\\.", -1);
        int dots = parts.length - 1;
        List<List<String>> splits = new ArrayList<>(1 << dots);
        for (int mask = 0; mask < (1 << dots); mask++) {
            List<String> keys = new ArrayList<>();
            StringBuilder key = new StringBuilder(parts[0]);
            for (int i = 1; i < parts.length; i++) {
                if ((mask & (1 << (i - 1))) != 0) {
                    keys.add(key.toString());
                    key.setLength(0);
                    key.append(parts[i]);
                } else {
                    key.append('.').append(parts[i]);
                }
            }
            keys.add(key.toString());
            splits.add(List.copyOf(keys));
        }
        return splits;
    }

    /**
     * Removes the member at {@code keys}, a no-op when it, or an object on the way to it, is missing. A parent object the removal
     * leaves empty is removed too, up to the value itself, so the result is what the source loads with the dimension excluded;
     * an object that was already empty is kept.
     */
    static void remove(Map<String, Object> object, List<String> keys) {
        List<Map<String, Object>> parents = new ArrayList<>(keys.size());
        Map<String, Object> current = object;
        for (int i = 0; i < keys.size() - 1; i++) {
            parents.add(current);
            if (current.get(keys.get(i)) instanceof Map<?, ?> nested) {
                current = uncheckedObjectMap(nested);
            } else {
                return;
            }
        }
        String leaf = keys.getLast();
        if (current.containsKey(leaf) == false) {
            return;
        }
        current.remove(leaf);
        for (int i = parents.size() - 1; i >= 0 && current.isEmpty(); i--) {
            current = parents.get(i);
            current.remove(keys.get(i));
        }
    }

    static Map<String, Object> read(BytesRef value) throws IOException {
        try (
            var parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, value.bytes, value.offset, value.length)
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("expected a JSON object");
            }
            Map<String, Object> object = parser.mapOrdered();
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException("trailing content after JSON object");
            }
            return object;
        } catch (XContentParseException e) {
            throw new IllegalArgumentException("invalid JSON object", e);
        }
    }

    static BytesRef write(Map<String, Object> object) throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.value(sorted(object));
            return BytesReference.bytes(builder).toBytesRef();
        }
    }

    private static Object sorted(Object value) {
        if (value instanceof Map<?, ?> map) {
            var sorted = new TreeMap<String, Object>();
            map.forEach((key, item) -> sorted.put((String) key, sorted(item)));
            return sorted;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(TimeSeriesMetadataSupport::sorted).toList();
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> uncheckedObjectMap(Map<?, ?> map) {
        return (Map<String, Object>) map;
    }
}
