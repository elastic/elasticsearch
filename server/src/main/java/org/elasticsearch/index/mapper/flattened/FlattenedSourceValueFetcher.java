/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceFormat;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A {@link SourceValueFetcher} for flattened fields that flattens nested objects into
 * dot-notation keys and converts all leaf values to strings, matching the representation
 * stored in doc values. When the source value is a list of maps, all maps are merged
 * into a single flat JSON blob rather than being processed individually. Just like doc
 * values.
 */
final class FlattenedSourceValueFetcher extends SourceValueFetcher {
    private final Set<String> sourcePaths;
    private final Object nullValue;
    private final boolean preserveArrays;

    FlattenedSourceValueFetcher(
        Set<String> sourcePaths,
        Object nullValue,
        IgnoredSourceFormat ignoredSourceFormat,
        boolean preserveArrays
    ) {
        super(sourcePaths, nullValue, ignoredSourceFormat);
        this.sourcePaths = sourcePaths;
        this.nullValue = nullValue;
        this.preserveArrays = preserveArrays;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) {
        Flattener flattener = new Flattener();
        for (String path : sourcePaths) {
            Object sourceValue = source.extractValue(path, nullValue);
            if (sourceValue instanceof Map<?, ?>) {
                flattener.flatten("", (Map<String, Object>) sourceValue);
            } else if (sourceValue instanceof List<?> list) {
                for (Object item : list) {
                    if (item instanceof Map<?, ?>) {
                        flattener.flatten("", (Map<String, Object>) item);
                    } else if (item != null) {
                        ignoredValues.add(item);
                    }
                }
            } else if (sourceValue != null) {
                ignoredValues.add(sourceValue);
            }
        }
        return flattener.result();
    }

    @Override
    protected Object parseSourceValue(Object value) {
        throw new UnsupportedOperationException("fetchValues is overridden, parseSourceValue is unused");
    }

    /**
     * Accumulates flattened key-value pairs from one or more source maps into a single
     * merged result, then serializes to a JSON blob matching the doc values representation.
     * Uses {@link BytesRef} keys and values so ordering matches Lucene's UTF-8 sort order.
     * Values are sorted and deduplicated to match sorted-set doc values behavior.
     */
    private class Flattener {
        private final TreeMap<BytesRef, Collection<BytesRef>> merged = new TreeMap<>();

        @SuppressWarnings("unchecked")
        void flatten(String prefix, Map<String, Object> source) {
            for (Map.Entry<String, Object> entry : source.entrySet()) {
                String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                Object value = entry.getValue();
                if (value instanceof Map) {
                    flatten(key, (Map<String, Object>) value);
                } else if (value instanceof List<?> list) {
                    for (Object item : list) {
                        if (item != null) {
                            addValue(key, item.toString());
                        } else if (preserveArrays) {
                            addNull(key);
                        }
                    }
                } else if (value != null) {
                    addValue(key, value.toString());
                } else if (preserveArrays) {
                    addNull(key);
                }
            }
        }

        private void addValue(String key, String value) {
            merged.computeIfAbsent(new BytesRef(key), k -> preserveArrays ? new ArrayList<>() : new TreeSet<>()).add(new BytesRef(value));
        }

        private void addNull(String key) {
            merged.computeIfAbsent(new BytesRef(key), k -> new ArrayList<>()).add(null);
        }

        /**
         * Returns the merged result as a single-element list with one JSON blob,
         * or an empty list if no values were accumulated.
         */
        List<Object> result() {
            if (merged.isEmpty()) {
                return List.of();
            }
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                for (Map.Entry<BytesRef, Collection<BytesRef>> entry : merged.entrySet()) {
                    List<BytesRef> values = new ArrayList<>(entry.getValue());
                    writeField(builder, entry.getKey(), values);
                }
                builder.endObject();
                return List.of(Strings.toString(builder));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static void writeField(XContentBuilder builder, BytesRef key, List<BytesRef> values) throws IOException {
            builder.field(key.utf8ToString());
            if (values.size() == 1) {
                BytesRef v = values.getFirst();
                if (v == null) {
                    builder.nullValue();
                } else {
                    builder.value(v.utf8ToString());
                }
                return;
            }
            builder.startArray();
            for (BytesRef value : values) {
                if (value == null) {
                    builder.nullValue();
                } else {
                    builder.value(value.utf8ToString());
                }
            }
            builder.endArray();
        }
    }
}
