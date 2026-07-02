/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.index.mapper.BinaryDVBlockLoaderTestCase;
import org.elasticsearch.index.mapper.BlockLoaderTestRunner;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class FlattenedFieldRootBlockLoaderTests extends BinaryDVBlockLoaderTestCase {

    public FlattenedFieldRootBlockLoaderTests(Params params) {
        super("flattened", params);
    }

    @Override
    protected BlockLoaderTestRunner configureRunner(BlockLoaderTestRunner runner, Settings.Builder settings, Mapping mapping) {
        return runner.matcher((expected, actual) -> {
            List<Object> expectedList = parseExpected(expected);
            List<String> expectedJsons = expectedList.stream().map(v -> {
                try {
                    return toAlphabeticalJson(v);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).toList();
            List<String> actualJsons = parseActualAsStrings(actual);
            assertEquals(expectedJsons, actualJsons);
        });
    }

    private static String toAlphabeticalJson(Object value) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        writeAlphabetical(builder, value);
        return BytesReference.bytes(builder).utf8ToString();
    }

    @SuppressWarnings("unchecked")
    private static void writeAlphabetical(XContentBuilder builder, Object value) throws IOException {
        if (value instanceof Map<?, ?>) {
            builder.startObject();
            for (Map.Entry<String, Object> entry : new TreeMap<>((Map<String, Object>) value).entrySet()) {
                builder.field(entry.getKey());
                writeAlphabetical(builder, entry.getValue());
            }
            builder.endObject();
        } else if (value instanceof List<?>) {
            builder.startArray();
            for (Object item : (List<?>) value) {
                writeAlphabetical(builder, item);
            }
            builder.endArray();
        } else if (value != null) {
            builder.value(value);
        } else {
            builder.nullValue();
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> parseActualAsStrings(Object actual) {
        return switch (actual) {
            case List<?> list -> ((List<BytesRef>) actual).stream().map(BytesRef::utf8ToString).toList();
            case BytesRef bytesRef -> List.of(bytesRef.utf8ToString());
            case null -> Collections.emptyList();
            default -> throw new IllegalArgumentException("Expected list or BytesRef, found " + actual.getClass().getSimpleName());
        };
    }

    @SuppressWarnings("unchecked")
    private List<Object> parseExpected(Object expected) {
        return switch (expected) {
            case Map<?, ?> map -> List.of(map);
            case List<?> list -> (List<Object>) list;
            case null -> Collections.emptyList();
            default -> throw new IllegalArgumentException("Expected array or object, found " + expected.getClass().getSimpleName());
        };
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        var nullValue = (String) fieldMapping.get("null_value");
        // null_value is applied via doc values (substituted at index time by FlattenedFieldParser)
        // and via the source path (substituted at read time by FlattenedSourceValueFetcher).
        // Apply it unconditionally here to match both code paths.
        if (nullValue != null) {
            value = applyFlattenedNullValue(value, nullValue);
        }
        ValuesMode mode = ValuesMode.from(fieldMapping, params);
        return flattenAndStringify(value, mode);
    }

    private enum ValuesMode {
        /** Keep values in source order, preserve duplicates and nulls (preserve_leaf_arrays: exact). */
        AS_IS,
        /** Sort values and remove duplicates. Both SortedSetDocValues and SortedBinaryDocValues
         *  use SORTED_UNIQUE ordering for flattened fields. */
        SORTED_UNIQUE;

        static ValuesMode from(Map<String, Object> fieldMapping, Params params) {
            if ("exact".equals(fieldMapping.get("preserve_leaf_arrays"))) {
                // preserve_leaf_arrays: exact preserves duplicates, null slots, and original order
                // in both the doc values path (via offset stream) and the source path.
                return AS_IS;
            }
            return SORTED_UNIQUE;
        }
    }

    /**
     * Mirrors flattened source normalization by materializing mapped {@code null_value}
     * for null leaves in the expected source tree before comparison.
     */
    private static Object applyFlattenedNullValue(Object value, String nullValue) {
        return switch (value) {
            case null -> nullValue;
            case Map<?, ?> map -> map.entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        e -> (String) e.getKey(),
                        e -> applyFlattenedNullValue(e.getValue(), nullValue),
                        (a, b) -> b,
                        LinkedHashMap::new
                    )
                );
            case List<?> list -> list.stream().map(v -> applyFlattenedNullValue(v, nullValue)).toList();
            default -> value;
        };
    }

    /**
     * Flattened fields store all leaf values as strings and flatten nested objects
     * into dot-notation keys, mirroring the behavior in
     * {@link org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.RootFlattenedFieldType#blockLoader}.
     * A multi-valued flattened field (list of maps) is merged into a single flat map
     * because the block loader produces one JSON blob per document.
     */
    @SuppressWarnings("unchecked")
    private static Object flattenAndStringify(Object value, ValuesMode mode) {
        return switch (value) {
            case null -> null;
            case Map<?, ?> map -> flattenMaps(List.of((Map<String, Object>) map), mode);
            case List<?> list -> {
                List<Map<String, Object>> maps = new ArrayList<>();
                for (Object item : list) {
                    if (item instanceof Map<?, ?>) {
                        maps.add((Map<String, Object>) item);
                    }
                }
                yield maps.isEmpty() ? null : flattenMaps(maps, mode);
            }
            default -> value;
        };
    }

    private static LinkedHashMap<String, Object> flattenMaps(List<Map<String, Object>> maps, ValuesMode mode) {
        TreeMap<BytesRef, List<BytesRef>> flat = new TreeMap<>();
        for (Map<String, Object> map : maps) {
            flattenSource("", map, flat, mode);
        }
        if (flat.isEmpty()) {
            return null;
        }
        collapseValues(flat, mode);
        // Convert to a LinkedHashMap preserving the BytesRef key order
        LinkedHashMap<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<BytesRef, List<BytesRef>> e : flat.entrySet()) {
            List<BytesRef> values = e.getValue();
            if (values.size() == 1) {
                BytesRef v = values.getFirst();
                result.put(e.getKey().utf8ToString(), v == null ? null : v.utf8ToString());
            } else {
                result.put(e.getKey().utf8ToString(), values.stream().map(v -> v == null ? null : v.utf8ToString()).toList());
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static void flattenSource(
        String prefix,
        Map<String, Object> source,
        TreeMap<BytesRef, List<BytesRef>> result,
        ValuesMode mode
    ) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                flattenSource(key, (Map<String, Object>) value, result, mode);
            } else if (value instanceof List<?> list) {
                for (Object item : list) {
                    if (item != null) {
                        result.computeIfAbsent(new BytesRef(key), k -> new ArrayList<>()).add(new BytesRef(item.toString()));
                    } else if (mode == ValuesMode.AS_IS) {
                        result.computeIfAbsent(new BytesRef(key), k -> new ArrayList<>()).add(null);
                    }
                }
            } else if (value != null) {
                result.computeIfAbsent(new BytesRef(key), k -> new ArrayList<>()).add(new BytesRef(value.toString()));
            } else if (mode == ValuesMode.AS_IS) {
                result.computeIfAbsent(new BytesRef(key), k -> new ArrayList<>()).add(null);
            }
        }
    }

    private static void collapseValues(TreeMap<BytesRef, List<BytesRef>> result, ValuesMode mode) {
        if (mode == ValuesMode.AS_IS) {
            // preserve_leaf_arrays: exact — offset stream restores original order, dups, and nulls
            return;
        }
        for (Map.Entry<BytesRef, List<BytesRef>> entry : result.entrySet()) {
            List<BytesRef> list = entry.getValue();
            TreeSet<BytesRef> unique = new TreeSet<>(list);
            list.clear();
            list.addAll(unique);
        }
    }

    public void testBlockLoaderMultiValuedField() throws IOException {
        runner.breaker(newLimitedBreaker(TEST_BREAKER_SIZE));
        // Source has a list of maps — both doc values and source paths merge all keyed
        // values into one flat JSON blob.
        runner.document(Map.of("field", List.of(Map.of("a", "1", "b", "2"), Map.of("c", "3", "a", "4"))));
        runner.fieldName("field");

        Mapping mapping = new Mapping(
            Map.of("_doc", Map.of("properties", Map.of("field", Map.of("type", "flattened")))),
            Map.of("field", Map.of("type", "flattened"))
        );

        var settings = getSettingsForParams();
        runner.mapperService(createMapperService(settings.build(), XContentFactory.jsonBuilder().map(mapping.raw())));
        runner.run(new BytesRef("{\"a\":[\"1\",\"4\"],\"b\":\"2\",\"c\":\"3\"}"));
    }

    public void testBlockLoaderDottedKeyAndNestedObject() throws IOException {
        runner.breaker(newLimitedBreaker(TEST_BREAKER_SIZE));
        // "a.b" as a dotted key and "a":{"b":...} as a nested object both flatten to the same key
        runner.document(Map.of("field", Map.of("a.b", "cat", "a", Map.of("b", "dog"))));
        runner.fieldName("field");

        Mapping mapping = new Mapping(
            Map.of("_doc", Map.of("properties", Map.of("field", Map.of("type", "flattened")))),
            Map.of("field", Map.of("type", "flattened"))
        );

        // Both paths produce keyed value "a.b" — doc values deduplicates and sorts
        String expected = "{\"a.b\":[\"cat\",\"dog\"]}";

        var settings = getSettingsForParams();
        runner.mapperService(createMapperService(settings.build(), XContentFactory.jsonBuilder().map(mapping.raw())));
        runner.run(new BytesRef(expected));
    }

    public void testBlockLoaderOutputFlatStructure() throws IOException {
        runner.breaker(newLimitedBreaker(TEST_BREAKER_SIZE));
        runner.document(Map.of("field", Map.of("a", Map.of("x", "10"), "b", Map.of("y", "20"))));
        runner.fieldName("field");

        Mapping mapping = new Mapping(
            Map.of("_doc", Map.of("properties", Map.of("field", Map.of("type", "flattened")))),
            Map.of("field", Map.of("type", "flattened"))
        );

        String expected = "{\"a.x\":\"10\",\"b.y\":\"20\"}";

        var settings = getSettingsForParams();
        runner.mapperService(createMapperService(settings.build(), XContentFactory.jsonBuilder().map(mapping.raw())));
        runner.run(new BytesRef(expected));
    }

    public void testBlockLoaderForcesSourceWhenMappedTextSubfieldPresent() throws IOException {
        assumeFalse("a bare text sub-field is not allowed under synthetic source", params.syntheticSource());
        assumeFalse("columnar-stored source does not retain a bare text sub-field", params.isColumnarStored());

        runner.breaker(newLimitedBreaker(TEST_BREAKER_SIZE));
        Map<String, Object> labels = Map.of("env", "prod", "status_code", 200, "message", "hello");
        runner.document(Map.of("field", labels));
        runner.fieldName("field");

        // status_code is a mapped long (doc values), message is a mapped text (no doc values, not stored), env is unmapped.
        Map<String, Object> flattenedMapping = Map.of(
            "type",
            "flattened",
            "properties",
            Map.of("status_code", Map.of("type", "long"), "message", Map.of("type", "text"))
        );
        Mapping mapping = new Mapping(
            Map.of("_doc", Map.of("properties", Map.of("field", flattenedMapping))),
            Map.of("field", flattenedMapping)
        );

        String expected = "{\"env\":\"prod\",\"message\":\"hello\",\"status_code\":\"200\"}";

        var settings = getSettingsForParams();
        runner.mapperService(createMapperService(settings.build(), XContentFactory.jsonBuilder().map(mapping.raw())));
        runner.run(new BytesRef(expected));
    }

    public void testBlockLoaderStringifiesMappedRootViaSource() throws IOException {
        runner.breaker(newLimitedBreaker(TEST_BREAKER_SIZE));
        // status is a mapped keyword, code a mapped long; unmapped_key lands in the keyed channel.
        runner.document(Map.of("field", Map.of("status", "ok", "code", 200, "unmapped_key", "some_value")));
        runner.fieldName("field");

        Map<String, Object> flattenedMapping = Map.of(
            "type",
            "flattened",
            "properties",
            Map.of("status", Map.of("type", "keyword"), "code", Map.of("type", "long"))
        );
        Mapping mapping = new Mapping(
            Map.of("_doc", Map.of("properties", Map.of("field", flattenedMapping))),
            Map.of("field", flattenedMapping)
        );

        String expected = "{\"code\":\"200\",\"status\":\"ok\",\"unmapped_key\":\"some_value\"}";

        var settings = getSettingsForParams();
        runner.mapperService(createMapperService(settings.build(), XContentFactory.jsonBuilder().map(mapping.raw())));
        runner.run(new BytesRef(expected));
    }

    public void testBlockLoaderMappedPropertyOnlyViaSource() throws IOException {
        runner.breaker(newLimitedBreaker(TEST_BREAKER_SIZE));
        // Only the mapped keyword sub-field has a value; the keyed channel is empty.
        runner.document(Map.of("field", Map.of("status", "active")));
        runner.fieldName("field");

        Map<String, Object> flattenedMapping = Map.of("type", "flattened", "properties", Map.of("status", Map.of("type", "keyword")));
        Mapping mapping = new Mapping(
            Map.of("_doc", Map.of("properties", Map.of("field", flattenedMapping))),
            Map.of("field", flattenedMapping)
        );

        // A mapped sub-field forces _source even with no unmapped keys, so the single mapped leaf renders as a string
        // and the blob is identical on every loading path.
        String expected = "{\"status\":\"active\"}";

        var settings = getSettingsForParams();
        runner.mapperService(createMapperService(settings.build(), XContentFactory.jsonBuilder().map(mapping.raw())));
        runner.run(new BytesRef(expected));
    }

    @Override
    protected boolean supportsMultiField() {
        return false;
    }
}
