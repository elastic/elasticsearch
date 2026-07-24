/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests that {@link UnmappedFieldsBlockLoader} reads {@code _source} and keeps only the top-level keys
 * selected by its {@link UnmappedFieldsPattern}, dropping everything else: the mapped fields (which the
 * analyzer adds to the pattern's excludes) and any key that does not match the includes.
 */
public class UnmappedFieldsBlockLoaderTests extends ESTestCase {
    public void testFiltersOutMappedFieldsKeepingUnmappedSourceKeys() throws IOException {
        // Mirrors production: includes "*" with the mapped field names added to the excludes, so only the
        // unmapped source keys survive into _unmapped_fields.
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.excludes(List.of("emp_no", "first_name")),
            Map.of("emp_no", 1, "first_name", "John", "first_pet", "Rex", "hobby", "chess")
        );
        assertMap(filtered, matchesMap().entry("first_pet", "Rex").entry("hobby", "chess"));
    }

    public void testIncludeWildcardSelectsMatchingSourceKeys() throws IOException {
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.includes(List.of("first*")),
            Map.of("first_name", "John", "first_pet", "Rex", "last_name", "Doe", "age", 30)
        );
        assertMap(filtered, matchesMap().entry("first_name", "John").entry("first_pet", "Rex"));
    }

    public void testIncludeWildcardWithExcludeRemovesMatchingKey() throws IOException {
        // A surviving key must match every include and no exclude: "first_name" matches "first*" but is excluded.
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.includes(List.of("first*")).withAdditionalExcludes(List.of("first_name")),
            Map.of("first_name", "John", "first_pet", "Rex", "first_toy", "ball", "last_name", "Doe")
        );
        assertMap(filtered, matchesMap().entry("first_pet", "Rex").entry("first_toy", "ball"));
    }

    public void testMultipleIncludesRequireAllToMatch() throws IOException {
        // Includes use AND semantics: a key survives only if it matches every include. "first_name_suffix" matches
        // both "first*" and "first_name*"; "first_pet" and "first_grade" match only "first*", so they are dropped.
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.includes(List.of("first*", "first_name*")),
            Map.of("first_name_suffix", "Jr", "first_pet", "Rex", "first_grade", "A", "last_name", "Doe")
        );
        assertMap(filtered, matchesMap().entry("first_name_suffix", "Jr"));
    }

    public void testExcludePatternRemovesMatchingSourceKeys() throws IOException {
        // excludes(...) starts from an include of "*" and then drops keys matching any exclude, so every key
        // outside the "secret*" family survives.
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.excludes(List.of("secret*")),
            Map.of("secret_key", "abc", "secret_token", "xyz", "public_note", "hello")
        );
        assertMap(filtered, matchesMap().entry("public_note", "hello"));
    }

    public void testNestedSourceValuesArePreserved() throws IOException {
        // Object- and array-valued keys pass through unchanged for surviving keys.
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.excludes(List.of("first_name")),
            Map.of("address", Map.of("city", "Berlin", "zip", "10115"), "tags", List.of("a", "b"), "first_name", "John")
        );
        assertMap(filtered, matchesMap().entry("address", Map.of("city", "Berlin", "zip", "10115")).entry("tags", List.of("a", "b")));
    }

    public void testNonePatternKeepsNoSourceKeys() throws IOException {
        Map<String, Object> filtered = load(UnmappedFieldsPattern.NONE, Map.of("a", "1", "b", "2"));
        assertMap(filtered, matchesMap());
    }

    public void testActivePatternMatchingNothingKeepsNoSourceKeys() throws IOException {
        // A non-NONE include pattern that matches no source key yields an empty object (distinct from the NONE sentinel).
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.includes(List.of("nomatch*")),
            Map.of("first_name", "John", "hobby", "chess")
        );
        assertMap(filtered, matchesMap());
    }

    public void testEmptySourceKeepsNoSourceKeys() throws IOException {
        Map<String, Object> filtered = load(UnmappedFieldsPattern.ALL, Map.of());
        assertMap(filtered, matchesMap());
    }

    public void testNullSourceValueIsKeptNotDropped() throws IOException {
        // _source may carry null values; the loader must keep the matching key with a null value rather than fail.
        Map<String, Object> source = new HashMap<>();
        source.put("first_pet", null);
        source.put("hobby", "chess");
        Map<String, Object> filtered = load(UnmappedFieldsPattern.ALL, source);
        assertMap(filtered, matchesMap().entry("first_pet", nullValue()).entry("hobby", "chess"));
    }

    /**
     * Runs the block loader over a single document whose {@code _source} is {@code sourceMap} and returns the emitted
     * JSON parsed back into a map. {@code convertToMap} returns a (content-type, map) tuple, so {@code v2()} is the map.
     */
    private static Map<String, Object> load(UnmappedFieldsPattern pattern, Map<String, Object> sourceMap) throws IOException {
        UnmappedFieldsBlockLoader loader = new UnmappedFieldsBlockLoader(pattern);
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(Source.fromMap(sourceMap, XContentType.JSON)), builder);
            BytesRef json = (BytesRef) ((TestBlock) builder.build()).get(0);
            return XContentHelper.convertToMap(new BytesArray(json.bytes, json.offset, json.length), false, XContentType.JSON).v2();
        }
    }

    /** Minimal stub; the loader only calls {@code source()}. */
    private static BlockLoader.StoredFields storedFields(Source source) {
        return new BlockLoader.StoredFields() {
            @Override
            public Source source() {
                return source;
            }

            @Override
            public String id() {
                return "0";
            }

            @Override
            public String routing() {
                return null;
            }

            @Override
            public Map<String, List<Object>> storedFields() {
                return Map.of();
            }

            @Override
            public boolean loaded() {
                return true;
            }
        };
    }
}
