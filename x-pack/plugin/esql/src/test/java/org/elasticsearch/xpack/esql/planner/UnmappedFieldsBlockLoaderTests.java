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

    public void testExcludePatternRemovesMatchingSourceKeys() throws IOException {
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.excludes(List.of("secret*")),
            Map.of("secret_key", "abc", "secret_token", "xyz", "public_note", "hello")
        );
        assertMap(filtered, matchesMap().entry("public_note", "hello"));
    }

    public void testNonePatternKeepsNoSourceKeys() throws IOException {
        Map<String, Object> filtered = load(UnmappedFieldsPattern.NONE, Map.of("a", "1", "b", "2"));
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

    /** Runs the block loader over a single document whose {@code _source} is {@code sourceMap}; returns the parsed JSON object. */
    private static Map<String, Object> load(UnmappedFieldsPattern pattern, Map<String, Object> sourceMap) throws IOException {
        UnmappedFieldsBlockLoader loader = new UnmappedFieldsBlockLoader(pattern);
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(Source.fromMap(sourceMap, XContentType.JSON)), builder);
            BytesRef json = (BytesRef) ((TestBlock) builder.build()).get(0);
            return XContentHelper.convertToMap(new BytesArray(json.bytes, json.offset, json.length), false, XContentType.JSON).v2();
        }
    }

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
