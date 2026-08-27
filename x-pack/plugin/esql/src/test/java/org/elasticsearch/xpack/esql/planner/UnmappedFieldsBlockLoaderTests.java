/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests that {@link UnmappedFieldsBlockLoader} reads {@code _source} and keeps only the top-level keys
 * selected by its {@link UnmappedFieldsPattern}, dropping everything else: the mapped fields (which the
 * analyzer adds to the pattern's excludes), any key that does not match the includes, and any key whose
 * value carries no information for the coordinator to turn into a column.
 */
public class UnmappedFieldsBlockLoaderTests extends ESTestCase {

    /** Any value above one exercises the scaling; production reads it from {@code PlannerSettings.SOURCE_RESERVATION_FACTOR}. */
    private static final double RESERVATION_FACTOR = 4;

    public void testFiltersOutMappedFieldsKeepingUnmappedSourceKeys() throws IOException {
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
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.includes(List.of("first*")).withAdditionalExcludes(List.of("first_name")),
            Map.of("first_name", "John", "first_pet", "Rex", "first_toy", "ball", "last_name", "Doe")
        );
        assertMap(filtered, matchesMap().entry("first_pet", "Rex").entry("first_toy", "ball"));
    }

    public void testSingleIncludeGroupUsesOrSemantics() throws IOException {
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.includes(List.of("first_name*", "salary_bonus*")),
            Map.of("first_name_suffix", "Jr", "salary_bonus", 100, "first_pet", "Rex", "last_name", "Doe")
        );
        assertMap(filtered, matchesMap().entry("first_name_suffix", "Jr").entry("salary_bonus", 100));
    }

    public void testMultipleIncludeGroupsRequireEachGroupToMatch() throws IOException {
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.includes(List.of("first*")).intersect(UnmappedFieldsPattern.includes(List.of("first_name*"))),
            Map.of("first_name_suffix", "Jr", "first_pet", "Rex", "first_grade", "A", "last_name", "Doe")
        );
        assertMap(filtered, matchesMap().entry("first_name_suffix", "Jr"));
    }

    public void testExcludePatternRemovesMatchingSourceKeys() throws IOException {
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.excludes(List.of("secret*")),
            Map.of("secret_key", "abc", "secret_token", "xyz", "public_note", "hello")
        );
        assertMap(filtered, matchesMap().entry("public_note", "hello"));
    }

    public void testNestedSourceValuesArePreserved() throws IOException {
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.excludes(List.of("first_name")),
            Map.of("address", Map.of("city", "Berlin", "zip", "10115"), "tags", List.of("a", "b"), "first_name", "John")
        );
        assertMap(filtered, matchesMap().entry("address", Map.of("city", "Berlin", "zip", "10115")).entry("tags", List.of("a", "b")));
    }

    public void testNonePatternEmitsNull() throws IOException {
        assertThat(load(UnmappedFieldsPattern.NONE, Map.of("a", "1", "b", "2")), nullValue());
    }

    public void testActivePatternMatchingNothingEmitsNull() throws IOException {
        assertThat(load(UnmappedFieldsPattern.includes(List.of("nomatch*")), Map.of("first_name", "John", "hobby", "chess")), nullValue());
    }

    public void testEmptySourceEmitsNull() throws IOException {
        assertThat(load(UnmappedFieldsPattern.ALL, Map.of()), nullValue());
    }

    /**
     * A key whose value is null, an empty array or an array of nulls says as little about the field as omitting the key would, so it
     * is not worth a trip to the coordinator - where it would earn the field an output column that is null in every row.
     */
    public void testValuelessSourceValuesAreDropped() throws IOException {
        Map<String, Object> source = new HashMap<>();
        source.put("null_value", null);
        source.put("empty_array", List.of());
        source.put("array_of_null", singletonList(null));
        source.put("nested_emptiness", List.of(singletonList(null), List.of()));
        source.put("hobby", "chess");
        Map<String, Object> filtered = load(UnmappedFieldsPattern.ALL, source);
        assertMap(filtered, matchesMap().entry("hobby", "chess"));
    }

    /**
     * Objects are not expanded into columns of their own, but a nully object still says nothing about the field it sits under, so it
     * must not keep that field's column alive either - not even through a leaf buried inside it, which would not be mapped anyway.
     */
    public void testValuelessSourceObjectsAreDropped() throws IOException {
        Map<String, Object> emptyObject = new HashMap<>();
        Map<String, Object> nullyLeaves = new HashMap<>();
        nullyLeaves.put("baz", singletonList(null));
        nullyLeaves.put("inga", emptyObject);
        Map<String, Object> source = new HashMap<>();
        source.put("empty_object", emptyObject);
        source.put("object_of_nully_leaves", nullyLeaves);
        source.put("array_of_nully_objects", List.of(Map.of("foo", singletonList(null)), Map.of("bar", List.of())));
        source.put("hobby", "chess");
        Map<String, Object> filtered = load(UnmappedFieldsPattern.ALL, source);
        assertMap(filtered, matchesMap().entry("hobby", "chess"));
    }

    /** An object with anything at all in it is kept, but only the parts of it that say something survive. */
    public void testSourceObjectWithAValueKeepsOnlyThatValue() throws IOException {
        Map<String, Object> nested = new HashMap<>();
        nested.put("baz", "world");
        nested.put("empty", List.of());
        nested.put("null_leaf", null);
        Map<String, Object> filtered = load(UnmappedFieldsPattern.ALL, Map.of("extra", nested));
        assertMap(filtered, matchesMap().entry("extra", matchesMap().entry("baz", "world")));
    }

    public void testOnlyValuelessSourceValuesEmitsNull() throws IOException {
        Map<String, Object> source = new HashMap<>();
        source.put("first_pet", null);
        source.put("tags", List.of());
        source.put("address", Map.of("city", List.of()));
        assertThat(load(UnmappedFieldsPattern.ALL, source), nullValue());
    }

    /**
     * One real element makes an array worth keeping, but the nulls around it are not: were the field mapped, the array would have
     * become a multi-value, and multi-values never contain nulls.
     */
    public void testNullsAreStrippedFromKeptArrays() throws IOException {
        Map<String, Object> filtered = load(UnmappedFieldsPattern.ALL, Map.of("tags", asList(null, "a", null, "b")));
        assertMap(filtered, matchesMap().entry("tags", List.of("a", "b")));
    }

    public void testNullsAreStrippedFromNestedArraysAndObjects() throws IOException {
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.ALL,
            Map.of("tags", asList(null, asList("b", null), List.of(), Map.of("keep", "me", "drop", List.of())))
        );
        assertMap(filtered, matchesMap().entry("tags", List.of(List.of("b"), Map.of("keep", "me"))));
    }

    /** Nothing to prune means nothing to rebuild: the loader hands the value straight through, untouched. */
    public void testValuesWithoutNullsPassThroughUnchanged() throws IOException {
        Map<String, Object> filtered = load(
            UnmappedFieldsPattern.ALL,
            Map.of("tags", List.of("a", "b"), "address", Map.of("city", "Berlin", "zips", List.of("10115")))
        );
        assertMap(
            filtered,
            matchesMap().entry("tags", List.of("a", "b")).entry("address", Map.of("city", "Berlin", "zips", List.of("10115")))
        );
    }

    public void testReaderToStringIsDistinctFromLoader() throws IOException {
        UnmappedFieldsBlockLoader loader = loader(UnmappedFieldsPattern.ALL);
        assertThat(loader.toString(), equalTo("UnmappedFieldsBlockLoader"));
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            assertThat(reader.toString(), equalTo("UnmappedFieldsBlockLoader.UnmappedFields"));
        }
    }

    public void testPerDocumentSourceParsingReservesAndReleasesBreakerBytes() throws IOException {
        Source source = Source.fromMap(Map.of("payload", "x".repeat(4096)), XContentType.JSON);
        long reservation = (long) (source.internalSourceRef().length() * RESERVATION_FACTOR);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(BlockSourceReader.ESTIMATED_SIZE + reservation));
        UnmappedFieldsBlockLoader loader = loader(UnmappedFieldsPattern.ALL);
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
            long readerReservation = breaker.getUsed();
            assertThat(readerReservation, equalTo(BlockSourceReader.ESTIMATED_SIZE));

            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(source), builder);

            assertThat(breaker.getUsed(), equalTo(readerReservation));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testSourceReservationIsScaledByReservationFactor() throws IOException {
        Source source = Source.fromMap(Map.of("payload", "x".repeat(4096)), XContentType.JSON);
        // Room for the raw source but not for the far larger map it parses into, so only a scaled reservation trips this.
        long limit = BlockSourceReader.ESTIMATED_SIZE + source.internalSourceRef().length();
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(limit));
        UnmappedFieldsBlockLoader loader = loader(UnmappedFieldsPattern.ALL);
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            expectThrows(CircuitBreakingException.class, () -> reader.read(0, storedFields(source), builder));
            assertThat(breaker.getUsed(), equalTo(BlockSourceReader.ESTIMATED_SIZE));
        }
    }

    public void testReaderUnderCrankyBreakerDoesNotLeak() throws IOException {
        UnmappedFieldsBlockLoader loader = loader(UnmappedFieldsPattern.ALL);
        Source source = Source.fromMap(Map.of("a", "b", "c", "d"), XContentType.JSON);
        var cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        for (int attempt = 0; attempt < 2000; attempt++) {
            try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(cranky, null)) {
                BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
                reader.read(0, storedFields(source), builder);
            } catch (CircuitBreakingException e) {
                // expected on some attempts
            }
            assertThat("breaker leaked on attempt " + attempt, cranky.getUsed(), equalTo(0L));
        }
    }

    private static UnmappedFieldsBlockLoader loader(UnmappedFieldsPattern pattern) {
        return new UnmappedFieldsBlockLoader(pattern, RESERVATION_FACTOR);
    }

    /**
     * Runs the block loader over a single document whose {@code _source} is {@code sourceMap} and returns the emitted
     * JSON parsed back into a map, or {@code null} if the loader emitted a null because no key matched the pattern.
     * {@code convertToMap} returns a (content-type, map) tuple, so {@code v2()} is the map.
     */
    private static Map<String, Object> load(UnmappedFieldsPattern pattern, Map<String, Object> sourceMap) throws IOException {
        UnmappedFieldsBlockLoader loader = loader(pattern);
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(Source.fromMap(sourceMap, XContentType.JSON)), builder);
            BytesRef json = (BytesRef) ((TestBlock) builder.build()).get(0);
            if (json == null) {
                return null;
            }
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
