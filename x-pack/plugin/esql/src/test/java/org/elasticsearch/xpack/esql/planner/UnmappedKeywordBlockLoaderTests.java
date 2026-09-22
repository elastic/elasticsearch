/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceFormat;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests that {@link UnmappedKeywordBlockLoader} reads a {@code _source} value onto the keyword typed attribute for an unmapped field:
 * an object value is read as {@code null}, an array contributes its scalar elements, and a scalar becomes a single value.
 */
public class UnmappedKeywordBlockLoaderTests extends ESTestCase {

    public void testScalarReadsAsSingleValue() throws IOException {
        assertThat(load("f", Map.of("f", "hello")), equalTo("hello"));
    }

    public void testNonStringScalarsUseToString() throws IOException {
        assertThat(load("f", Map.of("f", 42)), equalTo("42"));
        assertThat(load("f", Map.of("f", true)), equalTo("true"));
        assertThat(load("f", Map.of("f", 1.5)), equalTo("1.5"));
    }

    public void testObjectReadsAsNull() throws IOException {
        assertThat(load("f", Map.of("f", Map.of("a", "b"))), nullValue());
    }

    public void testArrayOfScalarsReadsAsMultivalue() throws IOException {
        assertThat(load("f", Map.of("f", List.of("a", "b"))), equalTo(List.of("a", "b")));
    }

    public void testNestedArraysAreFlattened() throws IOException {
        assertThat(load("f", Map.of("f", List.of(List.of("a", "b"), "c"))), equalTo(List.of("a", "b", "c")));
    }

    public void testObjectsInsideArrayAreDropped() throws IOException {
        assertThat(load("f", Map.of("f", Arrays.asList("a", Map.of("k", "v"), "b"))), equalTo(List.of("a", "b")));
    }

    public void testArrayOfOnlyObjectsReadsAsNull() throws IOException {
        assertThat(load("f", Map.of("f", List.of(Map.of("a", "b"), Map.of("c", "d")))), nullValue());
    }

    public void testEmptyArrayReadsAsNull() throws IOException {
        assertThat(load("f", Map.of("f", List.of())), nullValue());
    }

    public void testMissingFieldReadsAsNull() throws IOException {
        assertThat(load("f", Map.of("other", "x")), nullValue());
    }

    public void testExplicitNullReadsAsNull() throws IOException {
        Map<String, Object> source = new HashMap<>();
        source.put("f", null);
        assertThat(load("f", source), nullValue());
    }

    public void testSingleElementArrayCollapsesToSingleValue() throws IOException {
        assertThat(load("f", Map.of("f", List.of("only"))), equalTo("only"));
    }

    public void testDottedPathIsExtracted() throws IOException {
        assertThat(load("a.b", Map.of("a", Map.of("b", "leaf"))), equalTo("leaf"));
    }

    public void testDottedPathThroughArrayOfObjects() throws IOException {
        assertThat(load("a.b", Map.of("a", List.of(Map.of("b", "x"), Map.of("b", "y")))), equalTo(List.of("x", "y")));
    }

    public void testValuesAreUtf8Encoded() throws IOException {
        assertThat(load("f", Map.of("f", "χαίρετε")), equalTo("χαίρετε"));
    }

    public void testScalarsFromParsedJson() throws IOException {
        Source source = Source.fromBytes(new BytesArray("""
            {"f": 1.5}"""), XContentType.JSON);
        assertThat(loadFrom("f", source), equalTo("1.5"));
    }

    public void testDoesNotSupportOrdinals() {
        UnmappedKeywordBlockLoader loader = loader("f");
        assertThat(loader.supportsOrdinals(), equalTo(false));
        expectThrows(UnsupportedOperationException.class, () -> loader.ordinals(null));
    }

    public void testNoColumnAtATimeReader() {
        assertThat(loader("f").columnAtATimeReader(null), nullValue());
    }

    public void testToStringNamesTheFieldAndIsDistinctFromTheReader() throws IOException {
        UnmappedKeywordBlockLoader loader = loader("some_field");
        assertThat(loader.toString(), equalTo("UnmappedKeywordBlockLoader[some_field]"));
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            assertThat(reader.toString(), equalTo("UnmappedKeywordBlockLoader.Reader[some_field]"));
        }
    }

    /**
     * One reader serves a whole page ({@link org.elasticsearch.index.mapper.BlockStoredFieldsReader#canReuse} is always true), so the
     * scratch list it collects into has to be reset between documents. Interleaving multivalue, single, null and multivalue catches
     * values bleeding from one position into the next.
     */
    public void testOneReaderAcrossManyDocuments() throws IOException {
        List<Map<String, Object>> docs = List.of(
            Map.of("f", List.of("a", "b")),
            Map.of("f", "c"),
            Map.of("other", "x"),
            Map.of("f", List.of("d", "e"))
        );
        UnmappedKeywordBlockLoader loader = loader("f");
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), docs.size());
            for (int doc = 0; doc < docs.size(); doc++) {
                reader.read(doc, storedFields(Source.fromMap(docs.get(doc), XContentType.JSON)), builder);
            }
            TestBlock block = (TestBlock) builder.build();
            assertThat(asValue(block.get(0)), equalTo(List.of("a", "b")));
            assertThat(asValue(block.get(1)), equalTo("c"));
            assertThat(asValue(block.get(2)), nullValue());
            assertThat(asValue(block.get(3)), equalTo(List.of("d", "e")));
        }
    }

    /**
     * This reader adds no accounting of its own on top of the fixed reservation it inherits, so reading must not move the breaker.
     * Pins that asymmetry, so a per-document reservation added later without a matching release is caught.
     */
    public void testReadingHoldsOnlyTheInheritedReservation() throws IOException {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        UnmappedKeywordBlockLoader loader = loader("f");
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
            assertThat(breaker.getUsed(), equalTo(BlockSourceReader.ESTIMATED_SIZE));
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(Source.fromMap(Map.of("f", "x".repeat(4096)), XContentType.JSON)), builder);
            assertThat(breaker.getUsed(), equalTo(BlockSourceReader.ESTIMATED_SIZE));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private static UnmappedKeywordBlockLoader loader(String fieldName) {
        return new UnmappedKeywordBlockLoader(fieldName, Set.of(fieldName), IgnoredSourceFormat.NO_IGNORED_SOURCE);
    }

    /**
     * Runs the loader over a single document and returns what landed in the block: {@code null}, a {@code String} for a single value,
     * or a {@code List<String>} for a multivalue. {@link TestBlock} stores a position entry as a nested list, so the shape of the
     * return value is itself part of what these tests assert.
     */
    private static Object load(String fieldName, Map<String, Object> sourceMap) throws IOException {
        return loadFrom(fieldName, Source.fromMap(sourceMap, XContentType.JSON));
    }

    private static Object loadFrom(String fieldName, Source source) throws IOException {
        UnmappedKeywordBlockLoader loader = loader(fieldName);
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(source), builder);
            return asValue(((TestBlock) builder.build()).get(0));
        }
    }

    /** {@link TestBlock} stores a position entry as a nested list, so the shape here is itself part of what the tests assert. */
    private static Object asValue(Object blockValue) {
        if (blockValue instanceof List<?> list) {
            return list.stream().map(v -> ((BytesRef) v).utf8ToString()).toList();
        }
        return blockValue == null ? null : ((BytesRef) blockValue).utf8ToString();
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
