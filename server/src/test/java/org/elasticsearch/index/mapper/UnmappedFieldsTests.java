/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests that {@link UnmappedFields#blockLoader(String)} reads a {@code _source} value as the keywords of an unmapped field: an
 * object value is read as {@code null}, an array contributes its scalar elements, and a scalar becomes a single value.
 */
public class UnmappedFieldsTests extends MapperServiceTestCase {

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

    public void testDoesNotSupportOrdinals() throws IOException {
        BlockLoader loader = loader("f");
        assertThat(loader.supportsOrdinals(), equalTo(false));
        expectThrows(UnsupportedOperationException.class, () -> loader.ordinals(null));
    }

    public void testNoColumnAtATimeReader() throws IOException {
        assertThat(loader("f").columnAtATimeReader(null), nullValue());
    }

    public void testToStringNamesTheFieldAndIsDistinctFromTheReader() throws IOException {
        BlockLoader loader = loader("some_field");
        assertThat(loader.toString(), equalTo("UnmappedFields.Named[some_field]"));
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            assertThat(reader.toString(), equalTo("UnmappedFields.Named.Reader[some_field]"));
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
        BlockLoader loader = loader("f");
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
        BlockLoader loader = loader("f");
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
            assertThat(breaker.getUsed(), equalTo(BlockSourceReader.ESTIMATED_SIZE));
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(Source.fromMap(Map.of("f", "x".repeat(4096)), XContentType.JSON)), builder);
            assertThat(breaker.getUsed(), equalTo(BlockSourceReader.ESTIMATED_SIZE));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * A nested object is walked like any array of objects: the fields its mapping declares are left out and the others
     * hold the values of all of the nested objects, at any depth and by name too.
     */
    public void testUndeclaredFieldsBelowANestedObject() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            b.startObject("item").field("type", "nested").startObject("properties");
            b.startObject("declared").field("type", "keyword").endObject();
            b.startObject("inner").field("type", "nested").endObject();
            b.endObject().endObject();
            b.endObject();
        }));
        String json = """
            {
              "item": [ { "value": "a", "declared": "b", "obj": { "value": "c" }, "inner": [ { "value": "d" } ] }, { "value": "e" } ],
              "item.value": "f",
              "item.declared": "g"
            }""";
        assertThat(
            leaves(mapperService, json),
            equalTo(Map.of("item.value", List.of("a", "e", "f"), "item.obj.value", List.of("c"), "item.inner.value", List.of("d")))
        );
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        for (String name : List.of("item.value", "item.obj.value", "item.inner.value")) {
            assertTrue(name, unmappedFields.mayExist(name));
            assertThat(name, unmappedFields.blockLoader(name), hasToString("UnmappedFields.Named[" + name + "]"));
        }
        BlockLoader loader = unmappedFields.blockLoader("item.value");
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(Source.fromBytes(new BytesArray(json), XContentType.JSON)), builder);
            assertThat((List<?>) asValue(((TestBlock) builder.build()).get(0)), containsInAnyOrder("a", "e", "f"));
        }
    }

    /**
     * A key of the value of a mapped field is not an unmapped field, for the walk and by name. That holds for the types
     * that take an object as their value only.
     */
    public void testNameBelowAMappedFieldIsNotAnUnmappedField() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            b.startObject("span").field("type", "integer_range").endObject();
            b.startObject("labels").field("type", "flattened").endObject();
            b.startObject("obj").startObject("properties").startObject("point").field("type", "geo_point").endObject().endObject();
            b.endObject();
            b.endObject();
        }));
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        for (String name : List.of("span.gte", "labels.env", "labels.a.b", "obj.point.lat")) {
            assertFalse(name, unmappedFields.mayExist(name));
            assertThat(name, unmappedFields.blockLoader(name), sameInstance(ConstantNull.INSTANCE));
        }
        for (String name : List.of("spans.gte", "obj.other", "obj.points.lat")) {
            assertTrue(name, unmappedFields.mayExist(name));
        }
        assertThat(leaves(mapperService, """
            {
              "span": { "gte": 1 }, "labels": { "env": "prod" }, "obj": { "point": { "lat": 1, "lon": 2 }, "other": 3 },
              "spans": { "gte": 4 }
            }"""), equalTo(Map.of("obj.other", List.of(3), "spans.gte", List.of(4))));
    }

    /**
     * Whatever resolves to a field of the shard is left out: a field, its multi-fields, an alias and a runtime field.
     */
    public void testNamesThatResolveToAFieldAreLeftOut() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("runtime").startObject("computed").field("type", "keyword").endObject().endObject();
            b.startObject("properties");
            b.startObject("field").field("type", "keyword");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
            b.startObject("alias").field("type", "alias").field("path", "field").endObject();
            b.endObject();
        }));
        assertThat(leaves(mapperService, """
            { "field": "a", "field.raw": "b", "alias": "c", "computed": "d", "other": "e" }"""), equalTo(Map.of("other", List.of("e"))));
    }

    /**
     * A field whose type does not take an object never read what is below its path, so that is an unmapped field. It is
     * how a document spells a field next to it where subobjects are disabled, and what a document stored before the field
     * was mapped.
     */
    public void testPathsBelowAFieldThatTakesNoObject() throws IOException {
        MapperService root = createMapperService(topMapping(b -> {
            b.field("dynamic", false).field("subobjects", false);
            b.startObject("properties").startObject("host").field("type", "keyword").endObject().endObject();
        }));
        assertThat(
            leaves(root, """
                { "host": "h", "host.name": "n", "other.name": "o" }"""),
            equalTo(Map.of("host.name", List.of("n"), "other.name", List.of("o")))
        );
        assertThat(
            createSearchExecutionContext(root).unmappedFields().blockLoader("host.name"),
            hasToString("UnmappedFields.Named[host.name]")
        );

        MapperService inner = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            b.startObject("attributes").field("type", "object").field("subobjects", false).field("dynamic", false);
            b.startObject("properties").startObject("host").field("type", "keyword").endObject().endObject();
            b.endObject();
            b.startObject("plain").field("type", "keyword").endObject();
            b.endObject();
        }));
        assertThat(
            leaves(inner, """
                { "attributes": { "host": "h", "host.name": "n" }, "attributes.host.os": "o", "plain": { "below": "p" } }"""),
            equalTo(Map.of("attributes.host.name", List.of("n"), "attributes.host.os", List.of("o"), "plain.below", List.of("p")))
        );
        UnmappedFields unmappedFields = createSearchExecutionContext(inner).unmappedFields();
        assertTrue(unmappedFields.mayExist("plain.below"));
        assertFalse(unmappedFields.mayExist("attributes"));
        assertThat(unmappedFields.blockLoader("plain.below"), hasToString("UnmappedFields.Named[plain.below]"));
    }

    /**
     * What is written has no object below it: a field is a key spelled with dots, the keys are in the order the fields are first found,
     * and a field with several values has them in an array, in the order they were found. A null is never a value.
     */
    public void testWritesOneObjectOfNamesAsFound() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        Map<String, Object> written = extract(createSearchExecutionContext(mapperService).unmappedFields(), """
            {
              "k": { "l": [ 8, { "m": 9 } ] },
              "f": [ 4, null, [ 5 ], { "g": 6 }, { "g": "six" } ],
              "h": null,
              "b.c": 1,
              "b": { "c": 2, "d": { "e": true } },
              "a": [ "one" ]
            }""");
        assertThat(new ArrayList<>(written.keySet()), equalTo(List.of("k.l", "k.l.m", "f", "f.g", "b.c", "b.d.e", "a")));
        assertThat(written.get("a"), equalTo("one"));
        assertThat(written.get("b.c"), equalTo(List.of(1, 2)));
        assertThat(written.get("b.d.e"), equalTo(true));
        assertThat(written.get("f"), equalTo(List.of(4, 5)));
        assertThat(written.get("f.g"), equalTo(List.of(6, "six")));
        assertThat(written.get("k.l"), equalTo(8));
        assertThat(written.get("k.l.m"), equalTo(9));
    }

    /**
     * The values of a document are written as they are found unless it may spell a field in two ways: a key of an object
     * that holds an object or an array is what precedes a dot of another key of the same object.
     */
    public void testFieldSpelledInTwoWays() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties").startObject("mapped").field("type", "flattened").endObject().endObject();
        }));
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        for (String json : List.of("""
            { "a.b": 1, "a.c": 2, "d": { "e.f": 3, "e.g": 4 }, "h": [ { "i": 5 }, { "i": 6 } ] }""", """
            { "a": 1, "a.b": 2, "c": [ 3, 4 ], "d.e": { "f": 5 } }""", """
            { "a": [ { "b.c": 1, "b": { "c": 2 } } ] }""")) {
            assertTrue(json, writtenAsFound(unmappedFields, json));
            assertNotNull(extract(unmappedFields, json));
        }
        for (String json : List.of("""
            { "a": { "b": 1 }, "a.b": 2 }""", """
            { "a.b": 1, "a": { "b": 2 } }""", """
            { "a.b.c": 1, "a.b": { "c": 2 } }""", """
            { "a.b": [ { "c": 1 } ], "a.b.c": 2 }""", """
            { "x": { "a.b": 1, "a": [ 2 ] } }""", """
            { "a": { "b": 1 }, "a.z": 2 }""")) {
            assertFalse(json, writtenAsFound(unmappedFields, json));
            assertNotNull(extract(unmappedFields, json));
        }
        assertThat(extract(unmappedFields, """
            { "a": { "b": 1 }, "z": 0, "a.b": [ 2, 3 ] }"""), equalTo(Map.of("a.b", List.of(1, 2, 3), "z", 0)));
        // Nothing below a field that takes an object as its value is a field, however it is spelled.
        String mapped = """
            { "mapped": { "b": 1 }, "mapped.b": 2 }""";
        assertTrue(writtenAsFound(unmappedFields, mapped));
        assertThat(extract(unmappedFields, mapped), nullValue());
    }

    /**
     * Writing the values of a document as they are found gives what holding them all gives, over documents that spell
     * fields with dots, as keys of objects and in arrays.
     */
    public void testWritingAsFoundMatchesHoldingAll() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties").startObject("kw").field("type", "keyword").endObject().endObject();
        }));
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        List<String> keys = List.of("kw", "a", "b", "c", "a.b", "b.c", "a.b.c", "kw.a");
        int asFound = 0;
        for (int i = 0; i < 500; i++) {
            String json;
            try (XContentBuilder source = XContentFactory.jsonBuilder()) {
                json = Strings.toString(source.map(randomObject(keys, 0)));
            }
            extract(unmappedFields, json);
            if (writtenAsFound(unmappedFields, json)) {
                asFound++;
            }
        }
        assertThat(asFound, greaterThan(0));
        assertThat(asFound, lessThan(500));
    }

    /**
     * The loader of all unmapped fields emits one JSON object a document, and null for a document without any.
     */
    public void testAllFieldsLoader() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties").startObject("mapped").field("type", "keyword").endObject().endObject();
        }));
        BlockLoader loader = createSearchExecutionContext(mapperService).unmappedFields()
            .allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 2.0));
        assertThat(loader, hasToString("UnmappedFields.All"));
        assertTrue(loader.rowStrideStoredFieldSpec().requiresSource());
        assertFalse(loader.supportsOrdinals());
        assertThat(loader.columnAtATimeReader(null), nullValue());

        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
            assertThat(reader, hasToString("UnmappedFields.All.Reader"));
            long inherited = breaker.getUsed();
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 3);
            // The serialization of _source does not matter: what is loaded is JSON.
            XContentType sourceType = randomFrom(XContentType.JSON, XContentType.SMILE, XContentType.CBOR);
            try (XContentBuilder source = XContentBuilder.builder(sourceType.xContent())) {
                source.map(XContentHelper.convertToMap(JsonXContent.jsonXContent, """
                    { "mapped": "m", "one": 1, "many": [ "a", null, { "deep": true } ], "obj": { "x": 1.5 }, "skipped": 2 }""", true));
                reader.read(0, storedFields(Source.fromBytes(BytesReference.bytes(source), sourceType)), builder);
            }
            reader.read(1, storedFields(json("""
                { "mapped": "m", "skipped": 2, "empty": [], "nulls": [ null ], "none": null }""")), builder);
            reader.read(2, storedFields(json("{}")), builder);
            assertThat(breaker.getUsed(), equalTo(inherited));

            TestBlock block = (TestBlock) builder.build();
            assertThat(
                XContentHelper.convertToMap(JsonXContent.jsonXContent, ((BytesRef) block.get(0)).utf8ToString(), true),
                equalTo(Map.of("one", 1, "many", "a", "many.deep", true, "obj.x", 1.5, "skipped", 2))
            );
            assertThat(((BytesRef) block.get(1)).utf8ToString(), equalTo("{\"skipped\":2}"));
            assertThat(block.get(2), nullValue());
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * An array of many short values costs many times its size in {@code _source} while its values are held, more than the reservation
     * of the loader of all fields stands for. The values are charged as they are held, the read trips the breaker and it leaves nothing
     * charged.
     */
    public void testAllFieldsLoaderChargesTheValuesItHolds() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        // A reservation of the size of _source: all of what is held beyond the JSON that is built is charged.
        BlockLoader loader = createSearchExecutionContext(mapperService).unmappedFields()
            .allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 1.0));
        assertTripsWhileHoldingValues(loader, json("{\"a\":" + arrayOfShortStrings(100_000) + "}"));
    }

    /**
     * The values of a name are held with no reservation at all, so what they cost is charged from the first byte.
     */
    public void testNamedLoaderChargesTheValuesItHolds() throws IOException {
        assertTripsWhileHoldingValues(loader("a"), json("{\"a\":" + arrayOfShortStrings(100_000) + "}"));
    }

    /**
     * A document that spells a field in two ways is walked again with all of its values held.
     */
    public void testValuesOfADocumentSpelledInTwoWaysAreCharged() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        BlockLoader loader = createSearchExecutionContext(mapperService).unmappedFields()
            .allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 1.0));
        String array = arrayOfShortStrings(60_000);
        assertTripsWhileHoldingValues(loader, json("{\"a.b\":" + array + ",\"a\":{\"b\":" + array + "}}"));
    }

    /**
     * What is held up to the part of the reservation that stands for held values is paid for already, and is not charged again.
     */
    public void testHeldValuesTheReservationCoversAreNotChargedAgain() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        BlockLoader loader = createSearchExecutionContext(mapperService).unmappedFields()
            .allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 10.0));
        // Held values cost less than the share of the reservation that is left once the JSON that is built is paid for.
        String json = "{\"a\":" + "[" + String.join(",", Collections.nCopies(3_000, "\"" + "x".repeat(100) + "\"")) + "]}";
        long reservation = json.length() * 10L;
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(reservation + ByteSizeValue.ofKb(100).getBytes()));
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
            long inherited = breaker.getUsed();
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            reader.read(0, storedFields(json(json)), builder);
            assertThat(breaker.getUsed(), equalTo(inherited));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Reads {@code source} with a breaker that has room for the reservation of the loader but not for the values that the read holds,
     * which trips it and leaves nothing charged. A breaker with room for all of it reads the same document, so what trips the small
     * one is what is held.
     */
    private void assertTripsWhileHoldingValues(BlockLoader loader, Source source) throws IOException {
        CircuitBreaker roomy = newLimitedBreaker(ByteSizeValue.ofMb(100));
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(roomy, null)) {
            reader.read(0, storedFields(source), loader.builder(TestBlock.factory(), 1));
        }
        assertThat(roomy.getUsed(), equalTo(0L));

        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(2));
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
            long inherited = breaker.getUsed();
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            expectThrows(CircuitBreakingException.class, () -> reader.read(0, storedFields(source), builder));
            assertThat(breaker.getUsed(), equalTo(inherited));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private static String arrayOfShortStrings(int elements) {
        return "[" + String.join(",", Collections.nCopies(elements, "\"x\"")) + "]";
    }

    /**
     * An array of {@code elements} strings of random length up to {@code maxLength} characters, some of them with characters that are
     * not ASCII. Strings of one character cannot show that a charge is too small for longer ones, or for ones that take more than a
     * byte for a character: what a value costs to hold depends on both. A few dozen random strings are repeated, to keep a large
     * array cheap to make.
     */
    private static String arrayOfStrings(int elements, int maxLength) throws IOException {
        String[] pool = new String[64];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = randomBoolean()
                ? randomAlphaOfLengthBetween(1, maxLength)
                : randomRealisticUnicodeOfCodepointLengthBetween(1, maxLength);
        }
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startArray();
            for (int i = 0; i < elements; i++) {
                builder.value(pool[i % pool.length]);
            }
            builder.endArray();
            return Strings.toString(builder);
        }
    }

    /**
     * The heap that the loader of all fields holds while it reads an array of many short values is no more than what it has charged
     * to the breaker by then, with the reservation that ES|QL makes by default.
     */
    public void testAllFieldsLoaderChargesAtLeastTheHeapItHolds() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        BlockLoader loader = createSearchExecutionContext(mapperService).unmappedFields()
            .allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 10.0));
        assertChargesAtLeastTheHeapItHolds(loader, json("{\"a\":" + arrayOfShortStrings(1_000_000) + "}"));
    }

    /**
     * The same for strings of random length, some not ASCII. The reservation of {@code 10.0} times the size of {@code _source} covers
     * the values held of strings of more than a few characters, so a multiple that leaves no allowance is used, to charge every value
     * held for the check to measure.
     */
    public void testAllFieldsLoaderChargesAtLeastTheHeapItHoldsForRandomStrings() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        BlockLoader loader = createSearchExecutionContext(mapperService).unmappedFields()
            .allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 4.0));
        assertChargesAtLeastTheHeapItHolds(loader, json("{\"a\":" + arrayOfStrings(500_000, 16) + "}"));
    }

    /**
     * The heap that the loader of a named field holds while it reads an array of many short values, with no reservation, is no more
     * than what it has charged to the breaker by then.
     */
    public void testNamedLoaderChargesAtLeastTheHeapItHolds() throws IOException {
        assertChargesAtLeastTheHeapItHolds(loader("a"), json("{\"a\":" + arrayOfStrings(500_000, 16) + "}"));
    }

    /**
     * The same for strings of up to a hundred characters. The named loader once charged a copy of a string for as many bytes as the
     * string has characters, though the copy took three for each, which a string of one character cannot show.
     */
    public void testNamedLoaderChargesAtLeastTheHeapItHoldsForLongStrings() throws IOException {
        String array = "[" + String.join(",", Collections.nCopies(200_000, "\"" + "x".repeat(100) + "\"")) + "]";
        assertChargesAtLeastTheHeapItHolds(loader("a"), json("{\"a\":" + array + "}"));
        assertChargesAtLeastTheHeapItHolds(loader("a"), json("{\"a\":" + arrayOfStrings(200_000, 100) + "}"));
    }

    /**
     * A document that spells a field in two ways is walked again with all of its values held, and what that holds is no more than what
     * is charged for it.
     */
    public void testFieldSpelledInTwoWaysChargesAtLeastTheHeapItHolds() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        BlockLoader loader = createSearchExecutionContext(mapperService).unmappedFields()
            .allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 10.0));
        String array = arrayOfShortStrings(500_000);
        assertChargesAtLeastTheHeapItHolds(loader, json("{\"a.b\":" + array + ",\"a\":{\"b\":" + array + "}}"));
    }

    /** The same for strings of random length, some not ASCII, with no allowance, as in the test of the loader of all fields. */
    public void testFieldSpelledInTwoWaysChargesAtLeastTheHeapItHoldsForRandomStrings() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        BlockLoader loader = createSearchExecutionContext(mapperService).unmappedFields()
            .allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 4.0));
        String array = arrayOfStrings(250_000, 16);
        assertChargesAtLeastTheHeapItHolds(loader, json("{\"a.b\":" + array + ",\"a\":{\"b\":" + array + "}}"));
    }

    /**
     * Reads {@code source} with a {@link MeasuringBreaker} and checks that every time it measured, the heap held since the read started
     * was no more than what had been charged since then, give or take {@link #MEASUREMENT_SLACK} for the noise of measuring it.
     * <p>
     * This is what a heap attack test at the default request breaker limit would show, without a document large enough to exhaust a
     * heap: a read that holds more than it charges can exhaust the heap before the breaker trips.
     */
    private void assertChargesAtLeastTheHeapItHolds(BlockLoader loader, Source source) throws IOException {
        MeasuringBreaker breaker = new MeasuringBreaker(newLimitedBreaker(ByteSizeValue.ofGb(1)), ByteSizeValue.ofMb(4).getBytes());
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
            BlockLoader.StoredFields storedFields = storedFields(source);
            breaker.start();
            reader.read(0, storedFields, builder);
        }
        logger.info("charged and held heap since the read started, in MB: {}", breaker.samples);
        assertThat("too few measurements to show anything", breaker.samples.size(), greaterThan(4));
        for (Sample sample : breaker.samples) {
            assertThat(sample.toString(), sample.held, lessThanOrEqualTo(sample.charged + MEASUREMENT_SLACK));
        }
    }

    /** How far the heap measured after a GC may be off: objects of other threads, and what the GC leaves behind. */
    private static final long MEASUREMENT_SLACK = ByteSizeValue.ofMb(2).getBytes();

    private record Sample(long charged, long held) {
        @Override
        public String toString() {
            return "charged=" + charged / (1 << 20) + " held=" + held / (1 << 20);
        }
    }

    /**
     * A breaker that, each time what is charged to it has grown by {@code step} bytes, measures the heap that is reachable after a full
     * GC. That is the only point during a walk at which the values that it holds can be measured from outside, since they are alive
     * only while it runs, and it is where the breaker would trip.
     */
    private static final class MeasuringBreaker implements CircuitBreaker {
        private final CircuitBreaker delegate;
        private final long step;
        private final List<Sample> samples = new ArrayList<>();
        private long chargedAtStart;
        private long heldAtStart;
        private long nextSample = -1;

        MeasuringBreaker(CircuitBreaker delegate, long step) {
            this.delegate = delegate;
            this.step = step;
        }

        /** Takes what is charged and the heap that is reachable now as the baseline, and starts measuring. */
        void start() {
            chargedAtStart = delegate.getUsed();
            heldAtStart = reachableHeapBytes();
            nextSample = 0;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            delegate.addEstimateBytesAndMaybeBreak(bytes, label);
            if (nextSample < 0 || bytes <= 0) {
                return;
            }
            long charged = delegate.getUsed() - chargedAtStart;
            if (charged >= nextSample) {
                samples.add(new Sample(charged, reachableHeapBytes() - heldAtStart));
                nextSample = charged + step;
            }
        }

        private static long reachableHeapBytes() {
            // A hint, though with the collectors that tests run with it is a full collection. Twice, for what the first one finalizes.
            System.gc();
            System.gc();
            return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            delegate.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            delegate.addWithoutBreaking(bytes);
        }

        @Override
        public long getUsed() {
            return delegate.getUsed();
        }

        @Override
        public long getLimit() {
            return delegate.getLimit();
        }

        @Override
        public double getOverhead() {
            return delegate.getOverhead();
        }

        @Override
        public long getTrippedCount() {
            return delegate.getTrippedCount();
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public Durability getDurability() {
            return delegate.getDurability();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            delegate.setLimitAndOverhead(limit, overhead);
        }
    }

    /**
     * A document that cannot be read, or whose reservation does not fit, fails the read and leaves nothing reserved.
     */
    public void testAllFieldsLoaderReleasesItsReservationWhenItFails() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        BlockLoader loader = createSearchExecutionContext(mapperService).unmappedFields()
            .allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 2.0));
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
            long inherited = breaker.getUsed();
            BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);

            // Deeper than a parser accepts. Such a document cannot be indexed either.
            int depth = 5_000;
            String tooDeep = "{\"n\":".repeat(depth) + "1" + "}".repeat(depth);
            expectThrows(Exception.class, () -> reader.read(0, storedFields(json(tooDeep)), builder));
            assertThat(breaker.getUsed(), equalTo(inherited));

            String tooLarge = "{\"field\":\"" + "x".repeat((int) ByteSizeValue.ofKb(600).getBytes()) + "\"}";
            expectThrows(CircuitBreakingException.class, () -> reader.read(0, storedFields(json(tooLarge)), builder));
            assertThat(breaker.getUsed(), equalTo(inherited));

            reader.read(0, storedFields(json("{\"field\":1}")), builder);
            assertThat(((BytesRef) ((TestBlock) builder.build()).get(0)).utf8ToString(), equalTo("{\"field\":1}"));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Whenever the breaker refuses what a loader asks for, to open a reader or to read a document, nothing is left
     * reserved once the reader is closed.
     */
    public void testLoadersLeaveNothingReservedWhenTheBreakerRefuses() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        Source source = json("""
            { "a": 1, "b": { "c": [ 2, { "d": 3 } ] }, "e.f": "x", "e": { "f": "y" } }""");
        int refused = 0;
        int read = 0;
        for (int i = 0; i < 200; i++) {
            BlockLoader loader = randomBoolean()
                ? unmappedFields.allFieldsBlockLoader(new UnmappedFields.Walk(Set.of(), UnmappedFields.Filter.ALL, 2.0))
                : unmappedFields.blockLoader("b.c.d");
            CircuitBreaker breaker = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
            try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
                BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 3);
                for (int doc = 0; doc < 3; doc++) {
                    reader.read(doc, storedFields(source), builder);
                }
                assertThat(((TestBlock) builder.build()).size(), equalTo(3));
                read++;
            } catch (CircuitBreakingException e) {
                assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
                refused++;
            }
            assertThat(breaker.getUsed(), equalTo(0L));
        }
        assertThat(refused, greaterThan(0));
        assertThat(read, greaterThan(0));
    }

    /**
     * Whatever a walk finds is found by name with the same values, and a name a walk does not find reads as null, over
     * documents that put every shape of value at paths that are mapped, below a mapped path, and neither.
     */
    public void testNamingAFieldFindsWhatAWalkFinds() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("runtime").startObject("rt").field("type", "keyword").endObject().endObject();
            b.startObject("properties");
            b.startObject("kw").field("type", "keyword").endObject();
            b.startObject("al").field("type", "alias").field("path", "kw").endObject();
            b.startObject("span").field("type", "integer_range").endObject();
            b.startObject("labels").field("type", "flattened").endObject();
            b.startObject("item").field("type", "nested").startObject("properties");
            b.startObject("declared").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.startObject("obj").startObject("properties");
            b.startObject("kw").field("type", "keyword").endObject();
            b.startObject("point").field("type", "geo_point").endObject();
            b.endObject().endObject();
            b.startObject("off").field("type", "object").field("enabled", false).endObject();
            b.endObject();
        }));
        List<String> keys = List.of(
            "kw",
            "al",
            "rt",
            "span",
            "labels",
            "item",
            "obj",
            "off",
            "declared",
            "point",
            "gte",
            "x",
            "y",
            "x.y",
            "obj.kw",
            "obj.x",
            "item.x",
            "kw.x",
            "span.gte",
            "labels.x"
        );
        assertNamingFindsWhatAWalkFinds(mapperService, keys);
    }

    /** As {@link #testNamingAFieldFindsWhatAWalkFinds}, with keys that have empty or stray dots. */
    public void testNamingAFieldWithStrayDots() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties").startObject("a").startObject("properties");
            b.startObject("kw").field("type", "keyword").endObject();
            b.endObject().endObject().endObject();
        }));
        assertNamingFindsWhatAWalkFinds(mapperService, List.of("a", "b", "kw", "a.b", "a.kw", ".a", "a.", "a..b", ".", ""));
    }

    /**
     * A name that ends with a dot is read as it is spelled, as the column writes it, and not as the name without the dot
     * that a lookup in {@code _source} parsed into a map reads.
     */
    public void testNameEndingWithADot() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        Source source = json("""
            { "a": { "b": 1 }, "c": 2 }""");
        assertThat(readFromMap(source, "a.b."), equalTo(List.of("1")));
        assertThat(readFromMap(source, "c.."), equalTo(List.of("2")));
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        for (String name : List.of("a.b.", "c..", ".")) {
            BlockLoader loader = unmappedFields.blockLoader(name);
            try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
                BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
                reader.read(0, storedFields(source), builder);
                assertThat(name, ((TestBlock) builder.build()).get(0), nullValue());
            }
        }
    }

    /** As {@link #testNamingAFieldFindsWhatAWalkFinds}, below objects that do not have subobjects. */
    public void testNamingAFieldWithoutSubobjects() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            b.startObject("flat").field("type", "object").field("subobjects", false).startObject("properties");
            b.startObject("a.b").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.endObject();
        }));
        assertNamingFindsWhatAWalkFinds(mapperService, List.of("flat", "a", "b", "c", "a.b", "a.c", "flat.a", "flat.a.b"));
    }

    /** As {@link #testNamingAFieldFindsWhatAWalkFinds}, beside a pass-through object whose fields are also mapped at the root. */
    public void testNamingAFieldBesideAPassThroughObject() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            b.startObject("attrs").field("type", "passthrough").field("priority", 10).startObject("properties");
            b.startObject("host").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.endObject();
        }));
        assertNamingFindsWhatAWalkFinds(mapperService, List.of("attrs", "host", "x", "attrs.host", "attrs.x", "host.x"));
    }

    /**
     * Over random documents built from {@code keys}, loading a field by name gives what the lookup in {@code _source}
     * parsed into a map gives, in the same order, and naming fields in a walk changes nothing of what it writes.
     */
    private void assertNamingFindsWhatAWalkFinds(MapperService mapperService, List<String> keys) throws IOException {
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        for (int iteration = 0; iteration < 200; iteration++) {
            Map<String, Object> source = randomObject(keys, 0);
            Source parsed = Source.fromBytes(Source.fromMap(source, XContentType.JSON).internalSourceRef(), XContentType.JSON);
            String json = parsed.internalSourceRef().utf8ToString();
            Map<String, List<String>> walked = new HashMap<>();
            leaves(extract(unmappedFields, json)).forEach(
                (path, values) -> walked.put(path, values.stream().map(Object::toString).toList())
            );
            Set<String> names = new HashSet<>(walked.keySet());
            for (int i = 0; i < 20; i++) {
                names.add(String.join(".", randomList(1, 3, () -> randomFrom(keys))));
            }
            // Naming fields in a walk changes nothing of what it writes, and each name finds what it finds on its own.
            Set<String> unmappedNames = new HashSet<>(names);
            unmappedNames.removeIf(name -> mapperService.mappingLookup().getFieldType(name) != null);
            UnmappedFields.Found found = unmappedFields.walk(
                parsed.internalSourceRef(),
                XContentType.JSON,
                new UnmappedFields.Walk(unmappedNames, UnmappedFields.Filter.ALL, 0),
                NoopCircuitBreaker.INSTANCE,
                0
            );
            assertThat(
                json,
                found.written(),
                equalTo(unmappedFields.extract(parsed.internalSourceRef(), XContentType.JSON, NoopCircuitBreaker.INSTANCE))
            );
            for (String name : names) {
                if (mapperService.mappingLookup().getFieldType(name) != null) {
                    assertThat(name + " in " + source, walked.get(name), nullValue());
                    continue;
                }
                if (mapperService.mappingLookup().objectMappers().get(name) instanceof ObjectMapper object && object.isEnabled()) {
                    // A document with a value at an object the mapping parses is rejected, so the mapping rules it out
                    assertFalse(name, unmappedFields.mayExist(name));
                    assertThat(unmappedFields.blockLoader(name), sameInstance(ConstantNull.INSTANCE));
                    continue;
                }
                List<String> expected = walked.getOrDefault(name, List.of());
                assertThat(name + " in " + source, unmappedFields.mayExist(name) || expected.isEmpty(), equalTo(true));
                BlockLoader loader = unmappedFields.blockLoader(name);
                List<String> named = new ArrayList<>();
                if (loader == ConstantNull.INSTANCE) {
                    assertThat(name + " in " + source, expected, equalTo(List.of()));
                    continue;
                }
                try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
                    BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
                    reader.read(0, storedFields(parsed), builder);
                    Object value = asValue(((TestBlock) builder.build()).get(0));
                    if (value instanceof List<?> list) {
                        list.forEach(v -> named.add((String) v));
                    } else if (value != null) {
                        named.add((String) value);
                    }
                }
                assertThat(name + " in " + source, named, containsInAnyOrder(expected.toArray()));
                if (name.endsWith(".") == false) {
                    // The lookup drops the dots a name ends with, see testNameEndingWithADot.
                    assertThat(name + " in " + source, named, equalTo(readFromMap(parsed, name)));
                }
                List<BytesRef> foundByName = found.valuesOf(name);
                assertThat(name + " in " + source, foundByName.stream().map(BytesRef::utf8ToString).toList(), equalTo(named));
            }
        }
    }

    /**
     * A value is read by name as text exactly as a lookup in {@code _source} parsed into a map reads it, whatever the
     * serialization of {@code _source}.
     */
    public void testNamedValuesAreTheTextOfTheParsedValues() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        Map<String, Object> document = XContentHelper.convertToMap(JsonXContent.jsonXContent, """
            { "float": 1.10, "exp": 1e3, "negative": -0.0, "long": 9223372036854775807, "big": 123456789012345678901234567890,
              "bool": false, "str": "x", "empty": "", "nested": [ 1, [ 2.5, [ "three" ] ], null, { "skipped": 4 } ] }""", true);
        for (XContentType type : List.of(XContentType.JSON, XContentType.SMILE, XContentType.CBOR, XContentType.YAML)) {
            Source source;
            try (XContentBuilder builder = XContentBuilder.builder(type.xContent())) {
                // Rendered from the JSON text, so that each serialization keeps the numbers as the JSON parser read them.
                builder.map(document);
                source = Source.fromBytes(BytesReference.bytes(builder), type);
            }
            for (String name : document.keySet()) {
                BlockLoader loader = unmappedFields.blockLoader(name);
                try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
                    BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
                    reader.read(0, storedFields(source), builder);
                    Object value = asValue(((TestBlock) builder.build()).get(0));
                    List<String> fromMap = readFromMap(source, name);
                    Object expected = fromMap.isEmpty() ? null : fromMap.size() == 1 ? fromMap.get(0) : fromMap;
                    assertThat(type + " " + name, value, equalTo(expected));
                }
            }
        }
    }

    /** The values of {@code name} as a lookup in {@code _source} parsed into a map reads them, as text. */
    private static List<String> readFromMap(Source source, String name) {
        List<String> values = new ArrayList<>();
        flatten(XContentMapValues.extractValue(name, source.source()), values);
        return values;
    }

    private static void flatten(Object value, List<String> out) {
        if (value instanceof List<?> list) {
            list.forEach(v -> flatten(v, out));
        } else if (value != null && value instanceof Map<?, ?> == false) {
            out.add(value.toString());
        }
    }

    /**
     * The loaders made with one walk read a document once between them: the named fields get their own values and the
     * column gets the fields the filter includes, whatever the order the loaders read in.
     */
    public void testLoadersOfOneWalkReadADocumentOnce() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties").startObject("mapped").field("type", "keyword").endObject().endObject();
        }));
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        UnmappedFields.Filter onlyA = new UnmappedFields.Filter() {
            @Override
            public boolean includes(String path) {
                return path.startsWith("a");
            }

            @Override
            public boolean mayIncludeBelow(String path) {
                return path.startsWith("a");
            }
        };
        UnmappedFields.Walk walk = new UnmappedFields.Walk(Set.of("a.b", "c", "mapped", "missing"), onlyA, 2.0);
        List<BlockLoader> loaders = new ArrayList<>(
            List.of(
                unmappedFields.allFieldsBlockLoader(walk),
                unmappedFields.blockLoader("a.b", walk),
                unmappedFields.blockLoader("c", walk),
                unmappedFields.blockLoader("missing", walk)
            )
        );
        Collections.shuffle(loaders, random());
        CountingSource source = new CountingSource(json("""
            { "mapped": "m", "a": { "b": [ 1, 2 ], "x": true }, "c": "y", "d": { "e": 3 } }"""));
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        BlockLoader.StoredFields storedFields = storedFields(source);
        Map<String, Object> loaded = new HashMap<>();
        for (BlockLoader loader : loaders) {
            try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(breaker, null)) {
                BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
                reader.read(0, storedFields, builder);
                loaded.put(loader.toString(), asValue(((TestBlock) builder.build()).get(0)));
            }
        }
        assertThat(source.reads, equalTo(1));
        assertThat(breaker.getUsed(), equalTo(0L));
        assertThat(loaded.get("UnmappedFields.All"), equalTo("{\"a.b\":[1,2],\"a.x\":true}"));
        assertThat(loaded.get("UnmappedFields.Named[a.b]"), equalTo(List.of("1", "2")));
        assertThat(loaded.get("UnmappedFields.Named[c]"), equalTo("y"));
        assertThat(loaded.get("UnmappedFields.Named[missing]"), nullValue());
    }

    /**
     * A field spelled in several ways in a document has the values of all of them, in the order a lookup in {@code _source}
     * parsed into a map finds them, whether or not the walk also writes the column.
     */
    public void testFieldSpelledInSeveralWaysByName() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        Source source = json("""
            { "a.b": 1, "a": { "b": [ 2, 3 ] }, "a.b.c": 4,
              "x": [ { "y.z": 5, "y": { "z": 6 } }, { "y": { "z": 7 } } ] }""");
        Map<String, Object> expected = Map.of("a.b", List.of("2", "3", "1"), "a.b.c", "4", "x.y.z", List.of("6", "5", "7"));
        for (UnmappedFields.Filter filter : Arrays.asList(null, UnmappedFields.Filter.ALL)) {
            UnmappedFields.Walk walk = new UnmappedFields.Walk(expected.keySet(), filter, 2.0);
            BlockLoader.StoredFields storedFields = storedFields(source);
            for (String name : expected.keySet()) {
                BlockLoader loader = unmappedFields.blockLoader(name, walk);
                try (BlockLoader.RowStrideReader reader = loader.rowStrideReader(newLimitedBreaker(ByteSizeValue.ofMb(1)), null)) {
                    BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
                    reader.read(0, storedFields, builder);
                    Object value = asValue(((TestBlock) builder.build()).get(0));
                    assertThat(name, value, equalTo(expected.get(name)));
                    assertThat(name, value instanceof List<?> ? value : List.of(value), equalTo(readFromMap(source, name)));
                }
            }
        }
    }

    /** A {@link Source} that counts how many times its serialized form is read. */
    private static final class CountingSource implements Source {
        private final Source delegate;
        private int reads;

        CountingSource(Source delegate) {
            this.delegate = delegate;
        }

        @Override
        public XContentType sourceContentType() {
            return delegate.sourceContentType();
        }

        @Override
        public Map<String, Object> source() {
            reads++;
            return delegate.source();
        }

        @Override
        public BytesReference internalSourceRef() {
            reads++;
            return delegate.internalSourceRef();
        }

        @Override
        public Source filter(SourceFilter sourceFilter) {
            throw new UnsupportedOperationException();
        }
    }

    private static Map<String, Object> randomObject(List<String> keys, int depth) {
        Map<String, Object> object = new LinkedHashMap<>();
        for (int i = between(1, 4); i > 0; i--) {
            object.put(randomFrom(keys), randomValue(keys, depth));
        }
        return object;
    }

    private static Object randomValue(List<String> keys, int depth) {
        return switch (between(0, depth < 3 ? 6 : 4)) {
            case 0 -> randomAlphaOfLength(3);
            case 1 -> between(0, 9);
            case 2 -> between(0, 9) + 0.5;
            case 3 -> randomBoolean();
            case 4 -> null;
            case 5 -> randomObject(keys, depth + 1);
            case 6 -> randomList(0, 3, () -> randomValue(keys, depth + 1));
            default -> throw new AssertionError();
        };
    }

    private static Source json(String json) {
        return Source.fromBytes(new BytesArray(json), XContentType.JSON);
    }

    /** The unmapped leaves of a document, by path. */
    private Map<String, List<Object>> leaves(MapperService mapperService, String json) throws IOException {
        return leaves(extract(createSearchExecutionContext(mapperService).unmappedFields(), json));
    }

    /**
     * What {@link UnmappedFields#extract} returns for a document, or {@code null} if it has no unmapped field. Holding all
     * the values of the document must give the same bytes.
     */
    @Nullable
    private static Map<String, Object> extract(UnmappedFields unmappedFields, String json) throws IOException {
        BytesRef written = unmappedFields.extract(new BytesArray(json), XContentType.JSON, NoopCircuitBreaker.INSTANCE);
        assertThat(
            json,
            unmappedFields.extract(new BytesArray(json), XContentType.JSON, true, NoopCircuitBreaker.INSTANCE),
            equalTo(written)
        );
        if (written == null) {
            return null;
        }
        return XContentHelper.convertToMap(new BytesArray(written), true, XContentType.JSON).v2();
    }

    /** Whether the values of {@code json} are written as they are found. */
    private static boolean writtenAsFound(UnmappedFields unmappedFields, String json) throws IOException {
        return unmappedFields.extract(
            new BytesArray(json),
            XContentType.JSON,
            false,
            NoopCircuitBreaker.INSTANCE
        ) != UnmappedFields.SPELLED_IN_TWO_WAYS;
    }

    /** The values that {@code extracted} holds for each unmapped field, by path. Nothing in it may be an object, null or empty. */
    private static Map<String, List<Object>> leaves(@Nullable Map<String, Object> extracted) {
        Map<String, List<Object>> leaves = new LinkedHashMap<>();
        if (extracted != null) {
            assertFalse(extracted.isEmpty());
            extracted.forEach((path, value) -> {
                List<?> values = value instanceof List<?> list ? list : List.of(value);
                assertFalse(path, values.isEmpty());
                for (Object v : values) {
                    assertThat(path, v, not(anyOf(nullValue(), instanceOf(Map.class), instanceOf(List.class))));
                }
                leaves.put(path, new ArrayList<>(values));
            });
        }
        return leaves;
    }

    /**
     * The loader for a field of a shard whose mapping keeps the fields it does not declare.
     */
    private BlockLoader loader(String fieldName) throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        return createSearchExecutionContext(mapperService).unmappedFields().blockLoader(fieldName);
    }

    /**
     * Runs the loader over a single document and returns what landed in the block: {@code null}, a {@code String} for a single value,
     * or a {@code List<String>} for a multivalue. {@link TestBlock} stores a position entry as a nested list, so the shape of the
     * return value is itself part of what these tests assert.
     */
    private Object load(String fieldName, Map<String, Object> sourceMap) throws IOException {
        return loadFrom(fieldName, Source.fromMap(sourceMap, XContentType.JSON));
    }

    private Object loadFrom(String fieldName, Source source) throws IOException {
        BlockLoader loader = loader(fieldName);
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

    /** The stored fields of one document, which share what is read from it like {@link BlockLoaderStoredFieldsFromLeafLoader}. */
    private static BlockLoader.StoredFields storedFields(Source source) {
        return new BlockLoader.StoredFields() {
            private final Map<Object, Object> shared = new IdentityHashMap<>();

            @Override
            @SuppressWarnings("unchecked")
            public <T> T shared(Object key, IOFunction<BlockLoader.StoredFields, T> compute) throws IOException {
                if (shared.containsKey(key) == false) {
                    shared.put(key, compute.apply(this));
                }
                return (T) shared.get(key);
            }

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
