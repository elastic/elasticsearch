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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
            .allFieldsBlockLoader(2.0, UnmappedFields.Filter.ALL);
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
     * A document that cannot be read, or whose reservation does not fit, fails the read and leaves nothing reserved.
     */
    public void testAllFieldsLoaderReleasesItsReservationWhenItFails() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", false)));
        BlockLoader loader = createSearchExecutionContext(mapperService).unmappedFields()
            .allFieldsBlockLoader(2.0, UnmappedFields.Filter.ALL);
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
                ? unmappedFields.allFieldsBlockLoader(2.0, UnmappedFields.Filter.ALL)
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
        UnmappedFields unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
        for (int iteration = 0; iteration < 200; iteration++) {
            Map<String, Object> source = randomObject(keys, 0);
            Source parsed = Source.fromMap(source, XContentType.JSON);
            Map<String, List<String>> walked = new HashMap<>();
            leaves(extract(unmappedFields, parsed.internalSourceRef().utf8ToString())).forEach(
                (path, values) -> walked.put(path, values.stream().map(Object::toString).toList())
            );
            Set<String> names = new HashSet<>(walked.keySet());
            for (int i = 0; i < 20; i++) {
                names.add(String.join(".", randomList(1, 3, () -> randomFrom(keys))));
            }
            for (String name : names) {
                if (mapperService.mappingLookup().getFieldType(name) != null) {
                    assertThat(name + " in " + source, walked.get(name), nullValue());
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
            }
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
        BytesRef written = unmappedFields.extract(new BytesArray(json), XContentType.JSON);
        assertThat(json, unmappedFields.extract(new BytesArray(json), XContentType.JSON, true), equalTo(written));
        if (written == null) {
            return null;
        }
        return XContentHelper.convertToMap(new BytesArray(written), true, XContentType.JSON).v2();
    }

    /** Whether the values of {@code json} are written as they are found. */
    private static boolean writtenAsFound(UnmappedFields unmappedFields, String json) throws IOException {
        return unmappedFields.extract(new BytesArray(json), XContentType.JSON, false) != UnmappedFields.SPELLED_IN_TWO_WAYS;
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
