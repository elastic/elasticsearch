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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the object that {@link UnmappedFields#extract} writes for documents of every shape. Each document is read in
 * every format {@code _source} can be stored in, and both with its values written as they are found and with all of them
 * held: all of these must write the same bytes.
 */
public class UnmappedFieldsWalkTests extends MapperServiceTestCase {

    private UnmappedFields unmappedFields;

    @Before
    public void createUnmappedFields() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("runtime").startObject("rt").field("type", "keyword").endObject().endObject();
            b.startObject("properties");
            {
                b.startObject("kw").field("type", "keyword");
                b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
                b.endObject();
                b.startObject("al").field("type", "alias").field("path", "kw").endObject();
                b.startObject("rng").field("type", "integer_range").endObject();
                b.startObject("pt").field("type", "geo_point").endObject();
                b.startObject("flat").field("type", "flattened").endObject();
                b.startObject("nst").field("type", "nested").startObject("properties");
                b.startObject("declared").field("type", "keyword").endObject();
                b.endObject().endObject();
                b.startObject("obj").startObject("properties");
                b.startObject("kw").field("type", "keyword").endObject();
                b.endObject().endObject();
                b.startObject("off").field("type", "object").field("enabled", false).endObject();
                b.startObject("so").field("type", "object").field("subobjects", false).startObject("properties");
                b.startObject("a.b").field("type", "keyword").endObject();
                b.endObject().endObject();
            }
            b.endObject();
        }));
        unmappedFields = createSearchExecutionContext(mapperService).unmappedFields();
    }

    public void testNothingToWrite() throws IOException {
        assertThat(walk("{}"), nullValue());
        assertThat(walk("""
            { "a": null, "b": [], "c": {}, "d": [ null, [], {} ], "e": [ [ {} ] ], "f": { "g": { "h": null } } }"""), nullValue());
        assertThat(
            walk("""
                { "kw": "x", "al": "y", "rt": "z", "rng": { "gte": 1 }, "pt": [ 1, 2 ], "flat": { "k": 1 }, "obj": { "kw": "m" } }"""),
            nullValue()
        );
    }

    public void testScalars() throws IOException {
        assertThat(walk("""
            { "s": "text", "t": true, "f": false, "i": 1, "n": -7, "d": 1.5, "z": 0 }"""), equalTo("""
            {"s":"text","t":true,"f":false,"i":1,"n":-7,"d":1.5,"z":0}"""));
    }

    /**
     * A number is written as the number that was read, whatever its size and however it was spelled.
     */
    public void testNumbers() throws IOException {
        String json = """
            {
              "long": 9007199254740993,
              "min": -9223372036854775808,
              "big": 123456789012345678901234567890,
              "exponent": 1e2,
              "small": 1.0E-10,
              "negative_zero": -0.0,
              "digits": 0.1234567890123456789012345,
              "in_array": [ 9007199254740993, 123456789012345678901234567890, 1e2 ]
            }""";
        assertThat(
            XContentHelper.convertToMap(JsonXContent.jsonXContent, walk(json), true),
            equalTo(XContentHelper.convertToMap(JsonXContent.jsonXContent, json, true))
        );
    }

    public void testNamesAreWrittenAsTheyAre() throws IOException {
        assertThat(walk("""
            { "ключ": "значение", "q\\"uote": "line\\nbreak", "sp ace": "\\u0001", "emoji": "😀", "$$": 1, "a/b": 3 }"""), equalTo("""
            {"ключ":"значение","q\\"uote":"line\\nbreak","sp ace":"\\u0001","emoji":"😀","$$":1,"a/b":3}"""));
    }

    /**
     * A key may be empty, or start or end with a dot: its name is the keys that lead to it, joined with dots.
     */
    public void testEmptyKeysAndStrayDots() throws IOException {
        assertThat(walk("""
            { "": 1, "a": { "": 2 }, "b.": 3, ".c": 4, "d..e": 5, ".": 6 }"""), equalTo("""
            {"":1,"a.":2,"b.":3,".c":4,"d..e":5,".":6}"""));
        assertThat(walk("""
            { "a": { "": 1 }, "a.": 2 }"""), equalTo("""
            {"a.":[1,2]}"""));
        assertThat(walk("""
            { "": { "": 1 }, ".": 2 }"""), equalTo("""
            {".":[1,2]}"""));
    }

    public void testArrays() throws IOException {
        assertThat(walk("""
                    {
              "a": [ 1, [ 2, [ 3 ] ], { "b": [ 4, { "c": 5 } ] }, null, { "b": 6 } ],
              "z": [ [] ], "s": [ "only" ], "t": [ [ [ 7 ] ] ]
            }"""), equalTo("""
            {"a":[1,2,3],"a.b":[4,6],"a.b.c":5,"s":"only","t":7}"""));
        // The values of a field keep the order they are found in, and are not made distinct.
        assertThat(walk("""
            { "a": [ { "x": 1, "y": "p" }, { "y": "q", "x": 1 }, { "x": 0 } ] }"""), equalTo("""
            {"a.x":[1,1,0],"a.y":["p","q"]}"""));
        assertThat(walk("""
            { "a": [ "s", 1, true, 1.5, { "a": "inner" } ] }"""), equalTo("""
            {"a":["s",1,true,1.5],"a.a":"inner"}"""));
    }

    /**
     * A field can be spelled with a dot in a key and as a key of an object, at any depth: all are values of one field.
     */
    public void testFieldSpelledInSeveralWays() throws IOException {
        assertThat(walk("""
            { "a": { "b": { "c": 1 }, "b.c": 2 }, "a.b": { "c": 3 }, "a.b.c": 4 }"""), equalTo("""
            {"a.b.c":[1,2,3,4]}"""));
        assertThat(walk("""
            { "first": 0, "x": { "a.b": 1, "other": true, "a": { "b": 2 } }, "last": 9 }"""), equalTo("""
            {"first":0,"x.a.b":[1,2],"x.other":true,"last":9}"""));
        assertThat(walk("""
            { "a.b": [ 1, { "c": 2 } ], "a": [ { "b": 3 }, { "b.c": 4 } ] }"""), equalTo("""
            {"a.b":[1,3],"a.b.c":[2,4]}"""));
        // Keys that only share what precedes a dot are different fields.
        assertThat(walk("""
            { "http.method": "GET", "http.status": 200, "http": 1, "httpx": { "y": 2 } }"""), equalTo("""
            {"http.method":"GET","http.status":200,"http":1,"httpx.y":2}"""));
    }

    public void testMappedFieldsAreLeftOut() throws IOException {
        // A multi-field, an alias and a runtime field are fields of the shard. A path below a keyword is not.
        assertThat(walk("""
            { "kw": "x", "kw.raw": "y", "kw.sub": 1, "al": "z", "al.sub": 2, "rt": "r", "rt.sub": 3, "u": 4 }"""), equalTo("""
            {"kw.sub":1,"al.sub":2,"rt.sub":3,"u":4}"""));
        assertThat(walk("""
            { "kw": { "raw": "y", "sub": 1 }, "u": [ { "kw": 2 } ] }"""), equalTo("""
            {"kw.sub":1,"u.kw":2}"""));
        // Nothing at or below a field that takes an object as its value is an unmapped field, however it is spelled.
        assertThat(walk("""
            {
              "rng": { "gte": 1, "lt": 2 }, "rng.gte": 3, "rng.other": { "x": 4 },
              "pt": { "lat": 1, "lon": 2 }, "pt.lat": 5,
              "flat": { "k": { "deep": [ 1, 2 ] } }, "flat.k": 2, "flat.k.deep": 3,
              "rngx": 6
            }"""), equalTo("""
            {"rngx":6}"""));
    }

    public void testObjects() throws IOException {
        assertThat(walk("""
            { "obj": { "kw": "m", "other": 1, "deeper": { "kw": 2 } }, "obj.kw": "m2", "obj.x": 3 }"""), equalTo("""
            {"obj.other":1,"obj.deeper.kw":2,"obj.x":3}"""));
        assertThat(walk("""
            { "off": { "a": 1, "b": { "c": [ true, false ] }, "kw": "not the mapped one" } }"""), equalTo("""
            {"off.a":1,"off.b.c":[true,false],"off.kw":"not the mapped one"}"""));
        // An object that does not have objects below it maps a name with a dot, which a document can still spell as an object.
        assertThat(walk("""
            { "so": { "a.b": "m", "a.c": 1, "a": { "b": "m2", "d": 2 } } }"""), equalTo("""
            {"so.a.c":1,"so.a.d":2}"""));
    }

    public void testNestedObjects() throws IOException {
        assertThat(walk("""
            { "nst": [ { "declared": "d", "extra": 1 }, { "extra": 2, "deep": { "x": [ 3, 4 ] } } ], "nst.extra": 5 }"""), equalTo("""
            {"nst.extra":[1,2,5],"nst.deep.x":[3,4]}"""));
        assertThat(walk("""
            { "nst": { "declared": "d", "extra": 1 } }"""), equalTo("""
            {"nst.extra":1}"""));
        assertThat(walk("""
            { "nst": [ { "declared": "d" }, { "declared": [ "e", "f" ] } ] }"""), nullValue());
    }

    /**
     * The walk never sees a key twice in one object, which it could not write: the parser rejects such a document.
     */
    public void testDuplicateKeys() {
        for (String json : List.of("""
            { "a": 1, "a": 2 }""", """
            { "o": { "a": { "b": 1 }, "a": { "c": 2 } } }""")) {
            expectThrows(Exception.class, () -> unmappedFields.extract(new BytesArray(json), XContentType.JSON));
        }
    }

    public void testDeepDocument() throws IOException {
        int depth = 200;
        StringBuilder json = new StringBuilder();
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            json.append("{\"n").append(i).append("\":");
            name.append(i == 0 ? "" : ".").append('n').append(i);
        }
        json.append("[{\"leaf\":1},{\"leaf\":2}]").append("}".repeat(depth));
        assertThat(walk(json.toString()), equalTo("{\"" + name + ".leaf\":[1,2]}"));
    }

    public void testWideDocument() throws IOException {
        int fields = 10_000;
        StringBuilder json = new StringBuilder("{");
        StringBuilder expected = new StringBuilder("{");
        for (int i = 0; i < fields; i++) {
            json.append(i == 0 ? "" : ",").append("\"g").append(i % 100).append('_').append(i).append("\":{\"f\":").append(i).append('}');
            expected.append(i == 0 ? "" : ",").append("\"g").append(i % 100).append('_').append(i).append(".f\":").append(i);
        }
        assertThat(walk(json.append('}').toString()), equalTo(expected.append('}').toString()));
    }

    public void testLongArrayOfObjects() throws IOException {
        int elements = 50_000;
        StringBuilder json = new StringBuilder("{\"before\":0,\"a\":[");
        for (int i = 0; i < elements; i++) {
            json.append(i == 0 ? "" : ",").append("{\"x\":").append(i).append('}');
        }
        Map<String, Object> written = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            walk(json.append("],\"after\":1}").toString()),
            true
        );
        assertThat(List.copyOf(written.keySet()), equalTo(List.of("before", "a.x", "after")));
        List<?> values = (List<?>) written.get("a.x");
        assertThat(values, hasSize(elements));
        assertThat(values.get(elements - 1), equalTo(elements - 1));
    }

    /**
     * A binary value, which only some formats can hold, is written as text in base 64.
     */
    public void testBinaryValues() throws IOException {
        byte[] bytes = randomByteArrayOfLength(between(1, 64));
        String text = Base64.getEncoder().encodeToString(bytes);
        for (XContentType type : List.of(XContentType.SMILE, XContentType.CBOR)) {
            BytesReference source;
            try (XContentBuilder builder = XContentBuilder.builder(type.xContent())) {
                builder.startObject().field("bin", bytes);
                builder.startArray("in_array").value(bytes).startObject().field("bin", bytes).endObject().endArray();
                source = BytesReference.bytes(builder.endObject());
            }
            assertThat(
                type.toString(),
                walk(source, type, null),
                equalTo("{\"bin\":\"" + text + "\",\"in_array\":\"" + text + "\",\"in_array.bin\":\"" + text + "\"}")
            );
        }
    }

    /**
     * A filter leaves out the fields it does not include, wherever and however they are spelled, and the walk does not
     * enter an object or an array below which it includes nothing.
     */
    public void testFilter() throws IOException {
        String json = """
            { "a": { "b": 1, "c": { "d": 2 } }, "a.b": 3, "e": [ { "f": 4 }, { "g": 5 } ], "skip": { "x": { "y": [ 6, { "z": 7 } ] } } }""";
        assertThat(walk(json), equalTo("""
            {"a.b":[1,3],"a.c.d":2,"e.f":4,"e.g":5,"skip.x.y":6,"skip.x.y.z":7}"""));
        assertThat(walk(json, Set.of("a.b", "e.g")), equalTo("""
            {"a.b":[1,3],"e.g":5}"""));
        assertThat(walk(json, Set.of("a.c.d", "skip.x.y.z")), equalTo("""
            {"a.c.d":2,"skip.x.y.z":7}"""));
        assertThat(walk(json, Set.of("e")), nullValue());
        assertThat(walk(json, Set.of()), nullValue());
        // Mapped fields stay out whatever the filter includes.
        assertThat(walk("""
            { "kw": "m", "rng": { "gte": 1 }, "u": 1 }""", Set.of("kw", "rng.gte", "u")), equalTo("""
            {"u":1}"""));
    }

    /**
     * What a filter includes is what is written without one, less the fields it does not include, in the same order.
     */
    public void testFilterTakesFieldsOutOfWhatIsWritten() throws IOException {
        for (String json : List.of("""
            { "a": [ 1, [ 2, [ 3 ] ], { "b": [ 4, { "c": 5 } ] }, null, { "b": 6 } ], "s": [ "only" ] }""", """
            { "a": { "b": { "c": 1 }, "b.c": 2 }, "a.b": { "c": 3 }, "a.b.c": 4, "z": 0 }""", """
            { "first": 0, "x": { "a.b": 1, "other": true, "a": { "b": 2 } }, "last": 9 }""", """
            { "nst": [ { "declared": "d", "extra": 1 }, { "extra": 2, "deep": { "x": [ 3, 4 ] } } ], "nst.extra": 5 }""", """
            { "obj": { "kw": "m", "other": 1, "deeper": { "kw": 2 } }, "obj.kw": "m2", "obj.x": 3 }""")) {
            Map<String, Object> all = XContentHelper.convertToMap(JsonXContent.jsonXContent, walk(json), true);
            Set<String> included = new HashSet<>(randomSubsetOf(all.keySet()));
            Map<String, Object> expected = new LinkedHashMap<>(all);
            expected.keySet().retainAll(included);
            String written = walk(json, included);
            if (expected.isEmpty()) {
                assertThat(json, written, nullValue());
            } else {
                Map<String, Object> filtered = XContentHelper.convertToMap(JsonXContent.jsonXContent, written, true);
                assertThat(json, List.copyOf(filtered.entrySet()), equalTo(List.copyOf(expected.entrySet())));
            }
        }
    }

    /**
     * The walk asks a filter about an object or an array before entering it, and does not enter it when the filter
     * includes nothing below it.
     */
    public void testFilterSkipsWhatItIncludesNothingBelow() throws IOException {
        List<String> asked = new ArrayList<>();
        UnmappedFields.Filter keep = new UnmappedFields.Filter() {
            @Override
            public boolean includes(String path) {
                asked.add(path);
                return path.equals("keep");
            }

            @Override
            public boolean mayIncludeBelow(String path) {
                asked.add(path + ".");
                return false;
            }
        };
        BytesRef written = unmappedFields.extract(new BytesArray("""
            { "skip": { "x": { "y": [ 6, { "z": 7 } ] } }, "arr": [ { "q": 1 }, 2 ], "keep": 1 }"""), XContentType.JSON, keep);
        assertThat(written.utf8ToString(), equalTo("""
            {"keep":1}"""));
        assertThat(asked, equalTo(List.of("skip.", "arr", "arr.", "keep")));
    }

    /** A filter that includes exactly {@code included}. */
    private static UnmappedFields.Filter filter(Set<String> included) {
        return new UnmappedFields.Filter() {
            @Override
            public boolean includes(String path) {
                return included.contains(path);
            }

            @Override
            public boolean mayIncludeBelow(String path) {
                return included.stream().anyMatch(field -> field.startsWith(path + "."));
            }
        };
    }

    /**
     * What is written for {@code json}, or {@code null} if it has no unmapped field, which has to be the same in every
     * format {@code _source} can be stored in.
     */
    @Nullable
    private String walk(String json) throws IOException {
        return walk(json, null);
    }

    /**
     * What is written for {@code json} with a {@link #filter} of {@code included}, or without one if it is {@code null}.
     */
    @Nullable
    private String walk(String json, @Nullable Set<String> included) throws IOException {
        String written = walk(new BytesArray(json), XContentType.JSON, included);
        for (XContentType type : List.of(XContentType.SMILE, XContentType.CBOR, XContentType.YAML)) {
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json);
                XContentBuilder builder = XContentBuilder.builder(type.xContent())
            ) {
                builder.copyCurrentStructure(parser);
                assertThat(type + " " + json, walk(BytesReference.bytes(builder), type, included), equalTo(written));
            }
        }
        return written;
    }

    /**
     * What is written for {@code source}, which has to be the same when all of its values are held.
     */
    @Nullable
    private String walk(BytesReference source, XContentType type, @Nullable Set<String> included) throws IOException {
        if (included == null) {
            BytesRef written = unmappedFields.extract(source, type);
            assertThat(unmappedFields.extract(source, type, true), equalTo(written));
            return written == null ? null : written.utf8ToString();
        }
        BytesRef written = unmappedFields.extract(source, type, filter(included));
        assertThat(unmappedFields.extract(source, type, filter(included), true), equalTo(written));
        return written == null ? null : written.utf8ToString();
    }
}
