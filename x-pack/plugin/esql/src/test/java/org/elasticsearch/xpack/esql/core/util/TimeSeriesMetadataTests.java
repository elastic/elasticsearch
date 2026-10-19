/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Tests identity preservation at the boundary between metadata values and their encoded representation. */
public class TimeSeriesMetadataTests extends ESTestCase {
    public void testCanonicalBytesIgnoreInputOrderAndJsonSpelling() throws IOException {
        BytesRef expected = new BytesRef("{\"a\":\"1\",\"b\":\"2\"}");
        assertEquals(expected, encode(TimeSeriesMetadata.read(new BytesRef("{ \"b\": \"2\", \"\\u0061\": \"\\u0031\" }"))));
        assertEquals(expected, encode(TimeSeriesMetadata.of(Map.of("b", "2", "a", "1"))));
    }

    public void testKeysNormalizeEquivalentMetadataEncodings() throws IOException {
        TimeSeriesMetadata expected = TimeSeriesMetadata.of(Map.of("a", "1", "b", "2"));
        TimeSeriesMetadata decoded = TimeSeriesMetadata.read(
            new BytesRef("{ \"b\": \"2\", \"\\u0061\": \"\\u0031\", \"empty\":\"\", \"null\":null }")
        );
        assertEquals(key(expected), key(decoded));
        assertNotEquals(key(expected), key(expected.set("a", "2")));
        assertNotEquals(key(expected), key(expected.unset(List.of("b"))));
    }

    public void testKeyUsesIngestHashOfNormalizedStringLabels() throws IOException {
        var labels = Map.of("job", "api", "rack", "1", "dot.name", "with\nnewlines", "日本語", "🚀");
        var builder = new TsidBuilder();
        List<String> names = new ArrayList<>(labels.keySet());
        Collections.shuffle(names, random());
        for (String name : names) {
            builder.addStringDimension(name, labels.get(name));
        }
        BytesRef actual = key(TimeSeriesMetadata.of(labels));
        assertEquals(16, actual.length);
        assertEquals(new BytesRef(builder.hash().getBytes()), actual);
    }

    public void testEmptyKeyUsesIngestHashWithoutStorageLayout() throws IOException {
        BytesRef expected = new BytesRef(new TsidBuilder().hash().getBytes());
        assertEquals(16, expected.length);
        for (BytesRef metadata : new BytesRef[] { null, new BytesRef("{}"), new BytesRef("{\"null\":null,\"empty\":\"\"}") }) {
            assertEquals(expected, key(TimeSeriesMetadata.read(metadata)));
        }
        assertEquals(expected, key(TimeSeriesMetadata.of(Map.of("pod", "a")).set("pod", "")));
    }

    public void testAbsentNullAndEmptyHaveSameIdentity() throws IOException {
        assertSame(TimeSeriesMetadata.EMPTY, TimeSeriesMetadata.read(null));
        assertSame(TimeSeriesMetadata.EMPTY, TimeSeriesMetadata.read(new BytesRef("{}")));
        assertSame(TimeSeriesMetadata.EMPTY, TimeSeriesMetadata.read(new BytesRef("{\"null\":null,\"empty\":\"\"}")));
        var labels = new HashMap<String, String>();
        labels.put("null", null);
        labels.put("empty", "");
        labels.put("present", " ");
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of(labels);
        assertEquals(List.of(), metadata.get("null"));
        assertEquals(List.of(), metadata.get("empty"));
        assertEquals(List.of(), metadata.get("missing"));
        assertEquals(List.of(" "), metadata.get("present"));
        assertEquals(new BytesRef("{\"present\":\" \"}"), encode(metadata));
    }

    public void testReadHonorsByteSlice() throws IOException {
        String json = "{\"label\":\"значение\"}";
        byte[] bytes = ("invalid prefix" + json + "invalid suffix").getBytes(StandardCharsets.UTF_8);
        BytesRef slice = new BytesRef(bytes, "invalid prefix".length(), json.getBytes(StandardCharsets.UTF_8).length);
        assertEquals(List.of("значение"), TimeSeriesMetadata.read(slice).get("label"));
        assertEquals(new BytesRef(json), encode(TimeSeriesMetadata.read(slice)));
    }

    public void testMutationDoesNotChangeExistingMetadata() throws IOException {
        var input = new HashMap<>(Map.of("a", "1", "b", "2"));
        TimeSeriesMetadata original = TimeSeriesMetadata.of(input);
        input.clear();
        TimeSeriesMetadata replaced = original.set("a", "changed");
        var overlay = new HashMap<String, String>();
        overlay.put("a", null);
        overlay.put("b", "");
        overlay.put("c", "3");
        TimeSeriesMetadata overlaid = original.withLabels(overlay);
        overlay.clear();
        assertEquals(new BytesRef("{\"a\":\"1\",\"b\":\"2\"}"), encode(original));
        assertEquals(new BytesRef("{\"a\":\"changed\",\"b\":\"2\"}"), encode(replaced));
        assertEquals(new BytesRef("{\"c\":\"3\"}"), encode(overlaid));
        assertEquals(new BytesRef("{\"b\":\"2\"}"), encode(original.unset(List.of("a", "missing", "a"))));
        assertSame(TimeSeriesMetadata.EMPTY, original.unset(List.of("a", "b")));
        assertEquals(encode(original.unset(List.of("a"))), encode(original.set("a", randomBoolean() ? null : "")));
    }

    public void testPromotingAndRestoringLabelsPreservesIdentity() throws IOException {
        TimeSeriesMetadata original = TimeSeriesMetadata.of(Map.of("host", "one", "job", "test", "cluster", "prod"));
        List<String> host = original.get("host");
        TimeSeriesMetadata residual = original.unset(List.of("host"));
        assertEquals(key(original), key(residual.withLabelValues(Map.of("host", host))));
        assertNotEquals(key(original), key(residual.withLabels(Map.of("host", "two"))));
    }

    public void testEscapedAndUnicodeLabelsRoundTrip() throws IOException {
        var labels = Map.of("dot.name", "with\nnewlines\tand\\slashes\"", "日本語", "🚀", "embedded\u0000nul", "\u0000");
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of(labels);
        TimeSeriesMetadata decoded = TimeSeriesMetadata.read(encode(metadata));
        for (var label : labels.entrySet()) {
            assertEquals(List.of(label.getValue()), decoded.get(label.getKey()));
        }
        assertEquals(encode(metadata), encode(decoded));
    }

    public void testRandomizedLabelOrderProducesIdenticalKeys() throws IOException {
        var labels = new HashMap<String, String>();
        for (int i = 0, count = randomIntBetween(1, 40); i < count; i++) {
            labels.put(randomAlphaOfLengthBetween(1, 20), randomUnicodeOfLengthBetween(1, 30));
        }
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of(labels);
        BytesRef expected = key(metadata);
        List<String> names = new ArrayList<>(labels.keySet());
        Collections.shuffle(names, random());
        var reordered = new LinkedHashMap<String, String>();
        for (String name : names) {
            reordered.put(name, labels.get(name));
        }
        assertEquals(expected, key(TimeSeriesMetadata.of(reordered)));
        assertEquals(expected, key(TimeSeriesMetadata.read(encode(metadata))));
    }

    public void testDifferentLabelBoundariesHaveDifferentKeys() throws IOException {
        assertNotEquals(key(TimeSeriesMetadata.of(Map.of("a", "bc"))), key(TimeSeriesMetadata.of(Map.of("ab", "c"))));
        assertNotEquals(key(TimeSeriesMetadata.of(Map.of("a", "1,b=2"))), key(TimeSeriesMetadata.of(Map.of("a", "1", "b", "2"))));
    }

    public void testMultipleValuesNormalizeEmptyMembersAndSingletons() throws IOException {
        var labels = new HashMap<String, List<String>>();
        labels.put("region", Arrays.asList("west", "", null, "east", "west"));
        labels.put("single", List.of("one"));
        labels.put("empty", List.of());
        labels.put("absent", Arrays.asList(null, ""));
        labels.put("null", null);
        var metadata = TimeSeriesMetadata.ofValues(labels);
        assertEquals(List.of("west", "east", "west"), metadata.get("region"));
        assertEquals(List.of(), metadata.get("absent"));
        BytesRef expected = new BytesRef("{\"region\":[\"west\",\"east\",\"west\"],\"single\":\"one\"}");
        assertEquals(expected, encode(metadata));
        assertEquals(expected, encode(TimeSeriesMetadata.read(encode(metadata))));
        assertEquals(
            expected,
            encode(TimeSeriesMetadata.read(new BytesRef("{\"single\":[\"one\"],\"region\":[\"west\",null,\"\",\"east\",\"west\"]}")))
        );
        assertSame(TimeSeriesMetadata.EMPTY, TimeSeriesMetadata.read(new BytesRef("{\"empty\":[],\"absent\":[null,\"\"]}")));
    }

    public void testMultipleValueListsAreImmutable() throws IOException {
        var regions = new ArrayList<>(List.of("west", "east"));
        var labels = new HashMap<String, List<String>>();
        labels.put("region", regions);
        var original = TimeSeriesMetadata.ofValues(labels);
        regions.clear();
        labels.clear();
        assertEquals(List.of("west", "east"), original.get("region"));
        expectThrows(UnsupportedOperationException.class, () -> original.get("region").clear());

        regions.add("north");
        var replaced = original.withLabelValues(Map.of("region", regions));
        regions.add("south");
        assertEquals(List.of("north"), replaced.get("region"));
        assertEquals(List.of("west", "east"), original.get("region"));
        assertSame(TimeSeriesMetadata.EMPTY, original.withLabelValues(Map.of("region", List.of())));
        assertSame(TimeSeriesMetadata.EMPTY, original.unset(List.of("region")));
    }

    /** The ingest builder hashes every array member in source order; no joining, sorting or deduplication is added here. */
    public void testMultipleValueKeysUseIngestOrderAndDuplicates() throws IOException {
        var values = List.of("west", "east", "west", "north");
        var builder = new TsidBuilder();
        builder.addStringDimension("job", "api");
        values.forEach(value -> builder.addStringDimension("region", value));
        var metadata = TimeSeriesMetadata.ofValues(Map.of("region", values, "job", List.of("api")));
        assertEquals(new BytesRef(builder.hash().getBytes()), key(metadata));
        assertNotEquals(key(metadata), key(metadata.withLabelValues(Map.of("region", values.reversed()))));
        assertNotEquals(key(metadata), key(metadata.withLabelValues(Map.of("region", List.of("west", "east", "north")))));
        var residual = metadata.unset(List.of("region"));
        assertEquals(key(metadata), key(residual.withLabelValues(Map.of("region", metadata.get("region")))));
    }

    public void testRejectsDuplicateLabelsIncludingAbsentValues() {
        for (String json : List.of("{\"a\":\"1\",\"a\":\"2\"}", "{\"a\":null,\"a\":\"2\"}", "{\"a\":\"\",\"\\u0061\":\"2\"}")) {
            expectThrows(IllegalArgumentException.class, () -> TimeSeriesMetadata.read(new BytesRef(json)));
        }
    }

    public void testRejectsInvalidShapesAndTrailingContent() {
        for (String json : List.of(
            "",
            "[]",
            "null",
            "\"value\"",
            "{\"a\":{\"nested\":\"value\"}}",
            "{\"a\":[[\"value\"]]}",
            "{\"a\":[1]}",
            "{\"a\":[true]}",
            "{\"a\":[{}]}",
            "{\"a\":[\"unterminated\"",
            "{\"a\":1}",
            "{\"a\":true}",
            "{} {}",
            "{} null",
            "{} invalid",
            "{\"a\":\"unterminated",
            "{\"a\":\"value\"",
            "{\"\":\"value\"}"
        )) {
            expectThrows(IllegalArgumentException.class, () -> TimeSeriesMetadata.read(new BytesRef(json)));
        }
    }

    public void testRejectsInvalidNames() {
        for (String name : new String[] { null, "" }) {
            expectThrows(IllegalArgumentException.class, () -> TimeSeriesMetadata.of(Collections.singletonMap(name, "value")));
            expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesMetadata.ofValues(Collections.singletonMap(name, List.of("value")))
            );
            expectThrows(IllegalArgumentException.class, () -> TimeSeriesMetadata.EMPTY.get(name));
            expectThrows(IllegalArgumentException.class, () -> TimeSeriesMetadata.EMPTY.set(name, "value"));
            expectThrows(IllegalArgumentException.class, () -> TimeSeriesMetadata.EMPTY.unset(Collections.singleton(name)));
        }
    }

    public void testWritersLeaveSuppliedStreamOpen() throws IOException {
        var output = new ByteArrayOutputStream() {
            @Override
            public void close() {
                fail("metadata does not own its output stream");
            }
        };
        TimeSeriesMetadata.of(Map.of("a", "1")).writeTo(output);
        assertEquals("{\"a\":\"1\"}", output.toString(StandardCharsets.UTF_8));
        output.reset();
        TimeSeriesMetadata.of(Map.of("b", "2")).renderTo(output);
        assertEquals("{\"b\":\"2\"}", output.toString(StandardCharsets.UTF_8));
        output.reset();
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of(Map.of("c", "3"));
        metadata.writeKeyTo(output);
        assertEquals(key(metadata), new BytesRef(output.toByteArray()));
    }

    private static BytesRef key(TimeSeriesMetadata metadata) throws IOException {
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1)).withCircuitBreaking();
        try (var output = new ReleasableBytesStreamOutput(bigArrays)) {
            metadata.writeKeyTo(output);
            return BytesRef.deepCopyOf(output.bytes().toBytesRef());
        }
    }

    private static BytesRef encode(TimeSeriesMetadata metadata) throws IOException {
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1)).withCircuitBreaking();
        try (var output = new ReleasableBytesStreamOutput(bigArrays)) {
            metadata.writeTo(output);
            return BytesRef.deepCopyOf(output.bytes().toBytesRef());
        }
    }
}
