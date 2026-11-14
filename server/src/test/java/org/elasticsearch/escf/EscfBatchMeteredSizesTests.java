/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.plugins.internal.MeteringModel;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * Holds the column sweep of {@link EscfBatch#meteredRowSizes} to the accounting applied to the tokens of the original source, which
 * never looks at the batch, so the sweep is not checked against itself. The documents are encoded through {@link EscfEncoder} with
 * every value kind it produces, so every column kind (unions, columnar arrays of each child kind, inline arrays and objects, binary)
 * is metered, on whole batches and on random slices, under models whose charges are not additive over values.
 */
public class EscfBatchMeteredSizesTests extends ESTestCase {

    private static final String[] STRINGS = { "", "plain", "héllo wörld", "✓ 日本語 🚀", "a somewhat longer ascii string value" };

    private static MeteringModel randomModel(Set<String> excludedRootFields) {
        final int stringExtra = randomIntBetween(0, 3);
        return model(excludedRootFields, stringExtra, stringExtra + randomIntBetween(1, 3), randomIntBetween(0, 9), randomIntBetween(0, 9));
    }

    /**
     * A model whose string and binary charges carry a surcharge per value, so the charge of several values differs from the charge of
     * their combined length, and a binary value is never charged like a string of the same length.
     */
    private static MeteringModel model(Set<String> excludedRootFields, int stringExtra, int binaryExtra, long number, long bool) {
        return new MeteringModel() {
            @Override
            public boolean hasExcludedRootField(String fieldName) {
                final int dot = fieldName.indexOf('.');
                return excludedRootFields.contains(dot < 0 ? fieldName : fieldName.substring(0, dot));
            }

            @Override
            public long stringBytes(int utf8Length) {
                return utf8Length + stringExtra;
            }

            @Override
            public long binaryBytes(int length) {
                return length + binaryExtra;
            }

            @Override
            public long numberBytes() {
                return number;
            }

            @Override
            public long booleanBytes() {
                return bool;
            }
        };
    }

    /**
     * The size the sequential path reports: the model applied to the value tokens of the original source, with the root field cut at
     * the first dot the way a row-based parser sees it after expanding dotted names.
     */
    private static long sequentialSize(BytesReference source, XContentType type, MeteringModel model) throws IOException {
        try (XContentParser parser = type.xContent().createParser(XContentParserConfiguration.EMPTY, source.streamInput())) {
            long size = 0;
            int depth = 0;
            boolean charged = true;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null) {
                switch (token) {
                    case START_OBJECT, START_ARRAY -> depth++;
                    case END_OBJECT, END_ARRAY -> depth--;
                    case FIELD_NAME -> {
                        if (depth == 1) {
                            charged = model.hasExcludedRootField(parser.currentName()) == false;
                        }
                    }
                    case VALUE_STRING -> {
                        if (charged) {
                            final String text = parser.text();
                            size += model.stringBytes(UnicodeUtil.calcUTF16toUTF8Length(text, 0, text.length()));
                        }
                    }
                    case VALUE_EMBEDDED_OBJECT -> size += charged ? model.binaryBytes(parser.binaryValue().length) : 0;
                    case VALUE_NUMBER -> size += charged ? model.numberBytes() : 0;
                    case VALUE_BOOLEAN -> size += charged ? model.booleanBytes() : 0;
                    case VALUE_NULL -> {
                    }
                    default -> throw new AssertionError(token);
                }
            }
            return size;
        }
    }

    /** Pins the formula on hand-picked documents, so a change to it shows up as a concrete number and not only as a mismatch. */
    public void testKnownSizes() throws IOException {
        assertKnownSize("{\"s\":\"héllo\"}", 6);
        assertKnownSize("{\"s\":\"\"}", 0);
        assertKnownSize("{\"l\":1}", 8);
        assertKnownSize("{\"d\":1.5}", 8);
        assertKnownSize("{\"b\":true}", 1);
        assertKnownSize("{\"ba\":[true,false,true]}", 3);
        assertKnownSize("{\"n\":null}", 0);
        assertKnownSize("{\"e\":{}}", 0);
        assertKnownSize("{}", 0);
        assertKnownSize("{\"a\":[1,2,3]}", 24);
        assertKnownSize("{\"a\":[\"a\",\"\",\"日本\"]}", 7);
        assertKnownSize("{\"a\":[1,\"two\",3.5,true,null]}", 8 + 3 + 8 + 1);
        assertKnownSize("{\"a\":[[1,2],[\"x\"]]}", 16 + 1);
        assertKnownSize("{\"a\":[{\"x\":1},{\"y\":\"two\"}]}", 8 + 3);
        assertKnownSize("{\"o\":{\"k\":7,\"s\":\"ab\",\"i\":{\"z\":2.5}}}", 8 + 2 + 8);
    }

    /** The root field is the first dot-separated segment, whichever way the document spells it, and matches whole names only. */
    public void testExcludedRootField() throws IOException {
        final MeteringModel excludingEx = model(Set.of("ex"), 0, 0, 8, 1);
        for (String json : List.of(
            "{\"ex\":{\"k\":1,\"s\":\"abc\"},\"s\":\"abc\"}",
            "{\"ex.k\":1,\"ex.s\":\"abc\",\"s\":\"abc\"}",
            "{\"ex\":{\"k.l\":1},\"s\":\"abc\"}",
            "{\"ex\":[{\"k\":1},{\"k\":2}],\"s\":\"abc\"}",
            "{\"ex\":\"zzzz\",\"s\":\"abc\"}"
        )) {
            assertKnownSize(json, excludingEx, 3);
        }
        assertKnownSize("{\"ex\":1,\"exact\":\"abc\"}", excludingEx, 3);
    }

    public void testRandomDocuments() throws IOException {
        for (int iteration = 0; iteration < 10; iteration++) {
            final int docCount = randomIntBetween(1, 150);
            final List<BytesReference> sources = new ArrayList<>(docCount);
            for (int d = 0; d < docCount; d++) {
                sources.add(toJson(randomDocument()));
            }
            final MeteringModel model = randomModel(Set.copyOf(randomSubsetOf(FIELDS)));
            try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
                assertSizesMatchSequential(batch, sources, XContentType.JSON, model);
                for (int s = 0; s < 5; s++) {
                    final int from = randomIntBetween(0, docCount);
                    final int to = randomIntBetween(from, docCount);
                    final SourceBatch slice = batch.slice(from, to);
                    assertSizesMatchSequential(slice, sources.subList(from, to), XContentType.JSON, model);
                    assertThat(
                        "slice [" + from + ", " + to + ")",
                        slice.meteredRowSizes(model),
                        equalTo(Arrays.copyOfRange(batch.meteredRowSizes(model), from, to))
                    );
                }
            }
        }
    }

    /** Binary values only come from binary content types, which the encoder does not take, so these columns are built directly. */
    public void testBinaryValues() {
        final int docCount = randomIntBetween(1, 50);
        final boolean withArrays = randomBoolean();
        final MeteringModel model = randomModel(randomBoolean() ? Set.of() : Set.of("bins"));
        final SourceSchema schema = new SourceSchema();
        schema.appendLeaf("bin", 0);
        schema.appendLeaf("bins", 0);
        schema.appendLeaf("s", 0);
        final long[] expected = new long[docCount];
        try (
            EscfColumnBuilder bin = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
            EscfColumnBuilder bins = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
            EscfColumnBuilder s = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT)
        ) {
            for (int d = 0; d < docCount; d++) {
                if (randomBoolean()) {
                    final BytesRef value = new BytesRef(randomByteArrayOfLength(randomIntBetween(0, 20)));
                    bin.setBinary(d, value);
                    expected[d] += model.binaryBytes(value.length);
                }
                if (withArrays && randomBoolean()) {
                    bins.beginArray(d);
                    for (int i = randomIntBetween(0, 3); i > 0; i--) {
                        final BytesRef value = new BytesRef(randomByteArrayOfLength(randomIntBetween(0, 20)));
                        bins.appendBinary(value);
                        expected[d] += model.hasExcludedRootField("bins") ? 0 : model.binaryBytes(value.length);
                    }
                    bins.endArray();
                }
                if (randomBoolean()) {
                    final BytesRef value = new BytesRef(randomFrom(STRINGS));
                    s.setString(d, value);
                    expected[d] += model.stringBytes(value.length);
                }
            }
            final EscfColumnData[] columns = { bin.finish(docCount), bins.finish(docCount), s.finish(docCount) };
            try (EscfBatch batch = EscfBatch.of(schema, docCount, columns, () -> {})) {
                assertThat(batch.meteredRowSizes(model), equalTo(expected));
            }
        }
    }

    private static final List<String> FIELDS = List.of(
        "s",
        "l",
        "d",
        "b",
        "n",
        "e",
        "arr_s",
        "arr_l",
        "arr_d",
        "arr_mixed",
        "arr_nested",
        "arr_obj",
        "arr_promote",
        "promote",
        "obj",
        "dotted"
    );

    /** Asserts {@code expected} under a fixed accounting: the UTF-8 bytes of a string, 8 per number, 1 per boolean, nothing excluded. */
    private static void assertKnownSize(String json, long expected) throws IOException {
        assertKnownSize(json, model(Set.of(), 0, 0, 8, 1), expected);
    }

    private static void assertKnownSize(String json, MeteringModel model, long expected) throws IOException {
        final List<BytesReference> sources = List.of(new BytesArray(json));
        assertThat(json, sequentialSize(sources.get(0), XContentType.JSON, model), equalTo(expected));
        try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
            assertThat(json, batch.meteredRowSizes(model), equalTo(new long[] { expected }));
        }
    }

    private static void assertSizesMatchSequential(SourceBatch batch, List<BytesReference> sources, XContentType type, MeteringModel model)
        throws IOException {
        final long[] sizes = batch.meteredRowSizes(model);
        assertThat(sizes.length, equalTo(batch.docCount()));
        for (int d = 0; d < batch.docCount(); d++) {
            final long expected = sequentialSize(sources.get(d), type, model);
            final String description = type == XContentType.JSON ? sources.get(d).utf8ToString() : "document " + d;
            assertThat(description, sizes[d], equalTo(expected));
        }
    }

    /**
     * A document over a fixed set of fields, each left out at random so the columns are sparse, and some of them of a kind
     * that changes between documents so the column promotes to a union.
     */
    private static Map<String, Object> randomDocument() {
        final Map<String, Object> doc = new LinkedHashMap<>();
        maybePut(doc, "s", () -> randomFrom(STRINGS));
        maybePut(doc, "l", ESTestCase::randomLong);
        maybePut(doc, "d", ESTestCase::randomDouble);
        maybePut(doc, "b", ESTestCase::randomBoolean);
        maybePut(doc, "n", () -> null);
        maybePut(doc, "e", Map::of);
        maybePut(doc, "arr_s", () -> randomList(0, 6, () -> randomFrom(STRINGS)));
        maybePut(doc, "arr_l", () -> randomList(0, 6, ESTestCase::randomLong));
        maybePut(doc, "arr_d", () -> randomList(0, 6, ESTestCase::randomDouble));
        maybePut(doc, "arr_mixed", () -> Arrays.asList(1L, "two", 3.5, true, null));
        maybePut(doc, "arr_nested", () -> List.of(List.of(1L, 2L), List.of(3L)));
        maybePut(doc, "arr_obj", () -> List.of(Map.of("x", 1L), Map.of("y", "two")));
        maybePut(doc, "arr_promote", () -> randomBoolean() ? List.of(1L, 2L) : List.of("x", "yy"));
        maybePut(doc, "promote", EscfBatchMeteredSizesTests::randomUnionValue);
        maybePut(doc, "obj", () -> {
            final Map<String, Object> obj = new LinkedHashMap<>();
            maybePut(obj, "k", ESTestCase::randomLong);
            maybePut(obj, "s", () -> randomFrom(STRINGS));
            maybePut(obj, "inner", () -> Map.of("z", randomDouble()));
            return obj;
        });
        maybePut(doc, "dotted.name", () -> randomFrom(STRINGS));
        return doc;
    }

    /** A value of a different type on every call, so a column that holds it across documents becomes a union. */
    private static Object randomUnionValue() {
        return switch (randomIntBetween(0, 7)) {
            case 0 -> randomFrom(STRINGS);
            case 1 -> randomLong();
            case 2 -> randomDouble();
            case 3 -> randomBoolean();
            case 4 -> null;
            case 5 -> randomList(0, 4, ESTestCase::randomLong);
            case 6 -> Arrays.asList(1L, "two", 3.5, true, null);
            default -> Map.of("x", randomLong());
        };
    }

    private static void maybePut(Map<String, Object> doc, String field, Supplier<Object> value) {
        if (randomBoolean()) {
            doc.put(field, value.get());
        }
    }

    private static BytesReference toJson(Map<String, Object> doc) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            builder.map(doc);
            return BytesReference.bytes(builder);
        }
    }
}
