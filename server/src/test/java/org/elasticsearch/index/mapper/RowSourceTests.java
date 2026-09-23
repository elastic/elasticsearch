/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceRowXContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link RowSource} against rows produced by the production batch encoder
 * ({@link EscfEncoder}), so the row and schema under test are shaped the way the batch indexing
 * path shapes them.
 *
 * <p>{@link RowSource} offers two ways to read one document — stream it with
 * {@link RowSource#parser} or serialize it with {@link RowSource#originalBytes()} — and mappers
 * pick between them for performance. A divergence between them indexes a document differently
 * depending on which reader a mapper happened to call, so both are held to the source the row was
 * encoded from.
 */
public class RowSourceTests extends ESTestCase {

    private enum ValueKind {
        STRING,
        LONG,
        DOUBLE,
        BOOLEAN
    }

    /**
     * Holds both readers to the document that was encoded, over random shapes and over batches
     * whose rows leave scalar columns absent.
     */
    public void testReadPathsReproduceTheEncodedSource() throws IOException {
        forRandomBatches((rowSource, row, source) -> {
            final Map<String, Object> expected = toMap(source);
            assertThat(rowSource.xContentType(), equalTo(XContentType.JSON));
            assertFalse(source.utf8ToString(), rowSource.isEmpty());
            assertThat(rowSource.estimatedSizeInBytes(), equalTo(row.sizeInBytes()));
            assertThat("streamed " + source.utf8ToString(), parseToMap(rowSource), equalTo(expected));
            assertThat("serialized " + source.utf8ToString(), toMap(rowSource.originalBytes()), equalTo(expected));
            assertSame(rowSource.originalBytes(), rowSource.originalBytes());
        });
    }

    public void testRowWithNoValuesIsEmpty() throws IOException {
        final List<BytesReference> sources = List.of(new BytesArray("""
            {"host": "server-1"}"""), new BytesArray("{}"));
        try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
            final SourceRowXContentParser.SchemaNode schemaTree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            assertFalse(new RowSource(schemaTree, batch.row(0), XContentType.JSON).isEmpty());

            final RowSource empty = new RowSource(schemaTree, batch.row(1), XContentType.JSON);
            assertTrue(empty.isEmpty());
            assertThat(empty.originalBytes().utf8ToString(), equalTo("{}"));
            assertThat(parseToMap(empty), equalTo(Map.of()));
        }
    }

    /**
     * Materializing the bytes is lazy and guarded by a monitor, so concurrent first readers must
     * agree on a single serialization rather than each publishing their own.
     */
    public void testConcurrentReadersShareOneMaterialization() throws IOException {
        final BytesReference source = new BytesArray("""
            {"host": "server-1", "metrics": {"cpu": 0.25, "mem": 2048}}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            final SourceRowXContentParser.SchemaNode schemaTree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            final RowSource rowSource = new RowSource(schemaTree, batch.row(0), XContentType.JSON);

            final int readers = between(4, 16);
            final BytesReference[] seen = new BytesReference[readers];
            startInParallel(readers, i -> seen[i] = rowSource.originalBytes());

            for (BytesReference bytes : seen) {
                assertSame(seen[0], bytes);
            }
            assertThat(toMap(seen[0]), equalTo(toMap(source)));
        }
    }

    public void testRejectsNullComponents() throws IOException {
        try (EscfBatch batch = EscfEncoder.encode(List.of(new BytesArray("""
            {"host": "server-1"}""")), XContentType.JSON)) {
            final SourceRowXContentParser.SchemaNode schemaTree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            final SourceRow row = batch.row(0);

            expectThrows(NullPointerException.class, () -> new RowSource(null, row, XContentType.JSON));
            expectThrows(NullPointerException.class, () -> new RowSource(schemaTree, null, XContentType.JSON));
            expectThrows(NullPointerException.class, () -> new RowSource(schemaTree, row, null));
        }
    }

    private interface RowCheck {
        void check(RowSource rowSource, SourceRow row, BytesReference source) throws IOException;
    }

    /**
     * Encodes batches of randomly shaped documents and hands each row to {@code check}.
     *
     * <p>Rows leave scalar columns absent at random and keep every object column the batch's schema
     * carries. A row that omits a whole object column is streamed back by
     * {@link org.elasticsearch.sourcebatch.SourceRowXContentParser} as an empty object while
     * {@link RowSource#originalBytes()} omits it, so the two readers disagree on shapes this
     * generator stays away from.
     */
    private static void forRandomBatches(RowCheck check) throws IOException {
        for (int shapeIteration = 0; shapeIteration < randomIntBetween(2, 5); shapeIteration++) {
            final Map<String, Object> shape = randomShape(randomIntBetween(0, 2));
            final int docCount = randomIntBetween(1, 8);
            final List<BytesReference> sources = new ArrayList<>(docCount);
            for (int d = 0; d < docCount; d++) {
                sources.add(toJson(randomDoc(shape)));
            }

            try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
                final SourceRowXContentParser.SchemaNode schemaTree = SourceRowXContentParser.buildSchemaTree(batch.schema());
                for (int d = 0; d < docCount; d++) {
                    final SourceRow row = batch.row(d);
                    check.check(new RowSource(schemaTree, row, XContentType.JSON), row, sources.get(d));
                }
            }
        }
    }

    private static Map<String, Object> randomShape(int depth) {
        final int fieldCount = randomIntBetween(1, 5);
        final Map<String, Object> shape = new LinkedHashMap<>();
        while (shape.size() < fieldCount) {
            final String name = randomAlphaOfLengthBetween(3, 8).toLowerCase(Locale.ROOT);
            if (shape.containsKey(name)) {
                continue;
            }
            shape.put(name, depth > 0 && randomBoolean() ? randomShape(depth - 1) : randomFrom(ValueKind.values()));
        }
        return shape;
    }

    /**
     * Draws one document from {@code shape}, dropping scalar columns at random so the encoded
     * batch exercises absent values, and retrying until the document carries at least one of them.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> randomDoc(Map<String, Object> shape) {
        Map<String, Object> doc = new LinkedHashMap<>();
        while (doc.isEmpty()) {
            doc = new LinkedHashMap<>();
            for (Map.Entry<String, Object> field : shape.entrySet()) {
                if (field.getValue() instanceof Map<?, ?> nested) {
                    doc.put(field.getKey(), randomDoc((Map<String, Object>) nested));
                } else if (rarely()) {
                    continue;
                } else {
                    doc.put(field.getKey(), rarely() ? null : randomValue((ValueKind) field.getValue()));
                }
            }
        }
        return doc;
    }

    private static Object randomValue(ValueKind kind) {
        return switch (kind) {
            case STRING -> randomAlphaOfLengthBetween(1, 12);
            case LONG -> randomLong();
            // Halves are exact in binary, so the value survives any float/double widening the encoder applies.
            case DOUBLE -> randomLongBetween(-4096, 4096) / 8.0;
            case BOOLEAN -> randomBoolean();
        };
    }

    private static BytesReference toJson(Map<String, Object> doc) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(doc);
            return BytesReference.bytes(builder);
        }
    }

    private static Map<String, Object> toMap(BytesReference bytes) {
        return XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
    }

    private static Map<String, Object> parseToMap(RowSource rowSource) throws IOException {
        try (XContentParser parser = rowSource.parser(XContentParserConfiguration.EMPTY)) {
            return parser.map();
        }
    }
}
