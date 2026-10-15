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
 * Holds {@link RowSource} to {@link BytesSource} over the same documents, so the batch path can swap
 * one representation for the other and every document still reads the same. Takes
 * {@link BytesSource} as the reference, since it backs the non-batch indexing path.
 *
 * <p>Expects the two to agree on {@link DocumentSource#xContentType()}, on
 * {@link DocumentSource#hasContent()}, and on the document that {@link DocumentSource#parser} and
 * {@link DocumentSource#originalBytes()} yield, compared as maps because the encoder reorders fields.
 * Holds each side to its own definition of {@link DocumentSource#estimatedSizeInBytes()}, where the
 * two differ by design.
 */
public class DocumentSourceCompatibilityTests extends ESTestCase {

    private enum ValueKind {
        STRING,
        LONG,
        DOUBLE,
        BOOLEAN
    }

    /**
     * Draws random shapes and batches whose rows leave scalar columns absent, and expects every row to
     * read like the {@link BytesSource} of the bytes it was encoded from. Keeps every object column the
     * batch's schema carries in every row: a row that omits a whole object column streams back from
     * {@link SourceRowXContentParser} as an empty object that the same bytes lack.
     */
    public void testRandomDocumentsReadTheSameFromBothRepresentations() throws IOException {
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
                    final String json = sources.get(d).utf8ToString();
                    final BytesSource bytes = new BytesSource(sources.get(d), XContentType.JSON, true);
                    final RowSource row = new RowSource(schemaTree, batch.row(d), XContentType.JSON);

                    assertThat(json, row.xContentType(), equalTo(bytes.xContentType()));
                    assertThat(json, row.hasContent(), equalTo(bytes.hasContent()));
                    assertThat("streamed " + json, parseToMap(row), equalTo(parseToMap(bytes)));
                    assertThat("serialized " + json, toMap(row.originalBytes()), equalTo(toMap(bytes.originalBytes())));

                    assertThat(json, bytes.estimatedSizeInBytes(), equalTo(sources.get(d).length()));
                    assertThat(json, row.estimatedSizeInBytes(), equalTo(batch.row(d).sizeInBytes()));
                }
            }
        }
    }

    /**
     * Expects a document with no fields to carry content in both representations, so both reach the
     * parser, which indexes it as a document without fields.
     */
    public void testDocumentWithNoFieldsHasContentInBothRepresentations() throws IOException {
        final BytesReference noFields = new BytesArray("{}");
        final List<BytesReference> sources = List.of(new BytesArray("""
            {"host": "server-1"}"""), noFields);
        try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
            final SourceRowXContentParser.SchemaNode schemaTree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            final BytesSource bytes = new BytesSource(noFields, XContentType.JSON, true);
            final RowSource row = new RowSource(schemaTree, batch.row(1), XContentType.JSON);

            assertTrue(bytes.hasContent());
            assertTrue(row.hasContent());
            assertThat(parseToMap(row), equalTo(parseToMap(bytes)));
            assertThat(toMap(row.originalBytes()), equalTo(toMap(bytes.originalBytes())));
        }
    }

    /**
     * Pins the reason {@link RowSource#hasContent()} answers {@code true} unconditionally: the encoder
     * builds rows only from a top-level object, so zero bytes, which {@link BytesSource} reports as
     * carrying no content, fail to encode into any row. Encoding {@code {}} serves as the control.
     */
    public void testZeroBytesHaveNoRowRepresentation() throws IOException {
        assertFalse(new BytesSource(BytesArray.EMPTY, XContentType.JSON, true).hasContent());
        expectThrows(Exception.class, () -> EscfEncoder.encode(List.of(BytesArray.EMPTY), XContentType.JSON).close());
        try (EscfBatch batch = EscfEncoder.encode(List.of(new BytesArray("{}")), XContentType.JSON)) {
            assertThat(batch.docCount(), equalTo(1));
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
     * Draws one document from {@code shape}, dropping scalar columns at random so the encoded batch
     * exercises absent values, and retrying until the document carries at least one of them.
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

    /**
     * Draws a value of {@code kind}. Draws doubles as multiples of 1/8, which binary represents
     * exactly, so they compare equal after any float or double widening the encoder applies.
     */
    private static Object randomValue(ValueKind kind) {
        return switch (kind) {
            case STRING -> randomAlphaOfLengthBetween(1, 12);
            case LONG -> randomLong();
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

    private static Map<String, Object> parseToMap(DocumentSource source) throws IOException {
        try (XContentParser parser = source.parser(XContentParserConfiguration.EMPTY)) {
            return parser.map();
        }
    }
}
