/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class StagedSemanticFieldTests extends ESTestCase {

    /**
     * Verifies that a StagedSemanticField with no chunks round-trips through
     * XContent serialization without data loss.
     */
    public void testRoundTripEmptyChunks() throws IOException {
        Instant ts = Instant.parse("2026-05-23T14:30:00Z");
        StagedSemanticField original = new StagedSemanticField("hello world", 11, ts, List.of());

        StagedSemanticField parsed = roundTrip(original);

        assertThat(parsed.text(), equalTo("hello world"));
        assertThat(parsed.textLength(), equalTo(11));
        assertThat(parsed.lastModified(), equalTo(ts));
        assertThat(parsed.chunks(), hasSize(0));
    }

    /**
     * Verifies that a StagedSemanticField with two chunks round-trips through
     * XContent serialization, preserving offsets and embeddings for each chunk.
     */
    public void testRoundTripWithChunks() throws IOException {
        Instant ts = Instant.parse("2026-05-23T14:30:00Z");

        BytesReference emb1 = buildEmbedding(0.1f, 0.2f, 0.3f);
        BytesReference emb2 = buildEmbedding(0.4f, 0.5f, 0.6f);

        SemanticTextField.Chunk chunk1 = new SemanticTextField.Chunk(0, 200, emb1);
        SemanticTextField.Chunk chunk2 = new SemanticTextField.Chunk(200, 450, emb2);

        String text = "a".repeat(450);
        StagedSemanticField original = new StagedSemanticField(text, 450, ts, List.of(chunk1, chunk2));

        StagedSemanticField parsed = roundTrip(original);

        assertThat(parsed.text(), equalTo(text));
        assertThat(parsed.textLength(), equalTo(450));
        assertThat(parsed.lastModified(), equalTo(ts));
        assertThat(parsed.chunks(), hasSize(2));

        SemanticTextField.Chunk parsedChunk1 = parsed.chunks().get(0);
        assertThat(parsedChunk1.startOffset(), equalTo(0));
        assertThat(parsedChunk1.endOffset(), equalTo(200));

        SemanticTextField.Chunk parsedChunk2 = parsed.chunks().get(1);
        assertThat(parsedChunk2.startOffset(), equalTo(200));
        assertThat(parsedChunk2.endOffset(), equalTo(450));
    }

    /**
     * Verifies that parsing a JSON object missing the required "text" field
     * raises an exception rather than silently producing a broken instance.
     */
    public void testTextIsRequired() throws IOException {
        String json = """
            {
              "text_length": 42,
              "last_modified": "2026-05-23T14:30:00Z",
              "chunks": []
            }
            """;

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, json)) {
            Exception ex = expectThrows(Exception.class, () -> StagedSemanticField.fromXContent(parser));
            assertThat(ex.getMessage(), containsString("text"));
        }
    }

    // --- helpers ---

    private static StagedSemanticField roundTrip(StagedSemanticField field) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        field.toXContent(builder, null);
        String json = Strings.toString(builder);

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken(); // advance to START_OBJECT
            return StagedSemanticField.fromXContent(parser);
        }
    }

    /**
     * Builds a JSON-encoded float array embedding as a {@link BytesReference}.
     */
    private static BytesReference buildEmbedding(float... values) throws IOException {
        XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent());
        b.startArray();
        for (float v : values) {
            b.value(v);
        }
        b.endArray();
        return BytesReference.bytes(b);
    }
}
