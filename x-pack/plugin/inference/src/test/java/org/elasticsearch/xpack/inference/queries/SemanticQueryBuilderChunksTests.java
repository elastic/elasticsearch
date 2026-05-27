/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SemanticQueryBuilderChunksTests extends ESTestCase {

    public void testParseWithChunksPerDoc() throws IOException {
        String json = """
            {
                "field": "my_field",
                "query": "test query",
                "chunks_per_doc": 5
            }
            """;
        SemanticQueryBuilder builder = parseQuery(json);
        assertThat(builder.chunksPerDoc(), equalTo(5));
        assertThat(builder.minScore(), nullValue());
    }

    public void testParseWithMinScore() throws IOException {
        String json = """
            {
                "field": "my_field",
                "query": "test query",
                "min_score": 0.5
            }
            """;
        SemanticQueryBuilder builder = parseQuery(json);
        assertThat(builder.minScore(), equalTo(0.5f));
        assertThat(builder.chunksPerDoc(), nullValue());
    }

    public void testParseWithBothParams() throws IOException {
        String json = """
            {
                "field": "my_field",
                "query": "test query",
                "min_score": 0.3,
                "chunks_per_doc": 10
            }
            """;
        SemanticQueryBuilder builder = parseQuery(json);
        assertThat(builder.minScore(), equalTo(0.3f));
        assertThat(builder.chunksPerDoc(), equalTo(10));
    }

    public void testParseWithoutChunkParams() throws IOException {
        String json = """
            {
                "field": "my_field",
                "query": "test query"
            }
            """;
        SemanticQueryBuilder builder = parseQuery(json);
        assertThat(builder.minScore(), nullValue());
        assertThat(builder.chunksPerDoc(), nullValue());
    }

    public void testChunksPerDocMustBePositive() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SemanticQueryBuilder("field", "query", null, null, 0)
        );
        assertThat(e.getMessage(), containsString("chunks_per_doc must be at least 1"));
    }

    public void testMinScoreMustBeNonNegative() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SemanticQueryBuilder("field", "query", null, -0.1f, null)
        );
        assertThat(e.getMessage(), containsString("min_score must be non-negative"));
    }

    public void testHasChunkConfig() {
        SemanticQueryBuilder neither = new SemanticQueryBuilder("field", "query");
        assertFalse(neither.hasChunkConfig());

        SemanticQueryBuilder withMinScore = new SemanticQueryBuilder("field", "query", null, 0.5f, null);
        assertTrue(withMinScore.hasChunkConfig());

        SemanticQueryBuilder withChunksPerDoc = new SemanticQueryBuilder("field", "query", null, null, 3);
        assertTrue(withChunksPerDoc.hasChunkConfig());

        SemanticQueryBuilder withBoth = new SemanticQueryBuilder("field", "query", null, 0.5f, 3);
        assertTrue(withBoth.hasChunkConfig());
    }

    private SemanticQueryBuilder parseQuery(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return SemanticQueryBuilder.fromXContent(parser);
        }
    }
}
