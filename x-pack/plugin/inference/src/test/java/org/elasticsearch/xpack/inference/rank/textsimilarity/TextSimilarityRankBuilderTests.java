/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;

public class TextSimilarityRankBuilderTests extends AbstractXContentSerializingTestCase<TextSimilarityRankBuilder> {

    @Override
    protected TextSimilarityRankBuilder createTestInstance() {
        return new TextSimilarityRankBuilder(
            "my-field",
            "my-inference-id",
            "my-inference-text",
            randomIntBetween(1, 1000),
            randomBoolean() ? null : randomFloat()
        );
    }

    @Override
    protected TextSimilarityRankBuilder mutateInstance(TextSimilarityRankBuilder instance) throws IOException {
        String field = instance.field();
        String inferenceId = instance.inferenceId();
        String inferenceText = instance.inferenceText();
        int rankWindowSize = instance.rankWindowSize();
        Float minScore = instance.minScore();

        int mutate = randomIntBetween(0, 4);
        switch (mutate) {
            case 0 -> field = field + randomAlphaOfLength(2);
            case 1 -> inferenceId = inferenceId + randomAlphaOfLength(2);
            case 2 -> inferenceText = inferenceText + randomAlphaOfLength(2);
            case 3 -> rankWindowSize = randomValueOtherThan(instance.rankWindowSize(), this::randomRankWindowSize);
            case 4 -> minScore = randomValueOtherThan(instance.minScore(), this::randomMinScore);
            default -> throw new IllegalStateException("Requested to modify more than available parameters.");
        }
        return new TextSimilarityRankBuilder(field, inferenceId, inferenceText, rankWindowSize, minScore);
    }

    @Override
    protected Writeable.Reader<TextSimilarityRankBuilder> instanceReader() {
        return TextSimilarityRankBuilder::new;
    }

    @Override
    protected TextSimilarityRankBuilder doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        assertEquals(parser.currentToken(), XContentParser.Token.START_OBJECT);
        parser.nextToken();
        assertEquals(parser.currentToken(), XContentParser.Token.FIELD_NAME);
        assertEquals(parser.currentName(), TextSimilarityRankBuilder.NAME);
        TextSimilarityRankBuilder builder = TextSimilarityRankBuilder.PARSER.parse(parser, null);
        parser.nextToken();
        assertEquals(parser.currentToken(), XContentParser.Token.END_OBJECT);
        parser.nextToken();
        assertNull(parser.currentToken());
        return builder;
    }

    private int randomRankWindowSize() {
        return randomIntBetween(0, 1000);
    }

    private float randomMinScore() {
        return randomFloatBetween(-1.0f, 1.0f, true);
    }

    public void testParserDefaults() throws IOException {
        String json = """
            {
              "field": "my-field",
              "inference_id": "my-inference-id",
              "inference_text": "my-inference-text"
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            TextSimilarityRankBuilder parsed = TextSimilarityRankBuilder.PARSER.parse(parser, null);
            assertEquals(DEFAULT_RANK_WINDOW_SIZE, parsed.rankWindowSize());
        }
    }

}
