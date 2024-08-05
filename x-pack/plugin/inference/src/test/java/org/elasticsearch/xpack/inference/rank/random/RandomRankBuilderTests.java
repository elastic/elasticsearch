/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.random;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;

public class RandomRankBuilderTests extends AbstractXContentSerializingTestCase<RandomRankBuilder> {

    @Override
    protected RandomRankBuilder createTestInstance() {
        return new RandomRankBuilder(randomIntBetween(1, 1000), "my-field", randomBoolean() ? null : randomFloat());
    }

    @Override
    protected RandomRankBuilder mutateInstance(RandomRankBuilder instance) throws IOException {
        String field = instance.field();
        int rankWindowSize = instance.rankWindowSize();
        Float minScore = instance.minScore();

        int mutate = randomIntBetween(0, 2);
        switch (mutate) {
            case 0 -> field = field + randomAlphaOfLength(2);
            case 1 -> rankWindowSize = randomValueOtherThan(instance.rankWindowSize(), this::randomRankWindowSize);
            case 2 -> minScore = randomValueOtherThan(instance.minScore(), this::randomMinScore);
            default -> throw new IllegalStateException("Requested to modify more than available parameters.");
        }
        return new RandomRankBuilder(rankWindowSize, field, minScore);
    }

    @Override
    protected Writeable.Reader<RandomRankBuilder> instanceReader() {
        return RandomRankBuilder::new;
    }

    @Override
    protected RandomRankBuilder doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        assertEquals(parser.currentToken(), XContentParser.Token.START_OBJECT);
        parser.nextToken();
        assertEquals(parser.currentToken(), XContentParser.Token.FIELD_NAME);
        assertEquals(parser.currentName(), RandomRankBuilder.NAME);
        RandomRankBuilder builder = RandomRankBuilder.PARSER.parse(parser, null);
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
              "field": "my-field"
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            RandomRankBuilder parsed = RandomRankBuilder.PARSER.parse(parser, null);
            assertEquals(DEFAULT_RANK_WINDOW_SIZE, parsed.rankWindowSize());
        }
    }

}
