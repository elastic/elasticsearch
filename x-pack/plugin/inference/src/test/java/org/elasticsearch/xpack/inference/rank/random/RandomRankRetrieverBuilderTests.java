/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.random;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankBuilder;
import org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;

public class RandomRankRetrieverBuilderTests extends AbstractXContentTestCase<RandomRankRetrieverBuilder> {

    /**
     * Creates a random {@link RandomRankRetrieverBuilder}. The created instance
     * is not guaranteed to pass {@link SearchRequest} validation. This is purely
     * for x-content testing.
     */
    public static RandomRankRetrieverBuilder createRandomRankRetrieverBuilder() {
        return new RandomRankRetrieverBuilder(
            TestRetrieverBuilder.createRandomTestRetrieverBuilder(),
            randomAlphaOfLength(10),
            randomIntBetween(1, 10000),
            randomBoolean() ? randomIntBetween(1, 1000) : null
        );
    }

    @Override
    protected RandomRankRetrieverBuilder createTestInstance() {
        return createRandomRankRetrieverBuilder();
    }

    @Override
    protected RandomRankRetrieverBuilder doParseInstance(XContentParser parser) {
        return RandomRankRetrieverBuilder.PARSER.apply(
            parser,
            new RetrieverParserContext(
                new SearchUsage(),
                nf -> nf == RetrieverBuilder.RETRIEVERS_SUPPORTED || nf == RandomRankRetrieverBuilder.RANDOM_RERANKER_RETRIEVER_SUPPORTED
            )
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                TestRetrieverBuilder.TEST_SPEC.getName(),
                (p, c) -> TestRetrieverBuilder.TEST_SPEC.getParser().fromXContent(p, (RetrieverParserContext) c),
                TestRetrieverBuilder.TEST_SPEC.getName().getForRestApiVersion()
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                new ParseField(TextSimilarityRankBuilder.NAME),
                (p, c) -> TextSimilarityRankRetrieverBuilder.PARSER.apply(p, (RetrieverParserContext) c)
            )
        );
        return new NamedXContentRegistry(entries);
    }

    public void testParserDefaults() throws IOException {
        String json = """
            {
              "retriever": {
                "test": {
                  "value": "my-test-retriever"
                }
              },
              "field": "my-field"
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            RandomRankRetrieverBuilder parsed = RandomRankRetrieverBuilder.PARSER.parse(parser, null);
            assertEquals(DEFAULT_RANK_WINDOW_SIZE, parsed.rankWindowSize());
        }
    }

}
