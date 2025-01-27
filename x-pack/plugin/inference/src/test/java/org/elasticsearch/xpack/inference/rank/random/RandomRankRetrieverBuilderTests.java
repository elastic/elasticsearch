/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.random;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.hamcrest.Matchers.containsString;

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
    protected RandomRankRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return (RandomRankRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
            parser,
            new RetrieverParserContext(new SearchUsage(), Predicates.never())
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
                new ParseField(RandomRankBuilder.NAME),
                (p, c) -> RandomRankRetrieverBuilder.PARSER.apply(p, (RetrieverParserContext) c)
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

    public void testParserEmptyRetriever() throws IOException {
        String json = """
            {
              "retriever": {
              },
              "field": "my-field"
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            XContentParseException ex = expectThrows(
                XContentParseException.class,
                () -> RandomRankRetrieverBuilder.PARSER.parse(parser, null)
            );
            assertThat(ex.getMessage(), containsString("[random_reranker] failed to parse field [retriever]"));
            assertThat(ex.getCause().getMessage(), containsString("empty [retriever] object"));
        }
    }

    public void testParserWrongRetrieverName() throws IOException {
        String json = """
            {
              "retriever": {
                "test2": {
                  "value": "my-test-retriever"
                }
              },
              "field": "my-field"
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            XContentParseException ex = expectThrows(
                XContentParseException.class,
                () -> RandomRankRetrieverBuilder.PARSER.parse(parser, null)
            );
            assertThat(ex.getMessage(), containsString("[random_reranker] failed to parse field [retriever]"));
            assertThat(ex.getCause().getMessage(), containsString("unknown field [test2]"));
        }
    }

    public void testExtraContent() throws IOException {
        String json = """
            {
              "retriever": {
                "test": {
                  "value": "my-test-retriever"
                },
                "field2": "my-field"
              },
              "field": "my-field"
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            XContentParseException ex = expectThrows(
                XContentParseException.class,
                () -> RandomRankRetrieverBuilder.PARSER.parse(parser, null)
            );
            assertThat(ex.getMessage(), containsString("[random_reranker] failed to parse field [retriever]"));
            assertThat(ex.getCause().getMessage(), containsString("unexpected field [field2]"));
        }
    }

}
