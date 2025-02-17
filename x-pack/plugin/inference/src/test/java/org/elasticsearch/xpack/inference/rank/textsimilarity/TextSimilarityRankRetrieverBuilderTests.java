/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.DEFAULT_RERANK_ID;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TextSimilarityRankRetrieverBuilderTests extends AbstractXContentTestCase<TextSimilarityRankRetrieverBuilder> {

    /**
     * Creates a random {@link TextSimilarityRankRetrieverBuilder}. The created instance
     * is not guaranteed to pass {@link SearchRequest} validation. This is purely
     * for x-content testing.
     */
    public static TextSimilarityRankRetrieverBuilder createRandomTextSimilarityRankRetrieverBuilder() {
        return createRandomTextSimilarityRankRetrieverBuilder(TestRetrieverBuilder.createRandomTestRetrieverBuilder());
    }

    /**
     * Creates a random {@link TextSimilarityRankRetrieverBuilder}. The created instance
     * is not guaranteed to pass {@link SearchRequest} validation. This is purely
     * for x-content testing.
     */
    public static TextSimilarityRankRetrieverBuilder createRandomTextSimilarityRankRetrieverBuilder(RetrieverBuilder innerRetriever) {
        return new TextSimilarityRankRetrieverBuilder(
            innerRetriever,
            randomAlphaOfLength(10),
            randomAlphaOfLength(20),
            randomAlphaOfLength(50),
            randomIntBetween(100, 10000),
            randomBoolean()
        );
    }

    @Override
    protected TextSimilarityRankRetrieverBuilder createTestInstance() {
        return createRandomTextSimilarityRankRetrieverBuilder();
    }

    @Override
    protected TextSimilarityRankRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return (TextSimilarityRankRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
            parser,
            new RetrieverParserContext(
                new SearchUsage(),
                nf -> nf == RetrieverBuilder.RETRIEVERS_SUPPORTED
                    || nf == TextSimilarityRankRetrieverBuilder.TEXT_SIMILARITY_RERANKER_RETRIEVER_SUPPORTED
                    || nf == TextSimilarityRankRetrieverBuilder.TEXT_SIMILARITY_RERANKER_COMPOSITION_SUPPORTED
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
              "field": "my-field",
              "inference_text": "my-inference-text"
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            TextSimilarityRankRetrieverBuilder parsed = TextSimilarityRankRetrieverBuilder.PARSER.parse(
                parser,
                new RetrieverParserContext(new SearchUsage(), nf -> true)
            );
            assertThat(parsed.rankWindowSize(), equalTo(DEFAULT_RANK_WINDOW_SIZE));
            assertThat(parsed.inferenceId(), equalTo(DEFAULT_RERANK_ID));
            assertThat(parsed.failuresAllowed(), equalTo(false));

        }
    }

    public void testTextSimilarityRetrieverParsing() throws IOException {
        String restContent = """
            {
              "retriever": {
                "text_similarity_reranker": {
                  "retriever": {
                    "test": {
                      "value": "my-test-retriever"
                    }
                  },
                  "field": "my-field",
                  "inference_id": "my-inference-id",
                  "inference_text": "my-inference-text",
                  "rank_window_size": 100,
                  "min_score": 20.0,
                  "allow_rerank_failures": true,
                  "_name": "foo_reranker"
                }
              }
            }""";
        SearchUsageHolder searchUsageHolder = new UsageService().getSearchUsageHolder();
        try (XContentParser jsonParser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder source = new SearchSourceBuilder().parseXContent(jsonParser, true, searchUsageHolder, nf -> true);
            assertThat(source.retriever(), instanceOf(TextSimilarityRankRetrieverBuilder.class));
            TextSimilarityRankRetrieverBuilder parsed = (TextSimilarityRankRetrieverBuilder) source.retriever();
            assertThat(parsed.minScore(), equalTo(20f));
            assertThat(parsed.retrieverName(), equalTo("foo_reranker"));
            assertThat(parsed.failuresAllowed(), equalTo(true));
            try (XContentParser parseSerialized = createParser(JsonXContent.jsonXContent, Strings.toString(source))) {
                SearchSourceBuilder deserializedSource = new SearchSourceBuilder().parseXContent(
                    parseSerialized,
                    true,
                    searchUsageHolder,
                    nf -> true
                );
                assertThat(deserializedSource.retriever(), instanceOf(TextSimilarityRankRetrieverBuilder.class));
                TextSimilarityRankRetrieverBuilder deserialized = (TextSimilarityRankRetrieverBuilder) source.retriever();
                assertThat(parsed, equalTo(deserialized));
            }
        }
    }

    public void testTopDocsQuery() {
        RetrieverBuilder innerRetriever = new TestRetrieverBuilder(ESTestCase.randomAlphaOfLengthBetween(5, 10)) {
            @Override
            public QueryBuilder topDocsQuery() {
                return new TermQueryBuilder("field", "value");
            }
        };
        TextSimilarityRankRetrieverBuilder retriever = createRandomTextSimilarityRankRetrieverBuilder(innerRetriever);
        expectThrows(IllegalStateException.class, "Should not be called, missing a rewrite?", retriever::topDocsQuery);
    }
}
