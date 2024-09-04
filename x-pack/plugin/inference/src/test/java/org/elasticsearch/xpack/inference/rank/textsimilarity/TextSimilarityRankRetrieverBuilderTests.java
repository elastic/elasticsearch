/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

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
            randomIntBetween(1, 10000),
            randomBoolean() ? null : randomFloatBetween(-1.0f, 1.0f, true)
        );
    }

    @Override
    protected TextSimilarityRankRetrieverBuilder createTestInstance() {
        return createRandomTextSimilarityRankRetrieverBuilder();
    }

    @Override
    protected TextSimilarityRankRetrieverBuilder doParseInstance(XContentParser parser) {
        return TextSimilarityRankRetrieverBuilder.PARSER.apply(
            parser,
            new RetrieverParserContext(
                new SearchUsage(),
                nf -> nf == RetrieverBuilder.RETRIEVERS_SUPPORTED
                    || nf == TextSimilarityRankRetrieverBuilder.TEXT_SIMILARITY_RERANKER_RETRIEVER_SUPPORTED
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
              "inference_id": "my-inference-id",
              "inference_text": "my-inference-text"
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            TextSimilarityRankRetrieverBuilder parsed = TextSimilarityRankRetrieverBuilder.PARSER.parse(parser, null);
            assertEquals(DEFAULT_RANK_WINDOW_SIZE, parsed.rankWindowSize());
        }
    }

    public void testRewriteInnerRetriever() throws IOException {
        final boolean[] rewritten = { false };
        List<QueryBuilder> preFilterQueryBuilders = new ArrayList<>();
        if (randomBoolean()) {
            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                preFilterQueryBuilders.add(RandomQueryBuilder.createQuery(random()));
            }
        }
        RetrieverBuilder innerRetriever = new TestRetrieverBuilder("top-level-retriever") {
            @Override
            public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
                if (randomBoolean()) {
                    return this;
                }
                rewritten[0] = true;
                return new TestRetrieverBuilder("nested-rewritten-retriever") {
                    @Override
                    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
                        if (preFilterQueryBuilders.isEmpty() == false) {
                            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

                            for (QueryBuilder preFilterQueryBuilder : preFilterQueryBuilders) {
                                boolQueryBuilder.filter(preFilterQueryBuilder);
                            }
                            boolQueryBuilder.must(new RangeQueryBuilder("some_field"));
                            searchSourceBuilder.subSearches().add(new SubSearchSourceBuilder(boolQueryBuilder));
                        } else {
                            searchSourceBuilder.subSearches().add(new SubSearchSourceBuilder(new RangeQueryBuilder("some_field")));
                        }
                    }
                };
            }

            @Override
            public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
                if (preFilterQueryBuilders.isEmpty() == false) {
                    BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

                    for (QueryBuilder preFilterQueryBuilder : preFilterQueryBuilders) {
                        boolQueryBuilder.filter(preFilterQueryBuilder);
                    }
                    boolQueryBuilder.must(new TermQueryBuilder("field", "value"));
                    searchSourceBuilder.subSearches().add(new SubSearchSourceBuilder(boolQueryBuilder));
                } else {
                    searchSourceBuilder.subSearches().add(new SubSearchSourceBuilder(new TermQueryBuilder("field", "value")));
                }
            }
        };
        TextSimilarityRankRetrieverBuilder textSimilarityRankRetrieverBuilder = createRandomTextSimilarityRankRetrieverBuilder(
            innerRetriever
        );
        textSimilarityRankRetrieverBuilder.getPreFilterQueryBuilders().addAll(preFilterQueryBuilders);
        SearchSourceBuilder source = new SearchSourceBuilder().retriever(textSimilarityRankRetrieverBuilder);
        QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
        source = Rewriteable.rewrite(source, queryRewriteContext);
        assertNull(source.retriever());
        if (false == preFilterQueryBuilders.isEmpty()) {
            if (source.query() instanceof MatchAllQueryBuilder == false && source.query() instanceof MatchNoneQueryBuilder == false) {
                assertThat(source.query(), instanceOf(BoolQueryBuilder.class));
                BoolQueryBuilder bq = (BoolQueryBuilder) source.query();
                assertFalse(bq.must().isEmpty());
                assertThat(bq.must().size(), equalTo(1));
                if (rewritten[0]) {
                    assertThat(bq.must().get(0), instanceOf(RangeQueryBuilder.class));
                } else {
                    assertThat(bq.must().get(0), instanceOf(TermQueryBuilder.class));
                }
                for (int j = 0; j < bq.filter().size(); j++) {
                    assertEqualQueryOrMatchAllNone(bq.filter().get(j), preFilterQueryBuilders.get(j));
                }
            }
        } else {
            if (rewritten[0]) {
                assertThat(source.query(), instanceOf(RangeQueryBuilder.class));
            } else {
                assertThat(source.query(), instanceOf(TermQueryBuilder.class));
            }
        }
    }

    public void testIsCompound() {
        RetrieverBuilder compoundInnerRetriever = new TestRetrieverBuilder(ESTestCase.randomAlphaOfLengthBetween(5, 10)) {
            @Override
            public boolean isCompound() {
                return true;
            }
        };
        RetrieverBuilder nonCompoundInnerRetriever = new TestRetrieverBuilder(ESTestCase.randomAlphaOfLengthBetween(5, 10)) {
            @Override
            public boolean isCompound() {
                return false;
            }
        };
        TextSimilarityRankRetrieverBuilder compoundTextSimilarityRankRetrieverBuilder = createRandomTextSimilarityRankRetrieverBuilder(
            compoundInnerRetriever
        );
        assertTrue(compoundTextSimilarityRankRetrieverBuilder.isCompound());
        TextSimilarityRankRetrieverBuilder nonCompoundTextSimilarityRankRetrieverBuilder = createRandomTextSimilarityRankRetrieverBuilder(
            nonCompoundInnerRetriever
        );
        assertFalse(nonCompoundTextSimilarityRankRetrieverBuilder.isCompound());
    }

    public void testTopDocsQuery() {
        RetrieverBuilder innerRetriever = new TestRetrieverBuilder(ESTestCase.randomAlphaOfLengthBetween(5, 10)) {
            @Override
            public QueryBuilder topDocsQuery() {
                return new TermQueryBuilder("field", "value");
            }
        };
        TextSimilarityRankRetrieverBuilder retriever = createRandomTextSimilarityRankRetrieverBuilder(innerRetriever);
        assertThat(retriever.topDocsQuery(), instanceOf(TermQueryBuilder.class));
    }

    private static void assertEqualQueryOrMatchAllNone(QueryBuilder actual, QueryBuilder expected) {
        assertThat(actual, anyOf(instanceOf(MatchAllQueryBuilder.class), instanceOf(MatchNoneQueryBuilder.class), equalTo(expected)));
    }

}
