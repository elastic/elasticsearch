/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilderTests;
import org.elasticsearch.search.searchafter.SearchAfterBuilderTests;
import org.elasticsearch.search.sort.SortBuilderTests;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class StandardRetrieverBuilderParsingTests extends AbstractXContentTestCase<StandardRetrieverBuilder> {

    /**
     * Creates a random {@link StandardRetrieverBuilder}. The created instance
     * is not guaranteed to pass {@link SearchRequest} validation. This is purely
     * for x-content testing.
     */
    public static StandardRetrieverBuilder createRandomStandardRetrieverBuilder(
        BiFunction<XContent, BytesReference, XContentParser> createParser
    ) {
        try {
            StandardRetrieverBuilder standardRetrieverBuilder = new StandardRetrieverBuilder();

            if (randomBoolean()) {
                for (int i = 0; i < randomIntBetween(1, 3); ++i) {
                    standardRetrieverBuilder.getPreFilterQueryBuilders().add(RandomQueryBuilder.createQuery(random()));
                }
            }

            if (randomBoolean()) {
                standardRetrieverBuilder.queryBuilder = RandomQueryBuilder.createQuery(random());
            }

            if (randomBoolean()) {
                standardRetrieverBuilder.searchAfterBuilder = SearchAfterBuilderTests.randomJsonSearchFromBuilder(createParser);
            }

            if (randomBoolean()) {
                standardRetrieverBuilder.terminateAfter = randomNonNegativeInt();
            }

            if (randomBoolean()) {
                standardRetrieverBuilder.sortBuilders = SortBuilderTests.randomSortBuilderList(false);
            }

            if (randomBoolean()) {
                standardRetrieverBuilder.collapseBuilder = CollapseBuilderTests.randomCollapseBuilder(randomBoolean());
            }

            return standardRetrieverBuilder;
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    protected StandardRetrieverBuilder createTestInstance() {
        return createRandomStandardRetrieverBuilder((xContent, data) -> {
            try {
                return createParser(xContent, data);
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        });
    }

    @Override
    protected StandardRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return StandardRetrieverBuilder.fromXContent(
            parser,
            new RetrieverParserContext(
                new SearchUsage(),
                nf -> nf == RetrieverBuilder.RETRIEVERS_SUPPORTED || nf == StandardRetrieverBuilder.STANDARD_RETRIEVER_SUPPORTED
            )
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        // disable xcontent shuffling on the highlight builder
        return new String[] { "fields" };
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }

    public void testRewrite() throws IOException {
        for (int i = 0; i < 10; i++) {
            StandardRetrieverBuilder standardRetriever = createTestInstance();
            SearchSourceBuilder source = new SearchSourceBuilder().retriever(standardRetriever);
            QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
            source = Rewriteable.rewrite(source, queryRewriteContext);
            assertNull(source.retriever());
            assertTrue(source.knnSearch().isEmpty());
            if (standardRetriever.queryBuilder != null) {
                assertNotNull(source.query());
                if (standardRetriever.preFilterQueryBuilders.size() > 0) {
                    if (source.query() instanceof MatchAllQueryBuilder == false
                        && source.query() instanceof MatchNoneQueryBuilder == false) {
                        assertThat(source.query(), instanceOf(BoolQueryBuilder.class));
                        BoolQueryBuilder bq = (BoolQueryBuilder) source.query();
                        assertFalse(bq.must().isEmpty());
                        assertThat(bq.must().size(), equalTo(1));
                        assertThat(bq.must().get(0), equalTo(standardRetriever.queryBuilder));
                        for (int j = 0; j < bq.filter().size(); j++) {
                            assertEqualQueryOrMatchAllNone(bq.filter().get(j), standardRetriever.preFilterQueryBuilders.get(j));
                        }
                    }
                } else {
                    assertEqualQueryOrMatchAllNone(source.query(), standardRetriever.queryBuilder);
                }
            } else if (standardRetriever.preFilterQueryBuilders.size() > 0) {
                if (source.query() instanceof MatchAllQueryBuilder == false && source.query() instanceof MatchNoneQueryBuilder == false) {
                    assertNotNull(source.query());
                    assertThat(source.query(), instanceOf(BoolQueryBuilder.class));
                    BoolQueryBuilder bq = (BoolQueryBuilder) source.query();
                    assertTrue(bq.must().isEmpty());
                    for (int j = 0; j < bq.filter().size(); j++) {
                        assertEqualQueryOrMatchAllNone(bq.filter().get(j), standardRetriever.preFilterQueryBuilders.get(j));
                    }
                }
            } else {
                assertNull(source.query());
            }
            if (standardRetriever.sortBuilders != null) {
                assertThat(source.sorts().size(), equalTo(standardRetriever.sortBuilders.size()));
            }
        }
    }

    private static void assertEqualQueryOrMatchAllNone(QueryBuilder actual, QueryBuilder expected) {
        assertThat(actual, anyOf(instanceOf(MatchAllQueryBuilder.class), instanceOf(MatchNoneQueryBuilder.class), equalTo(expected)));
    }
}
