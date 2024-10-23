/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.action.search.SearchRequest;
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
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.rankdoc.RankDocsQueryBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.vectors.KnnSearchBuilderTests.randomVector;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class KnnRetrieverBuilderParsingTests extends AbstractXContentTestCase<KnnRetrieverBuilder> {

    /**
     * Creates a random {@link KnnRetrieverBuilder}. The created instance
     * is not guaranteed to pass {@link SearchRequest} validation. This is purely
     * for x-content testing.
     */
    public static KnnRetrieverBuilder createRandomKnnRetrieverBuilder() {
        String field = randomAlphaOfLength(6);
        int dim = randomIntBetween(2, 30);
        float[] vector = randomVector(dim);
        int k = randomIntBetween(1, 100);
        int numCands = randomIntBetween(k + 20, 1000);
        Float similarity = randomBoolean() ? null : randomFloat();

        KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(field, vector, null, k, numCands, similarity);

        List<QueryBuilder> preFilterQueryBuilders = new ArrayList<>();

        if (randomBoolean()) {
            for (int i = 0; i < randomIntBetween(1, 3); ++i) {
                preFilterQueryBuilders.add(RandomQueryBuilder.createQuery(random()));
            }
        }

        knnRetrieverBuilder.preFilterQueryBuilders.addAll(preFilterQueryBuilders);

        return knnRetrieverBuilder;
    }

    @Override
    protected KnnRetrieverBuilder createTestInstance() {
        return createRandomKnnRetrieverBuilder();
    }

    @Override
    protected KnnRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return (KnnRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
            parser,
            new RetrieverParserContext(
                new SearchUsage(),
                nf -> nf == RetrieverBuilder.RETRIEVERS_SUPPORTED || nf == KnnRetrieverBuilder.KNN_RETRIEVER_SUPPORTED
            )
        );
    }

    public void testRewrite() throws IOException {
        for (int i = 0; i < 10; i++) {
            KnnRetrieverBuilder knnRetriever = createRandomKnnRetrieverBuilder();
            SearchSourceBuilder source = new SearchSourceBuilder().retriever(knnRetriever);
            QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
            source = Rewriteable.rewrite(source, queryRewriteContext);
            assertNull(source.retriever());
            assertNull(source.query());
            assertThat(source.knnSearch().size(), equalTo(1));
            assertThat(source.knnSearch().get(0).getFilterQueries().size(), equalTo(knnRetriever.preFilterQueryBuilders.size()));
            for (int j = 0; j < knnRetriever.preFilterQueryBuilders.size(); j++) {
                assertThat(
                    source.knnSearch().get(0).getFilterQueries().get(j),
                    anyOf(
                        instanceOf(MatchAllQueryBuilder.class),
                        instanceOf(MatchNoneQueryBuilder.class),
                        equalTo(knnRetriever.preFilterQueryBuilders.get(j))
                    )
                );
            }
        }
    }

    public void testIsCompound() {
        KnnRetrieverBuilder knnRetriever = createRandomKnnRetrieverBuilder();
        assertFalse(knnRetriever.isCompound());
    }

    public void testTopDocsQuery() {
        KnnRetrieverBuilder knnRetriever = createRandomKnnRetrieverBuilder();
        knnRetriever.rankDocs = new RankDoc[] {
            new RankDoc(0, randomFloat(), 0),
            new RankDoc(10, randomFloat(), 0),
            new RankDoc(20, randomFloat(), 1),
            new RankDoc(25, randomFloat(), 1), };
        final int preFilters = knnRetriever.preFilterQueryBuilders.size();
        QueryBuilder topDocsQuery = knnRetriever.topDocsQuery();
        assertNotNull(topDocsQuery);
        assertThat(topDocsQuery, anyOf(instanceOf(RankDocsQueryBuilder.class), instanceOf(BoolQueryBuilder.class)));
        if (topDocsQuery instanceof BoolQueryBuilder bq) {
            assertThat(bq.must().size(), equalTo(1));
            assertThat(bq.must().get(0), instanceOf(RankDocsQueryBuilder.class));
            assertThat(bq.filter().size(), equalTo(preFilters));
            for (int i = 0; i < preFilters; i++) {
                assertThat(bq.filter().get(i), instanceOf(knnRetriever.preFilterQueryBuilders.get(i).getClass()));
            }
        }
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }
}
