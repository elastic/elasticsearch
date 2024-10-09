/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.RandomQueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.rankdoc.RankDocsQueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;
import static org.elasticsearch.search.vectors.KnnSearchBuilderTests.randomVector;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RankDocsRetrieverBuilderTests extends ESTestCase {

    private Supplier<RankDoc[]> rankDocsSupplier() {
        final int rankDocsCount = randomIntBetween(0, 10);
        final int shardIndex = 0;
        RankDoc[] rankDocs = new RankDoc[rankDocsCount];
        int docId = 0;
        for (int i = 0; i < rankDocsCount; i++) {
            RankDoc testRankDoc = new RankDoc(docId, randomFloat(), shardIndex);
            docId += randomInt(100);
            rankDocs[i] = testRankDoc;
        }
        return () -> rankDocs;
    }

    private List<RetrieverBuilder> innerRetrievers() {
        List<RetrieverBuilder> retrievers = new ArrayList<>();
        int numRetrievers = randomIntBetween(1, 10);
        for (int i = 0; i < numRetrievers; i++) {
            if (randomBoolean()) {
                StandardRetrieverBuilder standardRetrieverBuilder = new StandardRetrieverBuilder();
                standardRetrieverBuilder.queryBuilder = RandomQueryBuilder.createQuery(random());
                if (randomBoolean()) {
                    standardRetrieverBuilder.preFilterQueryBuilders = preFilters();
                }
                retrievers.add(standardRetrieverBuilder);
            } else {
                KnnRetrieverBuilder knnRetrieverBuilder = new KnnRetrieverBuilder(
                    randomAlphaOfLength(10),
                    randomVector(randomInt(10)),
                    null,
                    randomInt(10),
                    randomIntBetween(10, 100),
                    randomFloat()
                );
                if (randomBoolean()) {
                    knnRetrieverBuilder.preFilterQueryBuilders = preFilters();
                }
                knnRetrieverBuilder.rankDocs = rankDocsSupplier().get();
                retrievers.add(knnRetrieverBuilder);
            }
        }
        return retrievers;
    }

    private List<QueryBuilder> preFilters() {
        List<QueryBuilder> preFilters = new ArrayList<>();
        int numPreFilters = randomInt(10);
        for (int i = 0; i < numPreFilters; i++) {
            preFilters.add(RandomQueryBuilder.createQuery(random()));
        }
        return preFilters;
    }

    private RankDocsRetrieverBuilder createRandomRankDocsRetrieverBuilder() {
        return new RankDocsRetrieverBuilder(randomIntBetween(1, 100), innerRetrievers(), rankDocsSupplier(), preFilters());
    }

    public void testExtractToSearchSourceBuilder() {
        RankDocsRetrieverBuilder retriever = createRandomRankDocsRetrieverBuilder();
        SearchSourceBuilder source = new SearchSourceBuilder();
        if (randomBoolean()) {
            source.aggregation(new TermsAggregationBuilder("name").field("field"));
        }
        source.explain(randomBoolean());
        source.profile(randomBoolean());
        source.trackTotalHits(randomBoolean());
        final int preFilters = retriever.preFilterQueryBuilders.size();
        retriever.extractToSearchSourceBuilder(source, randomBoolean());
        assertNull(source.sorts());
        assertThat(source.query(), anyOf(instanceOf(BoolQueryBuilder.class), instanceOf(RankDocsQueryBuilder.class)));
        if (source.query() instanceof BoolQueryBuilder bq) {
            assertThat(bq.must().size(), equalTo(1));
            assertThat(bq.must().get(0), instanceOf(RankDocsQueryBuilder.class));
            assertThat(bq.filter().size(), equalTo(preFilters));
            for (int i = 0; i < preFilters; i++) {
                assertThat(bq.filter().get(i), instanceOf(retriever.preFilterQueryBuilders.get(i).getClass()));
            }
        }
        assertNull(source.postFilter());
    }

    public void testTopDocsQuery() {
        RankDocsRetrieverBuilder retriever = createRandomRankDocsRetrieverBuilder();
        QueryBuilder topDocs = retriever.topDocsQuery();
        assertNotNull(topDocs);
        assertThat(topDocs, instanceOf(BoolQueryBuilder.class));
        assertThat(((BoolQueryBuilder) topDocs).should(), hasSize(retriever.sources.size()));
    }

    public void testRewrite() throws IOException {
        RankDocsRetrieverBuilder retriever = createRandomRankDocsRetrieverBuilder();
        boolean compoundAdded = false;
        if (randomBoolean()) {
            compoundAdded = true;
            retriever.sources.add(new TestRetrieverBuilder("compound_retriever") {
                @Override
                public boolean isCompound() {
                    return true;
                }
            });
        }
        SearchSourceBuilder source = new SearchSourceBuilder().retriever(retriever);
        QueryRewriteContext queryRewriteContext = mock(QueryRewriteContext.class);
        int size = source.size() < 0 ? DEFAULT_SIZE : source.size();
        if (retriever.rankWindowSize < size) {
            if (compoundAdded) {
                expectThrows(AssertionError.class, () -> Rewriteable.rewrite(source, queryRewriteContext));
            }
        } else {
            if (compoundAdded) {
                expectThrows(AssertionError.class, () -> Rewriteable.rewrite(source, queryRewriteContext));
            } else {
                SearchSourceBuilder rewrittenSource = Rewriteable.rewrite(source, queryRewriteContext);
                assertNull(rewrittenSource.retriever());
                assertTrue(rewrittenSource.knnSearch().isEmpty());
                assertThat(rewrittenSource.query(), instanceOf(RankDocsQueryBuilder.class));
                if (rewrittenSource.query() instanceof BoolQueryBuilder) {
                    BoolQueryBuilder bq = (BoolQueryBuilder) rewrittenSource.query();
                    assertThat(bq.filter().size(), equalTo(retriever.preFilterQueryBuilders.size()));
                    assertThat(bq.must().size(), equalTo(1));
                    assertThat(bq.must().get(0), instanceOf(BoolQueryBuilder.class));
                    assertThat(bq.should().size(), equalTo(1));
                    assertThat(bq.should().get(0), instanceOf(RankDocsQueryBuilder.class));
                }
            }
        }
    }
}
