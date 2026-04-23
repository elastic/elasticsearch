/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndicesTests;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalAggregationsTests;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.TestRankBuilder;
import org.elasticsearch.search.rank.TestRankShardResult;
import org.elasticsearch.search.suggest.SuggestTests;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class QuerySearchResultTests extends ESTestCase {

    private final NamedWriteableRegistry namedWriteableRegistry;

    public QuerySearchResultTests() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(searchModule.getNamedWriteables());
        namedWriteables.add(new NamedWriteableRegistry.Entry(RankShardResult.class, TestRankBuilder.NAME, TestRankShardResult::new));
        this.namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    private static QuerySearchResult createTestInstance() throws Exception {
        ShardId shardId = new ShardId("index", "uuid", randomInt());
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(
            OriginalIndicesTests.randomOriginalIndices(),
            searchRequest,
            shardId,
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            randomNonNegativeLong(),
            null
        );
        QuerySearchResult result = new QuerySearchResult(
            new ShardSearchContextId(UUIDs.base64UUID(), randomLong()),
            new SearchShardTarget("node", shardId, null),
            shardSearchRequest
        );
        if (randomBoolean()) {
            result.terminatedEarly(randomBoolean());
        }
        TopDocs topDocs = new TopDocs(new TotalHits(randomLongBetween(0, Long.MAX_VALUE), TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
        result.topDocs(new TopDocsAndMaxScore(topDocs, randomBoolean() ? Float.NaN : randomFloat()), new DocValueFormat[0]);
        result.size(randomInt());
        result.from(randomInt());
        if (randomBoolean()) {
            int queryCount = randomIntBetween(2, 4);
            RankDoc[] docs = new RankDoc[randomIntBetween(5, 20)];
            for (int di = 0; di < docs.length; ++di) {
                docs[di] = new RankDoc(di, -1, queryCount);
            }
            result.setRankShardResult(new TestRankShardResult(docs));
        }
        if (randomBoolean()) {
            result.suggest(SuggestTests.createTestItem());
        }
        if (randomBoolean()) {
            result.aggregations(InternalAggregationsTests.createTestInstance());
        }
        return result;
    }

    /**
     * Test-only: {@link QuerySearchResult#decRef()} does not release refcounted completion option hits; release them
     * explicitly when discarding instances built with {@link SuggestTests#createTestItem()} (or wire copies thereof).
     */
    private static void releaseCompletionSuggestOptionHits(QuerySearchResult result) {
        SuggestTests.decRefCompletionOptionTestFactoryRefs(result.suggest());
    }

    public void testSerialization() throws Exception {
        QuerySearchResult querySearchResult = createTestInstance();
        try {
            boolean delayed = randomBoolean();
            QuerySearchResult deserialized = copyWriteable(
                querySearchResult,
                namedWriteableRegistry,
                delayed ? in -> new QuerySearchResult(in, true) : QuerySearchResult::new,
                TransportVersion.current()
            );
            try {
                assertEquals(querySearchResult.getContextId().getId(), deserialized.getContextId().getId());
                assertNull(deserialized.getSearchShardTarget());
                assertEquals(querySearchResult.topDocs().maxScore, deserialized.topDocs().maxScore, 0f);
                assertEquals(querySearchResult.topDocs().topDocs.totalHits, deserialized.topDocs().topDocs.totalHits);
                assertEquals(querySearchResult.from(), deserialized.from());
                assertEquals(querySearchResult.size(), deserialized.size());
                assertEquals(querySearchResult.hasAggs(), deserialized.hasAggs());
                if (deserialized.hasAggs()) {
                    assertThat(deserialized.aggregations().isSerialized(), is(delayed));
                    InternalAggregations aggs = querySearchResult.getAggs().expand();
                    querySearchResult.releaseAggs();
                    InternalAggregations deserializedAggs = deserialized.getAggs().expand();
                    deserialized.releaseAggs();
                    assertEquals(aggs.asList(), deserializedAggs.asList());
                    assertThat(deserialized.aggregations(), is(nullValue()));
                }
                assertEquals(querySearchResult.terminatedEarly(), deserialized.terminatedEarly());
            } finally {
                releaseCompletionSuggestOptionHits(deserialized);
                deserialized.decRef();
            }
        } finally {
            releaseCompletionSuggestOptionHits(querySearchResult);
            querySearchResult.decRef();
        }
    }

    public void testNullResponse() throws Exception {
        QuerySearchResult querySearchResult = QuerySearchResult.nullInstance();
        QuerySearchResult deserialized = copyWriteable(
            querySearchResult,
            namedWriteableRegistry,
            QuerySearchResult::new,
            TransportVersion.current()
        );
        assertEquals(querySearchResult.isNull(), deserialized.isNull());
    }

    /**
     * Wire-read (deserialized) QuerySearchResult is ref-counted with initial ref 1, so decRef() releases it and returns true once.
     */
    public void testWireReadResultIsRefCounted() throws Exception {
        QuerySearchResult original = createTestInstance();
        try {
            QuerySearchResult deserialized = copyWriteable(
                original,
                namedWriteableRegistry,
                QuerySearchResult::new,
                TransportVersion.current()
            );
            assertTrue("wire-read result should have references", deserialized.hasReferences());
            releaseCompletionSuggestOptionHits(deserialized);
            assertTrue("single decRef should release", deserialized.decRef());
            assertFalse("after release should have no references", deserialized.hasReferences());
        } finally {
            releaseCompletionSuggestOptionHits(original);
            original.decRef();
        }
    }

    /**
     * Regression: {@link QuerySearchResult#registerTopHitsForRelease} must be safe when called concurrently on the same
     * result. The queue is eagerly allocated as a {@link java.util.concurrent.ConcurrentLinkedQueue} so lazy init
     * cannot race; a buggy implementation could drop registrations under parallel {@code add}.
     */
    public void testRegisterTopHitsForReleaseConcurrently() throws Exception {
        int n = randomIntBetween(32, 128);
        QuerySearchResult result = createTestInstance();
        try {
            List<SearchHits> searchHitsList = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                SearchHit hit = new SearchHit(i, "hit-" + i);
                hit.score(1.0f);
                searchHitsList.add(new SearchHits(new SearchHit[] { hit }, new TotalHits(1, Relation.EQUAL_TO), 1.0f));
            }

            CyclicBarrier barrier = new CyclicBarrier(n + 1);
            ExecutorService executor = Executors.newFixedThreadPool(n);
            try {
                for (int i = 0; i < n; i++) {
                    final int idx = i;
                    executor.submit(() -> {
                        try {
                            barrier.await(1, TimeUnit.MINUTES);
                            result.registerTopHitsForRelease(searchHitsList.get(idx));
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    });
                }
                barrier.await(1, TimeUnit.MINUTES);
            } finally {
                terminate(executor);
            }

            assertEquals(n, result.topHitsToReleaseCollector().size());
            releaseCompletionSuggestOptionHits(result);
            assertTrue(result.decRef());
            for (SearchHits sh : searchHitsList) {
                assertTrue(sh.hasReferences());
                assertTrue(sh.decRef());
                assertFalse(sh.hasReferences());
            }
        } finally {
            if (result.hasReferences()) {
                releaseCompletionSuggestOptionHits(result);
                result.decRef();
            }
        }
    }
}
