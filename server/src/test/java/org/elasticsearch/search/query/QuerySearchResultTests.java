/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndicesTests;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregationsTests;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.TestRankBuilder;
import org.elasticsearch.search.rank.TestRankDoc;
import org.elasticsearch.search.rank.TestRankShardResult;
import org.elasticsearch.search.suggest.SuggestTests;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

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
            TestRankDoc[] docs = new TestRankDoc[randomIntBetween(5, 20)];
            for (int di = 0; di < docs.length; ++di) {
                docs[di] = new TestRankDoc(di, -1, queryCount);
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

    public void testSerialization() throws Exception {
        QuerySearchResult querySearchResult = createTestInstance();
        boolean delayed = randomBoolean();
        QuerySearchResult deserialized = copyWriteable(
            querySearchResult,
            namedWriteableRegistry,
            delayed ? in -> new QuerySearchResult(in, true) : QuerySearchResult::new,
                TransportVersion.current()
        );
        assertEquals(querySearchResult.getContextId().getId(), deserialized.getContextId().getId());
        assertNull(deserialized.getSearchShardTarget());
        assertEquals(querySearchResult.topDocs().maxScore, deserialized.topDocs().maxScore, 0f);
        assertEquals(querySearchResult.topDocs().topDocs.totalHits, deserialized.topDocs().topDocs.totalHits);
        assertEquals(querySearchResult.from(), deserialized.from());
        assertEquals(querySearchResult.size(), deserialized.size());
        assertEquals(querySearchResult.hasAggs(), deserialized.hasAggs());
        if (deserialized.hasAggs()) {
            assertThat(deserialized.aggregations().isSerialized(), is(delayed));
            Aggregations aggs = querySearchResult.consumeAggs();
            Aggregations deserializedAggs = deserialized.consumeAggs();
            assertEquals(aggs.asList(), deserializedAggs.asList());
            assertThat(deserialized.aggregations(), is(nullValue()));
        }
        assertEquals(querySearchResult.terminatedEarly(), deserialized.terminatedEarly());
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
}
