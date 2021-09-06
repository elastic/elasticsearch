/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class EnrichCacheTests extends ESTestCase {

    public void testCaching() {
        // Emulate cluster metadata:
        // (two enrich indices with corresponding alias entries)
        var metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy1") + "-1")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy1")).build())
            )
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy2") + "-1")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy2")).build())
            )
            .build();

        // Emulated search requests that an enrich processor could generate:
        // (two unique searches for two enrich policies)
        var searchRequest1 = new SearchRequest(EnrichPolicy.getBaseName("policy1")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "1"))
        );
        var searchRequest2 = new SearchRequest(EnrichPolicy.getBaseName("policy1")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "2"))
        );
        var searchRequest3 = new SearchRequest(EnrichPolicy.getBaseName("policy2")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "1"))
        );
        var searchRequest4 = new SearchRequest(EnrichPolicy.getBaseName("policy2")).source(
            new SearchSourceBuilder().query(new MatchQueryBuilder("match_field", "2"))
        );
        // Emulated search response (content doesn't matter, since it isn't used, it just a cache entry)
        var searchResponse = new SearchResponse(
            new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                InternalAggregations.EMPTY,
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()),
                false,
                false,
                1
            ),
            "",
            1,
            1,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        var enrichCache = new EnrichCache(3);
        enrichCache.setMetadata(metadata);
        enrichCache.put(searchRequest1, searchResponse);
        enrichCache.put(searchRequest2, searchResponse);
        enrichCache.put(searchRequest3, searchResponse);
        var cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(0L));
        assertThat(cacheStats.getMisses(), equalTo(0L));
        assertThat(cacheStats.getEvictions(), equalTo(0L));

        assertThat(enrichCache.get(searchRequest1), notNullValue());
        assertThat(enrichCache.get(searchRequest2), notNullValue());
        assertThat(enrichCache.get(searchRequest3), notNullValue());
        assertThat(enrichCache.get(searchRequest4), nullValue());
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(3L));
        assertThat(cacheStats.getMisses(), equalTo(1L));
        assertThat(cacheStats.getEvictions(), equalTo(0L));

        enrichCache.put(searchRequest4, searchResponse);
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(3L));
        assertThat(cacheStats.getMisses(), equalTo(1L));
        assertThat(cacheStats.getEvictions(), equalTo(1L));

        // Simulate enrich policy execution, which should make current cache entries unused.
        metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy1") + "-2")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy1")).build())
            )
            .put(
                IndexMetadata.builder(EnrichPolicy.getBaseName("policy2") + "-2")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName("policy2")).build())
            )
            .build();
        enrichCache.setMetadata(metadata);

        // Because enrich index has changed, cache can't serve cached entries
        assertThat(enrichCache.get(searchRequest1), nullValue());
        assertThat(enrichCache.get(searchRequest2), nullValue());
        assertThat(enrichCache.get(searchRequest3), nullValue());
        assertThat(enrichCache.get(searchRequest4), nullValue());

        // Add new entries using new enrich index name as key
        enrichCache.put(searchRequest1, searchResponse);
        enrichCache.put(searchRequest2, searchResponse);
        enrichCache.put(searchRequest3, searchResponse);

        // Entries can now be served:
        assertThat(enrichCache.get(searchRequest1), notNullValue());
        assertThat(enrichCache.get(searchRequest2), notNullValue());
        assertThat(enrichCache.get(searchRequest3), notNullValue());
        assertThat(enrichCache.get(searchRequest4), nullValue());
        cacheStats = enrichCache.getStats("_id");
        assertThat(cacheStats.getCount(), equalTo(3L));
        assertThat(cacheStats.getHits(), equalTo(6L));
        assertThat(cacheStats.getMisses(), equalTo(6L));
        assertThat(cacheStats.getEvictions(), equalTo(4L));
    }

}
