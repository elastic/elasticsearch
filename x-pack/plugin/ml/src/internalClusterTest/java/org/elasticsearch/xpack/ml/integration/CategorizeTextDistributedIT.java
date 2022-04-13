/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizeTextAggregationBuilder;
import org.elasticsearch.xpack.ml.aggs.categorization.InternalCategorizationAggregation;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CategorizeTextDistributedIT extends BaseMlIntegTestCase {

    /**
     * When categorizing text in a multi-node cluster the categorize_text2 aggregation has
     * a harder job than in a single node cluster. The categories must be serialized between
     * nodes and then merged appropriately on the receiving node. This test ensures that
     * this serialization and subsequent merging works in the same way that merging would work
     * on a single node.
     */
    public void testDistributedCategorizeText() {
        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster();

        // System indices may affect the distribution of shards of this index,
        // but it has so many that it should have shards on all the nodes
        String indexName = "data";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "9").put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0")
        );
        client().admin().indices().create(createIndexRequest).actionGet();

        // Spread 10000 documents in 4 categories across the shards
        for (int i = 0; i < 10; ++i) {
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
            for (int j = 0; j < 250; ++j) {
                IndexRequestBuilder indexRequestBuilder = client().prepareIndex(indexName)
                    .setSource(Map.of("message", "Aug 29, 2019 2:02:51 PM org.apache.coyote.http11.Http11BaseProtocol destroy"));
                bulkRequestBuilder.add(indexRequestBuilder);
                indexRequestBuilder = client().prepareIndex(indexName)
                    .setSource(Map.of("message", "Aug 29, 2019 2:02:51 PM org.apache.coyote.http11.Http11BaseProtocol init"));
                bulkRequestBuilder.add(indexRequestBuilder);
                indexRequestBuilder = client().prepareIndex(indexName)
                    .setSource(Map.of("message", "Aug 29, 2019 2:02:51 PM org.apache.coyote.http11.Http11BaseProtocol start"));
                bulkRequestBuilder.add(indexRequestBuilder);
                indexRequestBuilder = client().prepareIndex(indexName)
                    .setSource(Map.of("message", "Aug 29, 2019 2:02:51 PM org.apache.coyote.http11.Http11BaseProtocol stop"));
                bulkRequestBuilder.add(indexRequestBuilder);
            }
            bulkRequestBuilder.execute().actionGet();
        }
        client().admin().indices().prepareRefresh(indexName).execute().actionGet();

        // Confirm the theory that all 3 nodes will have a shard on
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(indexName).execute().actionGet();
        Set<String> nodesWithShards = Arrays.stream(indicesStatsResponse.getShards())
            .map(ShardStats::getShardRouting)
            .map(ShardRouting::currentNodeId)
            .collect(Collectors.toSet());
        assertThat(nodesWithShards, hasSize(internalCluster().size()));

        SearchResponse searchResponse = client().prepareSearch(indexName)
            .addAggregation(new CategorizeTextAggregationBuilder("categories", "message"))
            .setSize(0)
            .execute()
            .actionGet();

        InternalCategorizationAggregation aggregation = searchResponse.getAggregations().get("categories");
        assertThat(aggregation, notNullValue());

        // We should have created 4 categories, one for each of the distinct messages we indexed, all with counts of 2500 (= 10000/4)
        List<InternalCategorizationAggregation.Bucket> buckets = aggregation.getBuckets();
        assertThat(buckets, notNullValue());
        assertThat(buckets, hasSize(4));
        Set<String> expectedLastTokens = new HashSet<>(List.of("destroy", "init", "start", "stop"));
        for (InternalCategorizationAggregation.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), is(2500L));
            String[] tokens = bucket.getKeyAsString().split(" ");
            String lastToken = tokens[tokens.length - 1];
            assertThat(lastToken + " not found in " + expectedLastTokens, expectedLastTokens.remove(lastToken), is(true));
        }
        assertThat("Some expected last tokens not found " + expectedLastTokens, expectedLastTokens, empty());
    }
}
