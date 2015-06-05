/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.warmer;


import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.metadata.MetaData.CLUSTER_READ_ONLY_BLOCK;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)
public class IndicesWarmerBlocksTests extends ElasticsearchIntegrationTest {

    @Test
    public void testPutWarmerWithBlocks() {
        createIndex("test-blocks");
        ensureGreen("test-blocks");

        // Index reads are blocked, the warmer can't be registered
        try {
            enableIndexBlock("test-blocks", SETTING_BLOCKS_READ);
            assertBlocked(client().admin().indices().preparePutWarmer("warmer_blocked")
                    .setSearchRequest(client().prepareSearch("test-*").setTypes("a1").setQuery(QueryBuilders.matchAllQuery())), INDEX_READ_BLOCK);
        } finally {
            disableIndexBlock("test-blocks", SETTING_BLOCKS_READ);
        }

        // Index writes are blocked, the warmer can be registered
        try {
            enableIndexBlock("test-blocks", SETTING_BLOCKS_WRITE);
            assertAcked(client().admin().indices().preparePutWarmer("warmer_acked")
                    .setSearchRequest(client().prepareSearch("test-blocks").setTypes("a1").setQuery(QueryBuilders.matchAllQuery())));
        } finally {
            disableIndexBlock("test-blocks", SETTING_BLOCKS_WRITE);
        }

        // Index metadata changes are blocked, the warmer can't be registered
        try {
            enableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().preparePutWarmer("warmer_blocked")
                    .setSearchRequest(client().prepareSearch("test-*").setTypes("a1").setQuery(QueryBuilders.matchAllQuery())), INDEX_METADATA_BLOCK);
        } finally {
            disableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
        }

        // Index metadata changes are blocked, the warmer can't be registered
        try {
            enableIndexBlock("test-blocks", SETTING_READ_ONLY);
            assertBlocked(client().admin().indices().preparePutWarmer("warmer_blocked")
                    .setSearchRequest(client().prepareSearch("test-*").setTypes("a1").setQuery(QueryBuilders.matchAllQuery())), INDEX_READ_ONLY_BLOCK);
        } finally {
            disableIndexBlock("test-blocks", SETTING_READ_ONLY);
        }

        // Adding a new warmer is not possible when the cluster is read-only
        try {
            setClusterReadOnly(true);
            assertBlocked(client().admin().indices().preparePutWarmer("warmer_blocked")
                    .setSearchRequest(client().prepareSearch("test-blocks").setTypes("a1").setQuery(QueryBuilders.matchAllQuery())), CLUSTER_READ_ONLY_BLOCK);
        } finally {
            setClusterReadOnly(false);
        }
    }

    @Test
    public void testGetWarmerWithBlocks() {
        createIndex("test-blocks");
        ensureGreen("test-blocks");

        assertAcked(client().admin().indices().preparePutWarmer("warmer_block")
                .setSearchRequest(client().prepareSearch("test-blocks").setTypes("a1").setQuery(QueryBuilders.matchAllQuery())));

        // Request is not blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("test-blocks", blockSetting);
                GetWarmersResponse response = client().admin().indices().prepareGetWarmers("test-blocks").get();
                assertThat(response.warmers().size(), equalTo(1));

                ObjectObjectCursor<String, ImmutableList<IndexWarmersMetaData.Entry>> entry = response.warmers().iterator().next();
                assertThat(entry.key, equalTo("test-blocks"));
                assertThat(entry.value.size(), equalTo(1));
                assertThat(entry.value.iterator().next().name(), equalTo("warmer_block"));
            } finally {
                disableIndexBlock("test-blocks", blockSetting);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareGetWarmers("test-blocks"), INDEX_METADATA_BLOCK);
        } finally {
            disableIndexBlock("test-blocks", SETTING_BLOCKS_METADATA);
        }
    }

    @Test
    public void testDeleteWarmerWithBlocks() {
        createIndex("test-blocks");
        ensureGreen("test-blocks");

        // Request is not blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                assertAcked(client().admin().indices().preparePutWarmer("warmer_block")
                        .setSearchRequest(client().prepareSearch("test-blocks").setTypes("a1").setQuery(QueryBuilders.matchAllQuery())));

                enableIndexBlock("test-blocks", blockSetting);
                assertAcked(client().admin().indices().prepareDeleteWarmer().setIndices("test-blocks").setNames("warmer_block"));
            } finally {
                disableIndexBlock("test-blocks", blockSetting);
            }
        }

        // Request is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                assertAcked(client().admin().indices().preparePutWarmer("warmer_block")
                        .setSearchRequest(client().prepareSearch("test-blocks").setTypes("a1").setQuery(QueryBuilders.matchAllQuery())));

                enableIndexBlock("test-blocks", blockSetting);
                assertBlocked(client().admin().indices().prepareDeleteWarmer().setIndices("test-blocks").setNames("warmer_block"));
            } finally {
                disableIndexBlock("test-blocks", blockSetting);
            }
        }
    }
}
