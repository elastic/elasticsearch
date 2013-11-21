/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.engine.robin;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class RobinEngineIntegrationTest extends ElasticsearchIntegrationTest {

    @Test
    public void testSetIndexCompoundOnFlush() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.builder().put("number_of_replicas", 0).put("number_of_shards", 1)).get();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        refresh();
        assertTotalCompoundSegments(1, 1, "test");
        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(ImmutableSettings.builder().put(RobinEngine.INDEX_COMPOUND_ON_FLUSH, false)).get();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        refresh();
        assertTotalCompoundSegments(1, 2, "test");
        
        client().admin().indices().prepareUpdateSettings("test")
        .setSettings(ImmutableSettings.builder().put(RobinEngine.INDEX_COMPOUND_ON_FLUSH, true)).get();
        client().prepareIndex("test", "foo").setSource("field", "foo").get();
        refresh();
        assertTotalCompoundSegments(2, 3, "test");

    }

    private void assertTotalCompoundSegments(int i, int t, String index) {
        IndicesSegmentResponse indicesSegmentResponse = client().admin().indices().prepareSegments(index).get();
        IndexSegments indexSegments = indicesSegmentResponse.getIndices().get(index);
        Collection<IndexShardSegments> values = indexSegments.getShards().values();
        int compounds = 0;
        int total = 0;
        for (IndexShardSegments indexShardSegments : values) {
            for (ShardSegments s : indexShardSegments) {
                for (Segment segment : s) {
                    if (segment.isSearch() && segment.getNumDocs() > 0) {
                        if (segment.isCompound()) {
                            compounds++;
                        }
                        total++;
                    }
                }
            }
        }
        assertThat(compounds, Matchers.equalTo(i));
        assertThat(total, Matchers.equalTo(t));

    }
    @Test
    public void test4093() {
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()
                .put("index.store.type", "memory")
                .put("cache.memory.large_cache_size", new ByteSizeValue(1, ByteSizeUnit.MB)) // no need to cache a lot
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "0")
                .put("gateway.type", "none")
                .put(RobinEngine.INDEX_COMPOUND_ON_FLUSH, randomBoolean())
                .put("index.warmer.enabled", false)
                .build()).get());
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().setJvm(true).get();
        NodeInfo[] nodes = nodeInfos.getNodes();
        for (NodeInfo info : nodes) {
            ByteSizeValue directMemoryMax = info.getJvm().getMem().getDirectMemoryMax();
            logger.debug("  --> JVM max direct memory for node [{}] is set to [{}]", info.getNode().getName(), directMemoryMax);
        }
        final int numDocs = between(30, 100); // 30 docs are enough to fail without the fix for #4093
        logger.debug("  --> Indexing [{}] documents", numDocs);
        for (int i = 0; i < numDocs; i++) {
            if ((i+1) % 10 == 0) {
                logger.debug("  --> Indexed [{}] documents", i+1);
            }
            client().prepareIndex("test", "type1")
                    .setSource("a", "" + i)
                    .setRefresh(true)
                    .execute()
                    .actionGet();
        }
        logger.debug("  --> Done indexing [{}] documents", numDocs);
        assertHitCount(client().prepareCount("test").setQuery(QueryBuilders.matchAllQuery()).get(), numDocs);
    }
}
