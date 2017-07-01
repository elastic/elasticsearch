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

package org.elasticsearch.search.child;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ParentFieldLoadingIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class); // uses index.merge.enabled
    }

    private final Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    // We never want merges in this test to ensure we have two segments for the last validation
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .put("index.version.created", Version.V_5_6_0)
            .build();

    public void testEagerParentFieldLoading() throws Exception {
        logger.info("testing lazy loading...");
        assertAcked(prepareCreate("test")
                .setSettings(indexSettings)
                .addMapping("parent")
                .addMapping("child", childMapping(false)));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}", XContentType.JSON).get();
        refresh();

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), equalTo(0L));

        logger.info("testing default loading...");
        assertAcked(client().admin().indices().prepareDelete("test").get());
        assertAcked(prepareCreate("test")
                .setSettings(indexSettings)
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}", XContentType.JSON).get();
        refresh();

        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), equalTo(0L));

        logger.info("testing eager global ordinals loading...");
        assertAcked(client().admin().indices().prepareDelete("test").get());
        assertAcked(prepareCreate("test")
                .setSettings(indexSettings)
                .addMapping("parent")
                .addMapping("child", childMapping(true)));
        ensureGreen();

        // Need to do 2 separate refreshes, otherwise we have 1 segment and then we can't measure if global ordinals
        // is loaded by the size of the field data cache, because global ordinals on 1 segment shards takes no extra memory.
        client().prepareIndex("test", "parent", "1").setSource("{}", XContentType.JSON).get();
        refresh();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}", XContentType.JSON).get();
        refresh();

        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), greaterThan(0L));
    }

    public void testChangingEagerParentFieldLoadingAtRuntime() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(indexSettings)
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}", XContentType.JSON).get();
        refresh();

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), equalTo(0L));

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("child")
                .setSource(childMapping(true))
                .setUpdateAllTypes(true)
                .get();
        assertAcked(putMappingResponse);
        Index test = resolveIndex("test");
        assertBusy(() -> {
            ClusterState clusterState = internalCluster().clusterService().state();
            ShardRouting shardRouting = clusterState.routingTable().index("test").shard(0).getShards().get(0);
            String nodeName = clusterState.getNodes().get(shardRouting.currentNodeId()).getName();

            boolean verified = false;
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
            IndexService indexService = indicesService.indexService(test);
            if (indexService != null) {
                MapperService mapperService = indexService.mapperService();
                DocumentMapper documentMapper = mapperService.documentMapper("child");
                if (documentMapper != null) {
                    verified = documentMapper.parentFieldMapper().fieldType().eagerGlobalOrdinals();
                }
            }
            assertTrue(verified);
        });

        // Need to add a new doc otherwise the refresh doesn't trigger a new searcher
        // Because it ends up in its own segment, but isn't of type parent or child, this doc doesn't contribute to the size of the fielddata cache
        client().prepareIndex("test", "dummy", "dummy").setSource("{}", XContentType.JSON).get();
        refresh();
        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), greaterThan(0L));
    }

    private XContentBuilder childMapping(boolean eagerGlobalOrds) throws IOException {
        return jsonBuilder().startObject().startObject("child").startObject("_parent")
                .field("type", "parent")
                .startObject("fielddata").field("eager_global_ordinals", eagerGlobalOrds).endObject()
                .endObject().endObject().endObject();
    }

}
