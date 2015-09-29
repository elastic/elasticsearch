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
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.HasChildQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.MergePolicyConfig;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ParentFieldLoadingBwcIT extends ESIntegTestCase {

    private final Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexShard.INDEX_REFRESH_INTERVAL, -1)
                    // We never want merges in this test to ensure we have two segments for the last validation
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_6_0)
            .build();

    public void testParentFieldDataCacheBug() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put(indexSettings)
                        .put("index.refresh_interval", -1)) // Disable automatic refresh, so that the _parent doesn't get warmed
                .addMapping("parent", XContentFactory.jsonBuilder().startObject().startObject("parent")
                        .startObject("properties")
                        .startObject("p_field")
                        .field("type", "string")
                        .startObject("fielddata")
                        .field(FieldDataType.FORMAT_KEY, MappedFieldType.Loading.LAZY)
                        .endObject()
                        .endObject()
                        .endObject().endObject().endObject())
                .addMapping("child", XContentFactory.jsonBuilder().startObject().startObject("child")
                        .startObject("_parent")
                        .field("type", "parent")
                        .endObject()
                        .startObject("properties")
                        .startObject("c_field")
                        .field("type", "string")
                        .startObject("fielddata")
                        .field(FieldDataType.FORMAT_KEY, MappedFieldType.Loading.LAZY)
                        .endObject()
                        .endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();

        client().prepareIndex("test", "parent", "p0").setSource("p_field", "p_value0").get();
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();
        refresh();

        IndicesStatsResponse statsResponse = client().admin().indices()
                .prepareStats("test").setFieldData(true).setFieldDataFields("_parent").get();
        assertThat(statsResponse.getTotal().getFieldData().getMemorySizeInBytes(), greaterThan(0l));
        assertThat(statsResponse.getTotal().getFieldData().getFields().get("_parent"), greaterThan(0l));

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "blue"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        statsResponse = client().admin().indices()
                .prepareStats("test").setFieldData(true).setFieldDataFields("_parent").get();
        assertThat(statsResponse.getTotal().getFieldData().getMemorySizeInBytes(), greaterThan(0l));
        assertThat(statsResponse.getTotal().getFieldData().getFields().get("_parent"), greaterThan(0l));

        ClearIndicesCacheResponse clearCacheResponse = client().admin().indices().prepareClearCache("test").setFieldDataCache(true).get();
        assertNoFailures(clearCacheResponse);
        assertAllSuccessful(clearCacheResponse);
        statsResponse = client().admin().indices()
                .prepareStats("test").setFieldData(true).setFieldDataFields("_parent").get();
        assertThat(statsResponse.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0l));
        assertThat(statsResponse.getTotal().getFieldData().getFields().get("_parent"), equalTo(0l));
    }

    public void testEagerParentFieldLoading() throws Exception {
        logger.info("testing lazy loading...");
        assertAcked(prepareCreate("test")
                .setSettings(indexSettings)
                .addMapping("parent")
                .addMapping("child", childMapping(MappedFieldType.Loading.LAZY)));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("{}").get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}").get();
        refresh();

        IndicesStatsResponse r = client().admin().indices().prepareStats("test").setFieldData(true).setFieldDataFields("*").get();
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), equalTo(0l));

        logger.info("testing default loading...");
        assertAcked(client().admin().indices().prepareDelete("test").get());
        assertAcked(prepareCreate("test")
                .setSettings(indexSettings)
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("{}").get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}").get();
        refresh();

        response = client().admin().cluster().prepareClusterStats().get();
        long fielddataSizeDefault = response.getIndicesStats().getFieldData().getMemorySizeInBytes();
        assertThat(fielddataSizeDefault, greaterThan(0l));

        logger.info("testing eager loading...");
        assertAcked(client().admin().indices().prepareDelete("test").get());
        assertAcked(prepareCreate("test")
                .setSettings(indexSettings)
                .addMapping("parent")
                .addMapping("child", childMapping(MappedFieldType.Loading.EAGER)));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("{}").get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}").get();
        refresh();

        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), equalTo(fielddataSizeDefault));

        logger.info("testing eager global ordinals loading...");
        assertAcked(client().admin().indices().prepareDelete("test").get());
        assertAcked(prepareCreate("test")
            .setSettings(indexSettings)
            .addMapping("parent")
            .addMapping("child", childMapping(MappedFieldType.Loading.EAGER_GLOBAL_ORDINALS)));
        ensureGreen();

        // Need to do 2 separate refreshes, otherwise we have 1 segment and then we can't measure if global ordinals
        // is loaded by the size of the field data cache, because global ordinals on 1 segment shards takes no extra memory.
        client().prepareIndex("test", "parent", "1").setSource("{}").get();
        refresh();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}").get();
        refresh();

        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), greaterThan(fielddataSizeDefault));
    }
    
    public void testChangingEagerParentFieldLoadingAtRuntime() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(indexSettings)
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("{}").get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}").get();
        refresh();

        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
        long fielddataSizeDefault = response.getIndicesStats().getFieldData().getMemorySizeInBytes();
        assertThat(fielddataSizeDefault, greaterThan(0l));

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("child")
                .setSource(childMapping(MappedFieldType.Loading.EAGER_GLOBAL_ORDINALS))
                .setUpdateAllTypes(true)
                .get();
        assertAcked(putMappingResponse);
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState clusterState = internalCluster().clusterService().state();
                ShardRouting shardRouting = clusterState.routingTable().index("test").shard(0).getShards().get(0);
                String nodeName = clusterState.getNodes().get(shardRouting.currentNodeId()).getName();

                boolean verified = false;
                IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
                IndexService indexService = indicesService.indexService("test");
                if (indexService != null) {
                    MapperService mapperService = indexService.mapperService();
                    DocumentMapper documentMapper = mapperService.documentMapper("child");
                    if (documentMapper != null) {
                        verified = documentMapper.parentFieldMapper().getChildJoinFieldType().fieldDataType().getLoading() == MappedFieldType.Loading.EAGER_GLOBAL_ORDINALS;
                    }
                }
                assertTrue(verified);
            }
        });

        // Need to add a new doc otherwise the refresh doesn't trigger a new searcher
        // Because it ends up in its own segment, but isn't of type parent or child, this doc doesn't contribute to the size of the fielddata cache
        client().prepareIndex("test", "dummy", "dummy").setSource("{}").get();
        refresh();
        response = client().admin().cluster().prepareClusterStats().get();
        assertThat(response.getIndicesStats().getFieldData().getMemorySizeInBytes(), greaterThan(fielddataSizeDefault));
    }

    private XContentBuilder childMapping(MappedFieldType.Loading loading) throws IOException {
        return jsonBuilder().startObject().startObject("child").startObject("_parent")
                .field("type", "parent")
                .startObject("fielddata").field(MappedFieldType.Loading.KEY, loading).endObject()
                .endObject().endObject().endObject();
    }
    
    static HasChildQueryBuilder hasChildQuery(String type, QueryBuilder queryBuilder) {
        HasChildQueryBuilder hasChildQueryBuilder = QueryBuilders.hasChildQuery(type, queryBuilder);
        hasChildQueryBuilder.setShortCircuitCutoff(randomInt(10));
        return hasChildQueryBuilder;
    }

}
