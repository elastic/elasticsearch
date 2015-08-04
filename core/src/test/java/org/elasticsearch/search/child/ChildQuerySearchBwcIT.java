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
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasParentQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE)
public class ChildQuerySearchBwcIT extends ChildQuerySearchIT {

    @Override
    public Settings indexSettings() {
        return settings(Version.V_1_6_0).put(super.indexSettings()).build();
    }

    public void testSelfReferentialIsForbidden() {
        // we allowed this, but it was actually broken. The has_child/has_parent results were sometimes wrong...
        assertAcked(prepareCreate("test").addMapping("type", "_parent", "type=type"));
    }

    @Test
    public void testAdd_ParentFieldAfterIndexingParentDocButBeforeIndexingChildDoc() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder()
                        .put(indexSettings())
                        .put("index.refresh_interval", -1)));
        ensureGreen();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId).setSource("p_field", "1").get();
        refresh();
        assertAcked(client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource("_parent", "type=parent"));
        client().prepareIndex("test", "child", "c1").setSource("c_field", "1").setParent(parentId).get();
        client().admin().indices().prepareRefresh().get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("c_field", "1")))
                .get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, parentId);

        searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreType("max"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, parentId);


        searchResponse = client().prepareSearch("test")
                .setPostFilter(hasChildQuery("child", termQuery("c_field", "1")))
                .get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, parentId);

        searchResponse = client().prepareSearch("test")
                .setPostFilter(hasParentQuery("parent", termQuery("p_field", "1")))
                .get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "c1");

        searchResponse = client().prepareSearch("test")
                .setQuery(hasParentQuery("parent", termQuery("p_field", "1")).scoreType("score"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, "c1");
    }

    @Test
    public void testExplainUsage() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId).setSource("p_field", "1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "1").setParent(parentId).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setExplain(true)
                .setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreType("max"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).explanation().getDescription(), equalTo("not implemented yet..."));

        searchResponse = client().prepareSearch("test")
                .setExplain(true)
                .setQuery(hasParentQuery("parent", termQuery("p_field", "1")).scoreType("score"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).explanation().getDescription(), equalTo("not implemented yet..."));

        ExplainResponse explainResponse = client().prepareExplain("test", "parent", parentId)
                .setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreType("max"))
                .get();
        assertThat(explainResponse.isExists(), equalTo(true));
        // TODO: improve test once explanations are actually implemented
        assertThat(explainResponse.getExplanation().toString(), startsWith("1.0 ="));
    }

    @Test
    public void testParentFieldDataCacheBug() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put(indexSettings())
                        .put("index.refresh_interval", -1)) // Disable automatic refresh, so that the _parent doesn't get warmed
                .addMapping("parent", jsonBuilder().startObject().startObject("parent")
                        .startObject("properties")
                        .startObject("p_field")
                        .field("type", "string")
                        .startObject("fielddata")
                        .field(FieldDataType.FORMAT_KEY, MappedFieldType.Loading.LAZY)
                        .endObject()
                        .endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();

        client().prepareIndex("test", "parent", "p0").setSource("p_field", "p_value0").get();
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();

        refresh();
        // No _parent field yet, there shouldn't be anything in the field data for _parent field
        IndicesStatsResponse indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setFieldData(true).get();
        assertThat(indicesStatsResponse.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0l));

        // Now add mapping + children
        client().admin().indices().preparePutMapping("test").setType("child")
                .setSource(XContentFactory.jsonBuilder().startObject().startObject("child")
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
                        .endObject().endObject().endObject())
                .get();

        // index simple data
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();

        refresh();

        indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setFieldData(true).setFieldDataFields("_parent").get();
        assertThat(indicesStatsResponse.getTotal().getFieldData().getMemorySizeInBytes(), greaterThan(0l));
        assertThat(indicesStatsResponse.getTotal().getFieldData().getFields().get("_parent"), greaterThan(0l));

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "blue"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setFieldData(true).setFieldDataFields("_parent").get();
        assertThat(indicesStatsResponse.getTotal().getFieldData().getMemorySizeInBytes(), greaterThan(0l));
        assertThat(indicesStatsResponse.getTotal().getFieldData().getFields().get("_parent"), greaterThan(0l));

        ClearIndicesCacheResponse clearCacheResponse = client().admin().indices().prepareClearCache("test").setFieldDataCache(true).get();
        assertNoFailures(clearCacheResponse);
        assertAllSuccessful(clearCacheResponse);
        indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setFieldData(true).setFieldDataFields("_parent").get();
        assertThat(indicesStatsResponse.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0l));
        assertThat(indicesStatsResponse.getTotal().getFieldData().getFields().get("_parent"), equalTo(0l));
    }
}
