/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.child;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.search.child.ScoreType;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.facet.FacetBuilders.termsFacet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleChildQuerySearchTests extends ElasticsearchIntegrationTest {

    @Test
    public void multiLevelChild() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("grandchild")
                .setSource(
                        jsonBuilder().startObject().startObject("grandchild").startObject("_parent").field("type", "child").endObject()
                                .endObject().endObject()).execute().actionGet();

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "c_value1").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "grandchild", "gc1").setSource("gc_field", "gc_value1").setParent("c1").setRouting("gc1").execute()
                .actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        filteredQuery(
                                matchAllQuery(),
                                hasChildFilter(
                                        "child",
                                        filteredQuery(termQuery("c_field", "c_value1"),
                                                hasChildFilter("grandchild", termQuery("gc_field", "gc_value1")))))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasParentFilter("parent", termFilter("p_field", "p_value1")))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasParentFilter("child", termFilter("c_field", "c_value1")))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("gc1"));

        searchResponse = client().prepareSearch("test").setQuery(hasParentQuery("parent", termQuery("p_field", "p_value1"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));

        searchResponse = client().prepareSearch("test").setQuery(hasParentQuery("child", termQuery("c_field", "c_value1"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("gc1"));
    }

    @Test
    // see #2744
    public void test2744() throws ElasticSearchException, IOException {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("test")
                .setSource(
                        jsonBuilder().startObject().startObject("test").startObject("_parent").field("type", "foo").endObject().endObject()
                                .endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "foo", "1").setSource("foo", 1).execute().actionGet();
        client().prepareIndex("test", "test").setSource("foo", 1).setParent("1").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("test", matchQuery("foo", 1))).execute()
                .actionGet();
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

    }

    @Test
    public void simpleChildQuery() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        // TEST FETCHING _parent from child
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(idsQuery("child").ids("c1")).addFields("_parent").execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));

        // TEST matching on parent
        searchResponse = client().prepareSearch("test").setQuery(termQuery("child._parent", "p1")).addFields("_parent").execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(1).field("_parent").value().toString(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(termQuery("_parent", "p1")).addFields("_parent").execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(1).field("_parent").value().toString(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(queryString("_parent:p1")).addFields("_parent").execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(1).field("_parent").value().toString(), equalTo("p1"));

        // TOP CHILDREN QUERY
        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute()
                .actionGet();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "blue")))
                .execute().actionGet();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute()
                .actionGet();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD
        searchResponse = client().prepareSearch("test").setQuery(randomHasChild("child", "c_field", "yellow"))
                .execute().actionGet();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(randomHasChild("child", "c_field", "blue")).execute()
                .actionGet();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(randomHasChild("child", "c_field", "red")).execute().actionGet();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS PARENT
        searchResponse = client().prepareSearch("test")
                .setQuery(randomHasParent("parent", "p_field", "p_value2")).execute().actionGet();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c3"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c4"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomHasParent("parent", "p_field", "p_value1")).execute().actionGet();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c2"));
    }

    @Test
    public void testClearIdCacheBug() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "parent", "p0").setSource("p_field", "p_value0").execute().actionGet();
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();
        // No _parent field yet, there shouldn't be anything in the parent id cache
        IndicesStatsResponse indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setIdCache(true).execute().actionGet();
        assertThat(indicesStatsResponse.getTotal().getIdCache().getMemorySizeInBytes(), equalTo(0l));

        // Now add mapping + children
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("child")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setIdCache(true).execute().actionGet();
        // automatic warm-up has populated the cache since it found a parent field mapper
        assertThat(indicesStatsResponse.getTotal().getIdCache().getMemorySizeInBytes(), greaterThan(0l));

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue"))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setIdCache(true).execute().actionGet();
        assertThat(indicesStatsResponse.getTotal().getIdCache().getMemorySizeInBytes(), greaterThan(0l));

        client().admin().indices().prepareClearCache("test").setIdCache(true).execute().actionGet();
        indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setIdCache(true).execute().actionGet();
        assertThat(indicesStatsResponse.getTotal().getIdCache().getMemorySizeInBytes(), equalTo(0l));
    }

    @Test
    // See: https://github.com/elasticsearch/elasticsearch/issues/3290
    public void testCachingBug_withFqueryFilter() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "parent", Integer.toString(i)).setSource("p_field", i).execute().actionGet();
        }
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "child", Integer.toString(i)).setSource("c_field", i).setParent("" + 0).execute().actionGet();
        }
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "child", Integer.toString(i + 10)).setSource("c_field", i + 10).setParent(Integer.toString(i))
                    .execute().actionGet();
        }
        client().admin().indices().prepareFlush().execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(queryFilter(topChildrenQuery("child", matchAllQuery())).cache(true))).execute()
                    .actionGet();
            assertNoFailures(searchResponse);
            searchResponse = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(queryFilter(hasChildQuery("child", matchAllQuery()).scoreType("max")).cache(true)))
                    .execute().actionGet();
            assertNoFailures(searchResponse);
            searchResponse = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(queryFilter(hasParentQuery("parent", matchAllQuery()).scoreType("score")).cache(true)))
                    .execute().actionGet();
            assertNoFailures(searchResponse);
        }
    }

    @Test
    public void testHasParentFilter() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        Map<String, Set<String>> parentToChildren = newHashMap();
        // Childless parent
        client().prepareIndex("test", "parent", "p0").setSource("p_field", "p0").execute().actionGet();
        parentToChildren.put("p0", new HashSet<String>());

        String previousParentId = null;
        int numChildDocs = 32;
        int numChildDocsPerParent = 0;
        List<IndexRequestBuilder> builders = new ArrayList<IndexRequestBuilder>();
        for (int i = 1; i <= numChildDocs; i++) {

            if (previousParentId == null || i % numChildDocsPerParent == 0) {
                previousParentId = "p" + i;
                builders.add(client().prepareIndex("test", "parent", previousParentId).setSource("p_field", previousParentId));
                numChildDocsPerParent++;
            }

            String childId = "c" + i;
            builders.add(client().prepareIndex("test", "child", childId).setSource("c_field", childId).setParent(previousParentId));

            if (!parentToChildren.containsKey(previousParentId)) {
                parentToChildren.put(previousParentId, new HashSet<String>());
            }
            assertThat(parentToChildren.get(previousParentId).add(childId), is(true));
        }
        indexRandom(true, builders.toArray(new IndexRequestBuilder[0]));

        assertThat(parentToChildren.isEmpty(), equalTo(false));
        for (Map.Entry<String, Set<String>> parentToChildrenEntry : parentToChildren.entrySet()) {
            SearchResponse searchResponse = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", parentToChildrenEntry.getKey()))))
                    .setSize(numChildDocsPerParent).execute().actionGet();

            assertNoFailures(searchResponse);
            assertThat(searchResponse.getFailedShards(), equalTo(0));
            Set<String> childIds = parentToChildrenEntry.getValue();
            assertThat(searchResponse.getHits().totalHits(), equalTo((long) childIds.size()));
            for (int i = 0; i < searchResponse.getHits().totalHits(); i++) {
                assertThat(childIds.remove(searchResponse.getHits().getAt(i).id()), is(true));
                assertThat(searchResponse.getHits().getAt(i).score(), is(1.0f));
            }
            assertThat(childIds.size(), is(0));
        }
    }

    @Test
    public void simpleChildQueryWithFlush() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data with flushes, so we have many segments
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        // TOP CHILDREN QUERY

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow")))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "blue"))).execute()
                .actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD QUERY

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "blue"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD FILTER

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue"))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red"))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));
    }

    @Test
    public void simpleChildQueryWithFlushAnd3Shards() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 3).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data with flushes, so we have many segments
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        // TOP CHILDREN QUERY

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow")))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "blue"))).execute()
                .actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD QUERY

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "blue"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD FILTER

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue"))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red"))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));
    }

    @Test
    public void testScopedFacet() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(topChildrenQuery("child", boolQuery().should(termQuery("c_field", "red")).should(termQuery("c_field", "yellow"))))
                .addFacet(
                        termsFacet("facet1")
                                .facetFilter(boolFilter().should(termFilter("c_field", "red")).should(termFilter("c_field", "yellow")))
                                .field("c_field").global(true)).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        assertThat(searchResponse.getFacets().facets().size(), equalTo(1));
        TermsFacet termsFacet = searchResponse.getFacets().facet("facet1");
        assertThat(termsFacet.getEntries().size(), equalTo(2));
        assertThat(termsFacet.getEntries().get(0).getTerm().string(), equalTo("red"));
        assertThat(termsFacet.getEntries().get(0).getCount(), equalTo(2));
        assertThat(termsFacet.getEntries().get(1).getTerm().string(), equalTo("yellow"));
        assertThat(termsFacet.getEntries().get(1).getCount(), equalTo(1));
    }

    @Test
    public void testDeletedParent() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        // TOP CHILDREN QUERY

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow")))
                .execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        // update p1 and see what that we get updated values...

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1_updated").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1_updated\""));

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1_updated\""));
    }

    @Test
    public void testDfsSearchType() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(hasChildQuery("child", boolQuery().should(queryString("c_field:*"))))).execute().actionGet();
        assertNoFailures(searchResponse);

        searchResponse = client().prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(hasParentQuery("parent", boolQuery().should(queryString("p_field:*"))))).execute()
                .actionGet();
        assertNoFailures(searchResponse);

        searchResponse = client().prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(topChildrenQuery("child", boolQuery().should(queryString("c_field:*"))))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
    }

    @Test
    public void testFixAOBEIfTopChildrenIsWrappedInMusNotClause() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test").setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(topChildrenQuery("child", boolQuery().should(queryString("c_field:*"))))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
    }

    @Test
    public void testTopChildrenReSearchBug() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        int numberOfParents = 4;
        int numberOfChildrenPerParent = 123;
        for (int i = 1; i <= numberOfParents; i++) {
            String parentId = String.format(Locale.ROOT, "p%d", i);
            client().prepareIndex("test", "parent", parentId).setSource("p_field", String.format(Locale.ROOT, "p_value%d", i)).execute()
                    .actionGet();
            for (int j = 1; j <= numberOfChildrenPerParent; j++) {
                client().prepareIndex("test", "child", String.format(Locale.ROOT, "%s_c%d", parentId, j))
                        .setSource("c_field1", parentId, "c_field2", i % 2 == 0 ? "even" : "not_even").setParent(parentId).execute()
                        .actionGet();
            }
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field1", "p3")))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p3"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field2", "even"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));
    }

    @Test
    public void testHasChildAndHasParentFailWhenSomeSegmentsDontContainAnyParentOrChildDocs() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        client().prepareIndex("test", "parent", "1").setSource("p_field", 1).execute().actionGet();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("c_field", 1).execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("p_field", "p_value1").execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasChildFilter("child", matchAllQuery()))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasParentFilter("parent", matchAllQuery()))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testCountApiUsage() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId).setSource("p_field", "1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "1").setParent(parentId).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        CountResponse countResponse = client().prepareCount("test").setQuery(topChildrenQuery("child", termQuery("c_field", "1")))
                .execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreType("max"))
                .execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(hasParentQuery("parent", termQuery("p_field", "1")).scoreType("score"))
                .execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "1"))))
                .execute().actionGet();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", "1"))))
                .execute().actionGet();
        assertHitCount(countResponse, 1l);
    }

    @Test
    public void testExplainUsage() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId).setSource("p_field", "1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "1").setParent(parentId).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setExplain(true)
                .setQuery(topChildrenQuery("child", termQuery("c_field", "1")))
                .execute().actionGet();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).explanation().getDescription(), equalTo("not implemented yet..."));

        searchResponse = client().prepareSearch("test")
                .setExplain(true)
                .setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreType("max"))
                .execute().actionGet();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).explanation().getDescription(), equalTo("not implemented yet..."));

        searchResponse = client().prepareSearch("test")
                .setExplain(true)
                .setQuery(hasParentQuery("parent", termQuery("p_field", "1")).scoreType("score"))
                .execute().actionGet();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).explanation().getDescription(), equalTo("not implemented yet..."));

        ExplainResponse explainResponse = client().prepareExplain("test", "parent", parentId)
                .setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreType("max"))
                .execute().actionGet();
        assertThat(explainResponse.isExists(), equalTo(true));
        assertThat(explainResponse.getExplanation().getDescription(), equalTo("not implemented yet..."));
    }

    @Test
    public void testScoreForParentChildQueries() throws Exception {

        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "child",
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject())
                .addMapping(
                        "child1",
                        jsonBuilder().startObject().startObject("child1").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        indexRandom(false, createDocBuilders().toArray(new IndexRequestBuilder[0]));
        refresh();

        SearchResponse response = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasChildQuery("child",
                                QueryBuilders.customScoreQuery(matchQuery("c_field2", 0)).script("doc['c_field1'].value")).scoreType("sum"))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("1"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(4f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(3f));

        response = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasChildQuery("child",
                                QueryBuilders.customScoreQuery(matchQuery("c_field2", 0)).script("doc['c_field1'].value")).scoreType("max"))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(4f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("1"));
        assertThat(response.getHits().hits()[2].score(), equalTo(2f));

        response = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasChildQuery("child",
                                QueryBuilders.customScoreQuery(matchQuery("c_field2", 0)).script("doc['c_field1'].value")).scoreType("avg"))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(4f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("1"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1.5f));

        response = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasParentQuery("parent",
                                QueryBuilders.customScoreQuery(matchQuery("p_field1", "p_value3")).script("doc['p_field2'].value"))
                                .scoreType("score")).addSort(SortBuilders.fieldSort("c_field3")).addSort(SortBuilders.scoreSort())
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(7l));
        assertThat(response.getHits().hits()[0].id(), equalTo("13"));
        assertThat(response.getHits().hits()[0].score(), equalTo(5f));
        assertThat(response.getHits().hits()[1].id(), equalTo("14"));
        assertThat(response.getHits().hits()[1].score(), equalTo(5f));
        assertThat(response.getHits().hits()[2].id(), equalTo("15"));
        assertThat(response.getHits().hits()[2].score(), equalTo(5f));
        assertThat(response.getHits().hits()[3].id(), equalTo("16"));
        assertThat(response.getHits().hits()[3].score(), equalTo(5f));
        assertThat(response.getHits().hits()[4].id(), equalTo("17"));
        assertThat(response.getHits().hits()[4].score(), equalTo(5f));
        assertThat(response.getHits().hits()[5].id(), equalTo("18"));
        assertThat(response.getHits().hits()[5].score(), equalTo(5f));
        assertThat(response.getHits().hits()[6].id(), equalTo("1"));
        assertThat(response.getHits().hits()[6].score(), equalTo(5f));
    }

    List<IndexRequestBuilder> createDocBuilders() {
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        // Parent 1 and its children
        indexBuilders.add(client().prepareIndex().setType("parent").setId("1").setIndex("test").setSource("p_field", "p_value1"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("1").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 0).setParent("1"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("2").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 0).setParent("1"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("3").setIndex("test")
                .setSource("c_field1", 2, "c_field2", 0).setParent("1"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("4").setIndex("test")
                .setSource("c_field1", 2, "c_field2", 0).setParent("1"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("5").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 1).setParent("1"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("6").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 2).setParent("1"));

        // Parent 2 and its children
        indexBuilders.add(client().prepareIndex().setType("parent").setId("2").setIndex("test").setSource("p_field", "p_value2"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("7").setIndex("test")
                .setSource("c_field1", 3, "c_field2", 0).setParent("2"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("8").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 1).setParent("2"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("9").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 1).setParent("p")); // why
        // "p"????
        indexBuilders.add(client().prepareIndex().setType("child").setId("10").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 1).setParent("2"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("11").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 1).setParent("2"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("12").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 2).setParent("2"));

        // Parent 3 and its children

        indexBuilders.add(client().prepareIndex().setType("parent").setId("3").setIndex("test")
                .setSource("p_field1", "p_value3", "p_field2", 5));
        indexBuilders.add(client().prepareIndex().setType("child").setId("13").setIndex("test")
                .setSource("c_field1", 4, "c_field2", 0, "c_field3", 0).setParent("3"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("14").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 1, "c_field3", 1).setParent("3"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("15").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 2, "c_field3", 2).setParent("3")); // why
        // "p"????
        indexBuilders.add(client().prepareIndex().setType("child").setId("16").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 2, "c_field3", 3).setParent("3"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("17").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 2, "c_field3", 4).setParent("3"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("18").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 2, "c_field3", 5).setParent("3"));
        indexBuilders.add(client().prepareIndex().setType("child1").setId("1").setIndex("test")
                .setSource("c_field1", 1, "c_field2", 2, "c_field3", 6).setParent("3"));

        return indexBuilders;
    }

    @Test
    public void testScoreForParentChildQueries_withFunctionScore() throws Exception {

        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "child",
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject())
                .addMapping(
                        "child1",
                        jsonBuilder().startObject().startObject("child1").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        indexRandom(false, createDocBuilders().toArray(new IndexRequestBuilder[0]));
        refresh();
        SearchResponse response = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasChildQuery(
                                "child",
                                QueryBuilders.functionScoreQuery(matchQuery("c_field2", 0), scriptFunction("doc['c_field1'].value"))
                                        .boostMode(CombineFunction.REPLACE.getName())).scoreType("sum")).execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("1"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(4f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(3f));

        response = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasChildQuery(
                                "child",
                                QueryBuilders.functionScoreQuery(matchQuery("c_field2", 0), scriptFunction("doc['c_field1'].value"))
                                        .boostMode(CombineFunction.REPLACE.getName())).scoreType("max")).execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(4f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("1"));
        assertThat(response.getHits().hits()[2].score(), equalTo(2f));

        response = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasChildQuery(
                                "child",
                                QueryBuilders.functionScoreQuery(matchQuery("c_field2", 0), scriptFunction("doc['c_field1'].value"))
                                        .boostMode(CombineFunction.REPLACE.getName())).scoreType("avg")).execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(4f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("1"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1.5f));

        response = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasParentQuery(
                                "parent",
                                QueryBuilders.functionScoreQuery(matchQuery("p_field1", "p_value3"), scriptFunction("doc['p_field2'].value"))
                                        .boostMode(CombineFunction.REPLACE.getName())).scoreType("score"))
                .addSort(SortBuilders.fieldSort("c_field3")).addSort(SortBuilders.scoreSort()).execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(7l));
        assertThat(response.getHits().hits()[0].id(), equalTo("13"));
        assertThat(response.getHits().hits()[0].score(), equalTo(5f));
        assertThat(response.getHits().hits()[1].id(), equalTo("14"));
        assertThat(response.getHits().hits()[1].score(), equalTo(5f));
        assertThat(response.getHits().hits()[2].id(), equalTo("15"));
        assertThat(response.getHits().hits()[2].score(), equalTo(5f));
        assertThat(response.getHits().hits()[3].id(), equalTo("16"));
        assertThat(response.getHits().hits()[3].score(), equalTo(5f));
        assertThat(response.getHits().hits()[4].id(), equalTo("17"));
        assertThat(response.getHits().hits()[4].score(), equalTo(5f));
        assertThat(response.getHits().hits()[5].id(), equalTo("18"));
        assertThat(response.getHits().hits()[5].score(), equalTo(5f));
        assertThat(response.getHits().hits()[6].id(), equalTo("1"));
        assertThat(response.getHits().hits()[6].score(), equalTo(5f));
    }

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/2536
    public void testParentChildQueriesCanHandleNoRelevantTypesInIndex() throws Exception {

        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping("parent", jsonBuilder().startObject().startObject("parent").endObject().endObject())
                .addMapping(
                        "child",
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value"))).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(0l));

        client().prepareIndex("test", "child1").setSource(jsonBuilder().startObject().field("text", "value").endObject()).setRefresh(true)
                .execute().actionGet();

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value"))).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value")).scoreType("max"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasParentQuery("child", matchQuery("text", "value"))).execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasParentQuery("child", matchQuery("text", "value")).scoreType("score"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(0l));
    }

    @Test
    public void testHasChildAndHasParentFilter_withFilter() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        client().prepareIndex("test", "parent", "1").setSource("p_field", 1).execute().actionGet();
        client().prepareIndex("test", "child", "2").setParent("1").setSource("c_field", 1).execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource("p_field", "p_value1").execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasChildFilter("child", termFilter("c_field", 1)))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasParentFilter("parent", termFilter("p_field", 1)))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("2"));
    }

    @Test
    public void testSimpleQueryRewrite() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data
        int childId = 0;
        for (int i = 0; i < 10; i++) {
            String parentId = String.format(Locale.ROOT, "p%03d", i);
            client().prepareIndex("test", "parent", parentId).setSource("p_field", parentId).execute().actionGet();
            int j = childId;
            for (; j < childId + 50; j++) {
                String childUid = String.format(Locale.ROOT, "c%03d", j);
                client().prepareIndex("test", "child", childUid).setSource("c_field", childUid).setParent(parentId).execute().actionGet();
            }
            childId = j;
        }
        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchType[] searchTypes = new SearchType[]{SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH};
        for (SearchType searchType : searchTypes) {
            SearchResponse searchResponse = client().prepareSearch("test").setSearchType(searchType)
                    .setQuery(hasChildQuery("child", prefixQuery("c_field", "c")).scoreType("max")).addSort("p_field", SortOrder.ASC)
                    .setSize(5).execute().actionGet();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(10L));
            assertThat(searchResponse.getHits().hits()[0].id(), equalTo("p000"));
            assertThat(searchResponse.getHits().hits()[1].id(), equalTo("p001"));
            assertThat(searchResponse.getHits().hits()[2].id(), equalTo("p002"));
            assertThat(searchResponse.getHits().hits()[3].id(), equalTo("p003"));
            assertThat(searchResponse.getHits().hits()[4].id(), equalTo("p004"));

            searchResponse = client().prepareSearch("test").setSearchType(searchType)
                    .setQuery(hasParentQuery("parent", prefixQuery("p_field", "p")).scoreType("score")).addSort("c_field", SortOrder.ASC)
                    .setSize(5).execute().actionGet();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(500L));
            assertThat(searchResponse.getHits().hits()[0].id(), equalTo("c000"));
            assertThat(searchResponse.getHits().hits()[1].id(), equalTo("c001"));
            assertThat(searchResponse.getHits().hits()[2].id(), equalTo("c002"));
            assertThat(searchResponse.getHits().hits()[3].id(), equalTo("c003"));
            assertThat(searchResponse.getHits().hits()[4].id(), equalTo("c004"));

            searchResponse = client().prepareSearch("test").setSearchType(searchType)
                    .setQuery(topChildrenQuery("child", prefixQuery("c_field", "c"))).addSort("p_field", SortOrder.ASC).setSize(5)
                    .execute().actionGet();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(10L));
            assertThat(searchResponse.getHits().hits()[0].id(), equalTo("p000"));
            assertThat(searchResponse.getHits().hits()[1].id(), equalTo("p001"));
            assertThat(searchResponse.getHits().hits()[2].id(), equalTo("p002"));
            assertThat(searchResponse.getHits().hits()[3].id(), equalTo("p003"));
            assertThat(searchResponse.getHits().hits()[4].id(), equalTo("p004"));
        }
    }

    @Test
    // See also issue:
    // https://github.com/elasticsearch/elasticsearch/issues/3144
    public void testReIndexingParentAndChildDocuments() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "x").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "x").setParent("p2").execute().actionGet();

        client().admin().indices().prepareRefresh("test").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("c_field", "yellow")).scoreType("sum")).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        boolQuery().must(matchQuery("c_field", "x")).must(
                                hasParentQuery("parent", termQuery("p_field", "p_value2")).scoreType("score"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c3"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c4"));

        // re-index
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
            client().prepareIndex("test", "child", "d" + i).setSource("c_field", "red").setParent("p1").execute().actionGet();
            client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
            client().prepareIndex("test", "child", "c3").setSource("c_field", "x").setParent("p2").execute().actionGet();
            client().admin().indices().prepareRefresh("test").execute().actionGet();
        }

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow")).scoreType("sum"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        boolQuery().must(matchQuery("c_field", "x")).must(
                                hasParentQuery("parent", termQuery("p_field", "p_value2")).scoreType("score"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), Matchers.anyOf(equalTo("c3"), equalTo("c4")));
        assertThat(searchResponse.getHits().getAt(1).id(), Matchers.anyOf(equalTo("c3"), equalTo("c4")));
    }

    @Test
    // See also issue:
    // https://github.com/elasticsearch/elasticsearch/issues/3203
    public void testHasChildQueryWithMinimumScore() throws Exception {

        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "x").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "x").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "x").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c5").setSource("c_field", "x").setParent("p2").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", matchAllQuery()).scoreType("sum"))
                .setMinScore(3) // Score needs to be 3 or above!
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(3.0f));
    }

    @Test
    public void testParentFieldFilter() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1)
                                .put("index.refresh_interval", -1)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child")
                .setSource(
                        jsonBuilder().startObject().startObject("child").startObject("_parent").field("type", "parent").endObject()
                                .endObject().endObject()).execute().actionGet();
        client().admin()
                .indices()
                .preparePutMapping("test")
                .setType("child2")
                .setSource(
                        jsonBuilder().startObject().startObject("child2").startObject("_parent").field("type", "parent2").endObject()
                                .endObject().endObject()).execute().actionGet();

        // test term filter
        SearchResponse response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("_parent", "p1")))
                .execute().actionGet();
        assertHitCount(response, 0l);

        client().prepareIndex("test", "some_type", "1").setSource("field", "value").execute().actionGet();
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "value").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "value").setParent("p1").execute().actionGet();

        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 0l);
        refresh();

        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("_parent", "parent#p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        client().prepareIndex("test", "parent2", "p1").setSource("p_field", "value").setRefresh(true).execute().actionGet();

        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("_parent", "parent#p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        // test terms filter
        client().prepareIndex("test", "child2", "c1").setSource("c_field", "value").setParent("p1").execute().actionGet();
        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("_parent", "parent#p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        refresh();
        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 2l);

        refresh();
        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termsFilter("_parent", "p1", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 2l);

        response = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("_parent", "parent#p1", "parent2#p1"))).execute().actionGet();
        assertHitCount(response, 2l);
    }

    @Test
    public void testHasChildNotBeingCached() throws ElasticSearchException, IOException {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("child")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "parent", "p3").setSource("p_field", "p_value3").execute().actionGet();
        client().prepareIndex("test", "parent", "p4").setSource("p_field", "p_value4").execute().actionGet();
        client().prepareIndex("test", "parent", "p5").setSource("p_field", "p_value5").execute().actionGet();
        client().prepareIndex("test", "parent", "p6").setSource("p_field", "p_value6").execute().actionGet();
        client().prepareIndex("test", "parent", "p7").setSource("p_field", "p_value7").execute().actionGet();
        client().prepareIndex("test", "parent", "p8").setSource("p_field", "p_value8").execute().actionGet();
        client().prepareIndex("test", "parent", "p9").setSource("p_field", "p_value9").execute().actionGet();
        client().prepareIndex("test", "parent", "p10").setSource("p_field", "p_value10").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setParent("p1").setSource("c_field", "blue").execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue")).cache(true)))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        client().prepareIndex("test", "child", "c2").setParent("p2").setSource("c_field", "blue").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue")).cache(true)))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }

    @Test
    public void testDeleteByQuery_has_child() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 1)
                                .put("index.refresh_interval", "-1")
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("child")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "parent", "p3").setSource("p_field", "p_value3").execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();
        client().prepareIndex("test", "child", "c5").setSource("c_field", "blue").setParent("p3").execute().actionGet();
        client().prepareIndex("test", "child", "c6").setSource("c_field", "red").setParent("p3").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        // p4 will not be found via search api, but will be deleted via delete_by_query api!
        client().prepareIndex("test", "parent", "p4").setSource("p_field", "p_value4").execute().actionGet();
        client().prepareIndex("test", "child", "c7").setSource("c_field", "blue").setParent("p4").execute().actionGet();
        client().prepareIndex("test", "child", "c8").setSource("c_field", "red").setParent("p4").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomHasChild("child", "c_field", "blue"))
                .execute().actionGet();
        assertHitCount(searchResponse, 2l);

        client().prepareDeleteByQuery("test").setQuery(randomHasChild("child", "c_field", "blue")).execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        searchResponse = client().prepareSearch("test")
                .setQuery(randomHasChild("child", "c_field", "blue"))
                .execute().actionGet();
        assertHitCount(searchResponse, 0l);
    }

    @Test
    public void testDeleteByQuery_has_child_SingleRefresh() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 1)
                                .put("index.refresh_interval", "-1")
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("child")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "parent", "p3").setSource("p_field", "p_value3").execute().actionGet();
        client().prepareIndex("test", "child", "c5").setSource("c_field", "blue").setParent("p3").execute().actionGet();
        client().prepareIndex("test", "child", "c6").setSource("c_field", "red").setParent("p3").execute().actionGet();
        client().prepareIndex("test", "parent", "p4").setSource("p_field", "p_value4").execute().actionGet();
        client().prepareIndex("test", "child", "c7").setSource("c_field", "blue").setParent("p4").execute().actionGet();
        client().prepareIndex("test", "child", "c8").setSource("c_field", "red").setParent("p4").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomHasChild("child", "c_field", "blue"))
                .execute().actionGet();
        assertHitCount(searchResponse, 3l);

        client().prepareDeleteByQuery("test").setQuery(randomHasChild("child", "c_field", "blue")).execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        searchResponse = client().prepareSearch("test")
                .setQuery(randomHasChild("child", "c_field", "blue"))
                .execute().actionGet();
        assertHitCount(searchResponse, 0l);
    }

    private QueryBuilder randomHasChild(String type, String field, String value) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return constantScoreQuery(hasChildFilter(type, termQuery(field, value)));
            } else {
                return filteredQuery(matchAllQuery(), hasChildFilter(type, termQuery(field, value)));
            }
        } else {
            return hasChildQuery(type, termQuery(field, value));
        }
    }

    @Test
    public void testDeleteByQuery_has_parent() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 1)
                                .put("index.refresh_interval", "-1")
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("child")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomHasParent("parent", "p_field", "p_value2"))
                .execute().actionGet();
        assertHitCount(searchResponse, 2l);

        client().prepareDeleteByQuery("test")
                .setQuery(randomHasParent("parent", "p_field", "p_value2"))
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        searchResponse = client().prepareSearch("test")
                .setQuery(randomHasParent("parent", "p_field", "p_value2"))
                .execute().actionGet();
        assertHitCount(searchResponse, 0l);
    }

    private QueryBuilder randomHasParent(String type, String field, String value) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return constantScoreQuery(hasParentFilter(type, termQuery(field, value)));
            } else {
                return filteredQuery(matchAllQuery(), hasParentFilter(type, termQuery(field, value)));
            }
        } else {
            return hasParentQuery(type, termQuery(field, value));
        }
    }

    @Test
    // Relates to bug: https://github.com/elasticsearch/elasticsearch/issues/3818
    public void testHasChildQueryOnlyReturnsSingleChildType() {
        client().admin().indices().prepareCreate("grandissue")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                )
                .addMapping("grandparent", "name", "type=string")
                .addMapping("parent", "_parent", "type=grandparent")
                .addMapping("child_type_one", "_parent", "type=parent")
                .addMapping("child_type_two", "_parent", "type=parent")
                .execute().actionGet();

        client().prepareIndex("grandissue", "grandparent", "1").setSource("name", "Grandpa").execute().actionGet();
        client().prepareIndex("grandissue", "parent", "2").setParent("1").setSource("name", "Dana").execute().actionGet();
        client().prepareIndex("grandissue", "child_type_one", "3").setParent("2").setRouting("1")
                .setSource("name", "William")
                .execute().actionGet();
        client().prepareIndex("grandissue", "child_type_two", "4").setParent("2").setRouting("1")
                .setSource("name", "Kate")
                .execute().actionGet();
        client().admin().indices().prepareRefresh("grandissue").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("grandissue").setQuery(
                boolQuery().must(
                        hasChildQuery(
                                "parent",
                                boolQuery().must(
                                        hasChildQuery(
                                                "child_type_one",
                                                boolQuery().must(
                                                        queryString("name:William*").analyzeWildcard(true)
                                                )
                                        )
                                )
                        )
                )
        ).execute().actionGet();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("grandissue").setQuery(
                boolQuery().must(
                        hasChildQuery(
                                "parent",
                                boolQuery().must(
                                        hasChildQuery(
                                                "child_type_two",
                                                boolQuery().must(
                                                        queryString("name:William*").analyzeWildcard(true)
                                                )
                                        )
                                )
                        )
                )
        ).execute().actionGet();
        assertHitCount(searchResponse, 0l);
    }

    @Test
    public void indexChildDocWithNoParentMapping() throws ElasticSearchException, IOException {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child1").setSource(
                jsonBuilder().startObject().startObject("child1").endObject().endObject()
        ).execute().actionGet();

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1", "_parent", "bla").execute().actionGet();
        try {
            client().prepareIndex("test", "child1", "c1").setParent("p1").setSource("c_field", "blue").execute().actionGet();
            fail();
        } catch (ElasticSearchIllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Can't specify parent if no parent field has been configured"));
        }
        try {
            client().prepareIndex("test", "child2", "c2").setParent("p1").setSource("c_field", "blue").execute().actionGet();
            fail();
        } catch (ElasticSearchIllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Can't specify parent if no parent field has been configured"));
        }

        refresh();
    }

    @Test
    public void testAddingParentToExistingMapping() throws ElasticSearchException, IOException {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("child").setSource("number", "type=integer")
                .execute().actionGet();
        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        try {
            // Adding _parent metadata field to existing mapping is prohibited:
            client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("child")
                    .startObject("_parent").field("type", "parent").endObject()
                    .endObject().endObject()).execute().actionGet();
            fail();
        } catch (MergeMappingException e) {
            assertThat(e.getMessage(), equalTo("Merge failed with failures {[The _parent field can't be added or updated]}"));
        }
    }

    @Test
    // The SimpleIdReaderTypeCache#docById method used lget, which can't be used if a map is shared.
    public void testTopChildrenBug_concurrencyIssue() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("child")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setParent("p1").setSource("c_field", "blue").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setParent("p1").setSource("c_field", "red").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setParent("p2").setSource("c_field", "red").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        int numThreads = 10;
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final AtomicReference<AssertionError> holder = new AtomicReference<AssertionError>();
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < 100; i++) {
                        SearchResponse searchResponse = client().prepareSearch("test")
                                .setQuery(topChildrenQuery("child", termQuery("c_field", "blue")))
                                .execute().actionGet();
                        assertNoFailures(searchResponse);
                        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

                        searchResponse = client().prepareSearch("test")
                                .setQuery(topChildrenQuery("child", termQuery("c_field", "red")))
                                .execute().actionGet();
                        assertNoFailures(searchResponse);
                        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
                    }
                } catch (AssertionError error) {
                    holder.set(error);
                } finally {
                    latch.countDown();
                }
            }
        };

        for (int i = 0; i < 10; i++) {
            new Thread(r).start();
        }
        latch.await();
        if (holder.get() != null) {
            throw holder.get();
        }
    }

    @Test
    public void testHasChildQueryWithNestedInnerObjects() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                )
                .addMapping("parent", "objects", "type=nested")
                .addMapping("child", "_parent", "type=parent")
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "parent", "p1")
                .setSource(jsonBuilder().startObject().field("p_field", "1").startArray("objects")
                        .startObject().field("i_field", "1").endObject()
                        .startObject().field("i_field", "2").endObject()
                        .startObject().field("i_field", "3").endObject()
                        .startObject().field("i_field", "4").endObject()
                        .startObject().field("i_field", "5").endObject()
                        .startObject().field("i_field", "6").endObject()
                        .endArray().endObject())
                .execute().actionGet();
        client().prepareIndex("test", "parent", "p2")
                .setSource(jsonBuilder().startObject().field("p_field", "2").startArray("objects")
                        .startObject().field("i_field", "1").endObject()
                        .startObject().field("i_field", "2").endObject()
                        .endArray().endObject())
                .execute().actionGet();
        client().prepareIndex("test", "child", "c1").setParent("p1").setSource("c_field", "blue").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setParent("p1").setSource("c_field", "red").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setParent("p2").setSource("c_field", "red").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        String scoreMode = ScoreType.values()[getRandom().nextInt(ScoreType.values().length)].name().toLowerCase(Locale.ROOT);
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(QueryBuilders.hasChildQuery("child", termQuery("c_field", "blue")).scoreType(scoreMode), notFilter(termFilter("p_field", "3"))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(QueryBuilders.hasChildQuery("child", termQuery("c_field", "red")).scoreType(scoreMode), notFilter(termFilter("p_field", "3"))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }

    private static HasChildFilterBuilder hasChildFilter(String type, QueryBuilder queryBuilder) {
        HasChildFilterBuilder hasChildFilterBuilder = FilterBuilders.hasChildFilter(type, queryBuilder);
        hasChildFilterBuilder.setShortCircuitCutoff(randomInt(10));
        return hasChildFilterBuilder;
    }

    private static HasChildFilterBuilder hasChildFilter(String type, FilterBuilder filterBuilder) {
        HasChildFilterBuilder hasChildFilterBuilder = FilterBuilders.hasChildFilter(type, filterBuilder);
        hasChildFilterBuilder.setShortCircuitCutoff(randomInt(10));
        return hasChildFilterBuilder;
    }

    private static HasChildQueryBuilder hasChildQuery(String type, QueryBuilder queryBuilder) {
        HasChildQueryBuilder hasChildQueryBuilder = QueryBuilders.hasChildQuery(type, queryBuilder);
        hasChildQueryBuilder.setShortCircuitCutoff(randomInt(10));
        return hasChildQueryBuilder;
    }

}
