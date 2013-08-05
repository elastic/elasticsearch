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

package org.elasticsearch.test.integration.search.child;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.facet.FacetBuilders.termsFacet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleChildQuerySearchTests extends AbstractSharedClusterTest {
    
    @Test
    public void multiLevelChild() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("grandchild").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "child").endObject()
                .endObject().endObject()).execute().actionGet();

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "c_value1").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "grandchild", "gc1").setSource("gc_field", "gc_value1").setParent("c1").setRouting("gc1").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(
                        filteredQuery(
                                matchAllQuery(),
                                hasChildFilter(
                                        "child",
                                        filteredQuery(termQuery("c_field", "c_value1"), hasChildFilter("grandchild", termQuery("gc_field", "gc_value1")))
                                )
                        )
                )
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(
                        filteredQuery(
                                matchAllQuery(),
                                hasParentFilter(
                                        "parent",
                                        termFilter("p_field", "p_value1")
                                )
                        )
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(
                        filteredQuery(
                                matchAllQuery(),
                                hasParentFilter(
                                        "child",
                                        termFilter("c_field", "c_value1")
                                )
                        )
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("gc1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(
                        hasParentQuery(
                                "parent",
                                termQuery("p_field", "p_value1")
                        )
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(
                        hasParentQuery(
                                "child",
                                termQuery("c_field", "c_value1")
                        )
                ).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("gc1"));
    }
    
    
    @Test // see #2744
    public void test2744() throws ElasticSearchException, IOException {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("test").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "foo").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "foo", "1").setSource("foo", 1).execute().actionGet();
        client().prepareIndex("test", "test").setSource("foo", 1).setParent("1").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        SearchResponse searchResponse =  client().prepareSearch("test").setQuery(hasChildQuery("test", matchQuery("foo", 1))).execute().actionGet();
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

    }

    @Test
    public void simpleChildQuery() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
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
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(idsQuery("child").ids("c1"))
                .addFields("_parent")
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));

        // TEST matching on parent
        searchResponse = client().prepareSearch("test")
                .setQuery(termQuery("child._parent", "p1"))
                .addFields("_parent")
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(1).field("_parent").value().toString(), equalTo("p1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(termQuery("_parent", "p1"))
                .addFields("_parent")
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(1).field("_parent").value().toString(), equalTo("p1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(queryString("_parent:p1"))
                .addFields("_parent")
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(1).field("_parent").value().toString(), equalTo("p1"));


        // TOP CHILDREN QUERY

        searchResponse = client().prepareSearch("test")
                .setQuery(topChildrenQuery("child", termQuery("c_field", "yellow")))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD QUERY

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
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

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue")))).execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS PARENT FILTER
        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", "p_value2")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c3"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c4"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", "p_value1")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c2"));

        // HAS PARENT QUERY
        searchResponse = client().prepareSearch("test").setQuery(hasParentQuery("parent", termQuery("p_field", "p_value2"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c3"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c4"));

        searchResponse = client().prepareSearch("test").setQuery(hasParentQuery("parent", termQuery("p_field", "p_value1"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c2"));


    }

    @Test
    // See: https://github.com/elasticsearch/elasticsearch/issues/3290
    public void testCachingBug_withFqueryFilter() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "parent", Integer.toString(i)).setSource("p_field", i).execute().actionGet();
        }
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "child", Integer.toString(i)).setSource("c_field", i).setParent("" + 0).execute().actionGet();
        }
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "child", Integer.toString(i + 10)).setSource("c_field", i + 10).setParent(Integer.toString(i)).execute().actionGet();
        }
        client().admin().indices().prepareFlush().execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(queryFilter(topChildrenQuery("child", matchAllQuery())).cache(true)))
                    .execute().actionGet();
            assertNoFailures(searchResponse);
            searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(queryFilter(hasChildQuery("child", matchAllQuery()).scoreType("max")).cache(true)))
                    .execute().actionGet();
            assertNoFailures(searchResponse);
            searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(queryFilter(hasParentQuery("parent", matchAllQuery()).scoreType("score")).cache(true)))
                    .execute().actionGet();
            assertNoFailures(searchResponse);
        }
    }

    @Test
    public void testHasParentFilter() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
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
            builders.add(client().prepareIndex("test", "child", childId)
                    .setSource("c_field", childId)
                    .setParent(previousParentId));

            if (!parentToChildren.containsKey(previousParentId)) {
                parentToChildren.put(previousParentId, new HashSet<String>());
            }
            assertThat(parentToChildren.get(previousParentId).add(childId), is(true));
        }
        indexRandom("test", true, builders.toArray(new IndexRequestBuilder[0]));

        assertThat(parentToChildren.isEmpty(), equalTo(false));
        for (Map.Entry<String, Set<String>> parentToChildrenEntry : parentToChildren.entrySet()) {
            SearchResponse searchResponse = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", parentToChildrenEntry.getKey()))))
                    .setSize(numChildDocsPerParent)
                    .execute().actionGet();

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
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
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

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD QUERY

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
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

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));
    }

    @Test
    public void simpleChildQueryWithFlushAnd3Shards() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 0)
        ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
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

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD QUERY

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "blue"))).execute().actionGet();
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

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));
    }

    @Test
    public void testScopedFacet() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(
                        topChildrenQuery("child", boolQuery().should(termQuery("c_field", "red")).should(termQuery("c_field", "yellow")))
                )
                .addFacet(
                        termsFacet("facet1")
                                .facetFilter(boolFilter().should(termFilter("c_field", "red")).should(termFilter("c_field", "yellow")))
                                .field("c_field")
                                .global(true)
                )
                .execute().actionGet();
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
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
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

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
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

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        // update p1 and see what that we get updated values...

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1_updated").execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1_updated\""));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1_updated\""));
    }

    @Test
    public void testDfsSearchType() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
        ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
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
                .setQuery(boolQuery().mustNot(hasChildQuery("child", boolQuery().should(queryString("c_field:*")))))
                .execute().actionGet();
        assertNoFailures(searchResponse);

        searchResponse = client().prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(hasParentQuery("parent", boolQuery().should(queryString("p_field:*")))))
                .execute().actionGet();
        assertNoFailures(searchResponse);

        searchResponse = client().prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(topChildrenQuery("child", boolQuery().should(queryString("c_field:*")))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
    }

    @Test
    public void testFixAOBEIfTopChildrenIsWrappedInMusNotClause() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
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
                .setQuery(boolQuery().mustNot(topChildrenQuery("child", boolQuery().should(queryString("c_field:*")))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
    }

    @Test
    public void testTopChildrenReSearchBug() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();


        int numberOfParents = 4;
        int numberOfChildrenPerParent = 123;
        for (int i = 1; i <= numberOfParents; i++) {
            String parentId = String.format(Locale.ROOT, "p%d", i);
            client().prepareIndex("test", "parent", parentId)
                    .setSource("p_field", String.format(Locale.ROOT, "p_value%d", i))
                    .execute()
                    .actionGet();
            for (int j = 1; j <= numberOfChildrenPerParent; j++) {
                client().prepareIndex("test", "child", String.format(Locale.ROOT, "%s_c%d", parentId, j))
                        .setSource(
                                "c_field1", parentId,
                                "c_field2", i % 2 == 0 ? "even" : "not_even"
                        ).setParent(parentId)
                        .execute()
                        .actionGet();
            }
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(topChildrenQuery("child", termQuery("c_field1", "p3")))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p3"));

        searchResponse = client().prepareSearch("test")
                .setQuery(topChildrenQuery("child", termQuery("c_field2", "even")))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));
    }

    @Test
    public void testHasChildAndHasParentFailWhenSomeSegmentsDontContainAnyParentOrChildDocs() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
        ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(
                jsonBuilder()
                        .startObject()
                        .startObject("type")
                        .startObject("_parent")
                        .field("type", "parent")
                        .endObject()
                        .endObject()
                        .endObject()
        ).execute().actionGet();

        client().prepareIndex("test", "parent", "1").setSource("p_field", 1).execute().actionGet();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("c_field", 1).execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource("p_field", "p_value1").execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasChildFilter("child", matchAllQuery())))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasParentFilter("parent", matchAllQuery())))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testCountApiUsage() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId)
                .setSource("p_field", "p_value1")
                .execute().actionGet();
        client().prepareIndex("test", "child", parentId + "_c1")
                .setSource("c_field1", parentId)
                .setParent(parentId)
                .execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        CountResponse countResponse = client().prepareCount("test")
                .setQuery(topChildrenQuery("child", termQuery("c_field1", "1")))
                .execute().actionGet();
        assertThat(countResponse.getFailedShards(), equalTo(1));
        assertThat(countResponse.getShardFailures()[0].reason().contains("top_children query hasn't executed properly"), equalTo(true));

        countResponse = client().prepareCount("test")
                .setQuery(hasChildQuery("child", termQuery("c_field1", "2")).scoreType("max"))
                .execute().actionGet();
        assertThat(countResponse.getFailedShards(), equalTo(1));
        assertThat(countResponse.getShardFailures()[0].reason().contains("has_child query hasn't executed properly"), equalTo(true));

        countResponse = client().prepareCount("test")
                .setQuery(hasParentQuery("parent", termQuery("p_field1", "1")).scoreType("score"))
                .execute().actionGet();
        assertThat(countResponse.getFailedShards(), equalTo(1));
        assertThat(countResponse.getShardFailures()[0].reason().contains("has_parent query hasn't executed properly"), equalTo(true));

        countResponse = client().prepareCount("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field1", "2"))))
                .execute().actionGet();
        assertThat(countResponse.getFailedShards(), equalTo(1));
        assertThat(countResponse.getShardFailures()[0].reason().contains("has_child filter hasn't executed properly"), equalTo(true));

        countResponse = client().prepareCount("test")
                .setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field1", "1"))))
                .execute().actionGet();
        assertThat(countResponse.getFailedShards(), equalTo(1));
        assertThat(countResponse.getShardFailures()[0].reason().contains("has_parent filter hasn't executed properly"), equalTo(true));
    }

    @Test
    public void testScoreForParentChildQueries() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .addMapping("child", jsonBuilder()
                        .startObject()
                        .startObject("type")
                        .startObject("_parent")
                        .field("type", "parent")
                        .endObject()
                        .endObject()
                        .endObject()
                ).addMapping("child1", jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("_parent")
                .field("type", "parent")
                .endObject()
                .endObject()
                .endObject()
        ).setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
        ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        // Parent 1 and its children
        client().prepareIndex("test", "parent", "1")
                .setSource("p_field", "p_value1")
                .execute().actionGet();
        client().prepareIndex("test", "child", "1")
                .setSource("c_field1", 1, "c_field2", 0)
                .setParent("1").execute().actionGet();
        client().prepareIndex("test", "child", "2")
                .setSource("c_field1", 1, "c_field2", 0)
                .setParent("1").execute().actionGet();
        client().prepareIndex("test", "child", "3")
                .setSource("c_field1", 2, "c_field2", 0)
                .setParent("1").execute().actionGet();
        client().prepareIndex("test", "child", "4")
                .setSource("c_field1", 2, "c_field2", 0)
                .setParent("1").execute().actionGet();
        client().prepareIndex("test", "child", "5")
                .setSource("c_field1", 1, "c_field2", 1)
                .setParent("1").execute().actionGet();
        client().prepareIndex("test", "child", "6")
                .setSource("c_field1", 1, "c_field2", 2)
                .setParent("1").execute().actionGet();

        // Parent 2 and its children
        client().prepareIndex("test", "parent", "2")
                .setSource("p_field", "p_value2")
                .execute().actionGet();
        client().prepareIndex("test", "child", "7")
                .setSource("c_field1", 3, "c_field2", 0)
                .setParent("2").execute().actionGet();
        client().prepareIndex("test", "child", "8")
                .setSource("c_field1", 1, "c_field2", 1)
                .setParent("2").execute().actionGet();
        client().prepareIndex("test", "child", "9")
                .setSource("c_field1", 1, "c_field2", 1)
                .setParent("p").execute().actionGet();
        client().prepareIndex("test", "child", "10")
                .setSource("c_field1", 1, "c_field2", 1)
                .setParent("2").execute().actionGet();
        client().prepareIndex("test", "child", "11")
                .setSource("c_field1", 1, "c_field2", 1)
                .setParent("2").execute().actionGet();
        client().prepareIndex("test", "child", "12")
                .setSource("c_field1", 1, "c_field2", 2)
                .setParent("2").execute().actionGet();

        // Parent 3 and its children
        client().prepareIndex("test", "parent", "3")
                .setSource("p_field1", "p_value3", "p_field2", 5)
                .execute().actionGet();
        client().prepareIndex("test", "child", "13")
                .setSource("c_field1", 4, "c_field2", 0, "c_field3", 0)
                .setParent("3").execute().actionGet();
        client().prepareIndex("test", "child", "14")
                .setSource("c_field1", 1, "c_field2", 1, "c_field3", 1)
                .setParent("3").execute().actionGet();
        client().prepareIndex("test", "child", "15")
                .setSource("c_field1", 1, "c_field2", 2, "c_field3", 2)
                .setParent("3").execute().actionGet();
        client().prepareIndex("test", "child", "16")
                .setSource("c_field1", 1, "c_field2", 2, "c_field3", 3)
                .setParent("3").execute().actionGet();
        client().prepareIndex("test", "child", "17")
                .setSource("c_field1", 1, "c_field2", 2, "c_field3", 4)
                .setParent("3").execute().actionGet();
        client().prepareIndex("test", "child", "18")
                .setSource("c_field1", 1, "c_field2", 2, "c_field3", 5)
                .setParent("3").execute().actionGet();
        client().prepareIndex("test", "child1", "1")
                .setSource("c_field1", 1, "c_field2", 2, "c_field3", 6)
                .setParent("3").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasChildQuery(
                                "child",
                                QueryBuilders.customScoreQuery(
                                        matchQuery("c_field2", 0)
                                ).script("doc['c_field1'].value")
                        ).scoreType("sum")
                )
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("1"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(4f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(3f));

        response = client().prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasChildQuery(
                                "child",
                                QueryBuilders.customScoreQuery(
                                        matchQuery("c_field2", 0)
                                ).script("doc['c_field1'].value")
                        ).scoreType("max")
                )
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(4f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("1"));
        assertThat(response.getHits().hits()[2].score(), equalTo(2f));

        response = client().prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasChildQuery(
                                "child",
                                QueryBuilders.customScoreQuery(
                                        matchQuery("c_field2", 0)
                                ).script("doc['c_field1'].value")
                        ).scoreType("avg")
                )
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(4f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("1"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1.5f));

        response = client().prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasParentQuery(
                                "parent",
                                QueryBuilders.customScoreQuery(
                                        matchQuery("p_field1", "p_value3")
                                ).script("doc['p_field2'].value")
                        ).scoreType("score")
                )
                .addSort(SortBuilders.fieldSort("c_field3"))
                .addSort(SortBuilders.scoreSort())
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

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/2536
    public void testParentChildQueriesCanHandleNoRelevantTypesInIndex() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .addMapping("parent", jsonBuilder()
                        .startObject()
                        .startObject("parent")
                        .endObject()
                        .endObject()
                ).addMapping("child", jsonBuilder()
                .startObject()
                .startObject("child")
                .startObject("_parent")
                .field("type", "parent")
                .endObject()
                .endObject()
                .endObject()
        ).setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
        ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value")))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(0l));

        client().prepareIndex("test", "child1").setSource(jsonBuilder().startObject().field("text", "value").endObject())
                .setRefresh(true)
                .execute().actionGet();

        client().prepareSearch("test")
                .setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value")))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(0l));

        client().prepareSearch("test")
                .setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value")).scoreType("max"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(0l));

        client().prepareSearch("test")
                .setQuery(QueryBuilders.hasParentQuery("child", matchQuery("text", "value")))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(0l));

        client().prepareSearch("test")
                .setQuery(QueryBuilders.hasParentQuery("child", matchQuery("text", "value")).scoreType("score"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(0l));
    }

    @Test
    public void testHasChildAndHasParentFilter_withFilter() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
        ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(
                jsonBuilder()
                        .startObject()
                        .startObject("type")
                        .startObject("_parent")
                        .field("type", "parent")
                        .endObject()
                        .endObject()
                        .endObject()
        ).execute().actionGet();

        client().prepareIndex("test", "parent", "1").setSource("p_field", 1).execute().actionGet();
        client().prepareIndex("test", "child", "2").setParent("1").setSource("c_field", 1).execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource("p_field", "p_value1").execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasChildFilter("child", termFilter("c_field", 1))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasParentFilter("parent", termFilter("p_field", 1))))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("2"));
    }

    @Test
    public void testSimpleQueryRewrite() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
        ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        int childId = 0;
        for (int i = 0; i < 10; i++) {
            String parentId = String.format(Locale.ROOT, "p%03d", i);
            client().prepareIndex("test", "parent", parentId)
                    .setSource("p_field", parentId)
                    .execute().actionGet();
            int j = childId;
            for (; j < childId + 50; j++) {
                String childUid = String.format(Locale.ROOT, "c%03d", j);
                client().prepareIndex("test", "child", childUid)
                        .setSource("c_field", childUid)
                        .setParent(parentId)
                        .execute().actionGet();
            }
            childId = j;
        }
        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchType[] searchTypes = new SearchType[]{SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH};
        for (SearchType searchType : searchTypes) {
            SearchResponse searchResponse = client().prepareSearch("test").setSearchType(searchType)
                    .setQuery(hasChildQuery("child", prefixQuery("c_field", "c")).scoreType("max"))
                    .addSort("p_field", SortOrder.ASC)
                    .setSize(5)
                    .execute().actionGet();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(10L));
            assertThat(searchResponse.getHits().hits()[0].id(), equalTo("p000"));
            assertThat(searchResponse.getHits().hits()[1].id(), equalTo("p001"));
            assertThat(searchResponse.getHits().hits()[2].id(), equalTo("p002"));
            assertThat(searchResponse.getHits().hits()[3].id(), equalTo("p003"));
            assertThat(searchResponse.getHits().hits()[4].id(), equalTo("p004"));

            searchResponse = client().prepareSearch("test").setSearchType(searchType)
                    .setQuery(hasParentQuery("parent", prefixQuery("p_field", "p")).scoreType("score"))
                    .addSort("c_field", SortOrder.ASC)
                    .setSize(5)
                    .execute().actionGet();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(500L));
            assertThat(searchResponse.getHits().hits()[0].id(), equalTo("c000"));
            assertThat(searchResponse.getHits().hits()[1].id(), equalTo("c001"));
            assertThat(searchResponse.getHits().hits()[2].id(), equalTo("c002"));
            assertThat(searchResponse.getHits().hits()[3].id(), equalTo("c003"));
            assertThat(searchResponse.getHits().hits()[4].id(), equalTo("c004"));

            searchResponse = client().prepareSearch("test").setSearchType(searchType)
                    .setQuery(topChildrenQuery("child", prefixQuery("c_field", "c")))
                    .addSort("p_field", SortOrder.ASC)
                    .setSize(5)
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
    // See also issue: https://github.com/elasticsearch/elasticsearch/issues/3144
    public void testReIndexingParentAndChildDocuments() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
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
                .setQuery(hasChildQuery("child", termQuery("c_field", "yellow")).scoreType("sum"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client().prepareSearch("test")
                .setQuery(
                        boolQuery()
                                .must(matchQuery("c_field", "x"))
                                .must(hasParentQuery("parent", termQuery("p_field", "p_value2")).scoreType("score"))
                )
                .execute().actionGet();
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

        searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("c_field", "yellow")).scoreType("sum"))
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client().prepareSearch("test")
                .setQuery(
                        boolQuery()
                                .must(matchQuery("c_field", "x"))
                                .must(hasParentQuery("parent", termQuery("p_field", "p_value2")).scoreType("score"))
                )
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), Matchers.anyOf(equalTo("c3"), equalTo("c4")));
        assertThat(searchResponse.getHits().getAt(1).id(), Matchers.anyOf(equalTo("c3"), equalTo("c4")));
    }

    @Test
    // See also issue: https://github.com/elasticsearch/elasticsearch/issues/3203
    public void testHasChildQueryWithMinimumScore() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();

        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("type")
                .startObject("_parent").field("type", "parent").endObject()
                .endObject().endObject()).execute().actionGet();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").execute().actionGet();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "x").setParent("p1").execute().actionGet();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").execute().actionGet();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "x").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "x").setParent("p2").execute().actionGet();
        client().prepareIndex("test", "child", "c5").setSource("c_field", "x").setParent("p2").execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery()).scoreType("sum"))
                .setMinScore(2) // Score needs to be above 2.0!
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(3.0f));
    }

}
