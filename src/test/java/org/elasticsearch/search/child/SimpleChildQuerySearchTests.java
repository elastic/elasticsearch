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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.FieldMapper.Loading;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.search.child.ScoreType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
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
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.settings.ImmutableSettings.builder;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.factorFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleChildQuerySearchTests extends ElasticsearchIntegrationTest {

    @Test
    public void multiLevelChild() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent")
                .addMapping("grandchild", "_parent", "type=child"));
        ensureGreen();

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "c_value1").setParent("p1").get();
        client().prepareIndex("test", "grandchild", "gc1").setSource("gc_field", "gc_value1")
                .setParent("c1").setRouting("p1").get();
        refresh();

        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        filteredQuery(
                                matchAllQuery(),
                                hasChildFilter(
                                        "child",
                                        filteredQuery(termQuery("c_field", "c_value1"),
                                                hasChildFilter("grandchild", termQuery("gc_field", "gc_value1")))))).get();
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
    // see #6722
    public void test6722() throws ElasticsearchException, IOException {
        assertAcked(prepareCreate("test")
                .addMapping("foo")
                .addMapping("test", "_parent", "type=foo"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "foo", "1").setSource("foo", 1).get();
        client().prepareIndex("test", "test", "2").setSource("foo", 1).setParent("1").get();
        refresh();
        String query = copyToStringFromClasspath("/org/elasticsearch/search/child/bool-query-with-empty-clauses.json");
        SearchResponse searchResponse = client().prepareSearch("test").setSource(query).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
    }

    @Test
    // see #2744
    public void test2744() throws ElasticsearchException, IOException {
        assertAcked(prepareCreate("test")
                .addMapping("foo")
                .addMapping("test", "_parent", "type=foo"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "foo", "1").setSource("foo", 1).get();
        client().prepareIndex("test", "test").setSource("foo", 1).setParent("1").get();
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("test", matchQuery("foo", 1))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

    }

    @Test
    public void simpleChildQuery() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();
        refresh();

        // TEST FETCHING _parent from child
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(idsQuery("child").ids("c1")).addFields("_parent").execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));

        // TEST matching on parent
        searchResponse = client().prepareSearch("test").setQuery(termQuery("child._parent", "p1")).addFields("_parent").execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(1).field("_parent").value().toString(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(termQuery("_parent", "p1")).addFields("_parent").get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(1).field("_parent").value().toString(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(queryString("_parent:p1")).addFields("_parent").get();
        assertNoFailures(searchResponse);
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
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute()
                .actionGet();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD
        searchResponse = client().prepareSearch("test").setQuery(randomHasChild("child", "c_field", "yellow"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(randomHasChild("child", "c_field", "blue")).execute()
                .actionGet();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(randomHasChild("child", "c_field", "red")).get();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS PARENT
        searchResponse = client().prepareSearch("test")
                .setQuery(randomHasParent("parent", "p_field", "p_value2")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c3"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c4"));

        searchResponse = client().prepareSearch("test")
                .setQuery(randomHasParent("parent", "p_field", "p_value1")).get();
        assertHitCount(searchResponse, 2l);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c2"));
    }

    @Test
    public void testClearIdCacheBug() throws Exception {
        // enforce lazy loading to make sure that p/c stats are not counted as part of field data
        assertAcked(prepareCreate("test")
                .setSettings(ImmutableSettings.builder().put(indexSettings())
                        .put("index.refresh_interval", -1)) // Disable automatic refresh, so that the _parent doesn't get warmed
                .addMapping("parent", XContentFactory.jsonBuilder().startObject().startObject("parent")
                        .startObject("properties")
                            .startObject("p_field")
                                .field("type", "string")
                                .startObject("fielddata")
                                    .field(FieldDataType.FORMAT_KEY, Loading.LAZY)
                                .endObject()
                            .endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();

        client().prepareIndex("test", "parent", "p0").setSource("p_field", "p_value0").get();
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();

        refresh();
        // No _parent field yet, there shouldn't be anything in the parent id cache
        IndicesStatsResponse indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setIdCache(true).get();
        assertThat(indicesStatsResponse.getTotal().getIdCache().getMemorySizeInBytes(), equalTo(0l));

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
                                    .field(FieldDataType.FORMAT_KEY, Loading.LAZY)
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
                .prepareStats("test").setFieldData(true).get();
        // automatic warm-up has populated the cache since it found a parent field mapper
        assertThat(indicesStatsResponse.getTotal().getIdCache().getMemorySizeInBytes(), greaterThan(0l));
        // Even though p/c is field data based the stats stay zero, because _parent field data field is kept
        // track of under id cache stats memory wise for bwc
        assertThat(indicesStatsResponse.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0l));

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setFieldData(true).get();
        assertThat(indicesStatsResponse.getTotal().getIdCache().getMemorySizeInBytes(), greaterThan(0l));
        assertThat(indicesStatsResponse.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0l));

        ClearIndicesCacheResponse clearCacheResponse = client().admin().indices().prepareClearCache("test").setIdCache(true).get();
        assertNoFailures(clearCacheResponse);
        assertAllSuccessful(clearCacheResponse);
        indicesStatsResponse = client().admin().indices()
                .prepareStats("test").setFieldData(true).get();
        assertThat(indicesStatsResponse.getTotal().getIdCache().getMemorySizeInBytes(), equalTo(0l));
        assertThat(indicesStatsResponse.getTotal().getFieldData().getMemorySizeInBytes(), equalTo(0l));
    }

    @Test
    public void testCheckFixedBitSetCache() throws Exception {
        boolean loadFixedBitSetLazily = randomBoolean();
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder().put(indexSettings())
                .put("index.refresh_interval", -1);
        if (loadFixedBitSetLazily) {
            settingsBuilder.put("index.load_fixed_bitset_filters_eagerly", false);
        }
        // enforce lazy loading to make sure that p/c stats are not counted as part of field data
        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder)
                .addMapping("parent")
        );

        client().prepareIndex("test", "parent", "p0").setSource("p_field", "p_value0").get();
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        refresh();
        ensureSearchable("test");

        // No _parent field yet, there shouldn't be anything in the parent id cache
        ClusterStatsResponse clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsResponse.getIndicesStats().getSegments().getFixedBitSetMemoryInBytes(), equalTo(0l));

        // Now add mapping + children
        assertAcked(
                client().admin().indices().preparePutMapping("test").setType("child").setSource("_parent", "type=parent")
        );

        // index simple data
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();
        refresh();
        ensureSearchable("test");

        if (loadFixedBitSetLazily) {
            clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
            assertThat(clusterStatsResponse.getIndicesStats().getSegments().getFixedBitSetMemoryInBytes(), equalTo(0l));

            // only when querying with has_child the fixed bitsets are loaded
            SearchResponse searchResponse = client().prepareSearch("test")
                    // Use setShortCircuitCutoff(0), otherwise the parent filter isn't used.
                    .setQuery(hasChildQuery("child", termQuery("c_field", "blue")).setShortCircuitCutoff(0))
                    .get();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        }
        clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsResponse.getIndicesStats().getSegments().getFixedBitSetMemoryInBytes(), greaterThan(0l));

        assertAcked(client().admin().indices().prepareDelete("test"));
        clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsResponse.getIndicesStats().getSegments().getFixedBitSetMemoryInBytes(), equalTo(0l));
    }

    @Test
    // See: https://github.com/elasticsearch/elasticsearch/issues/3290
    public void testCachingBug_withFqueryFilter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        // index simple data
        for (int i = 0; i < 10; i++) {
            builders.add(client().prepareIndex("test", "parent", Integer.toString(i)).setSource("p_field", i));
        }
        indexRandom(randomBoolean(), builders);
        builders.clear();
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                builders.add(client().prepareIndex("test", "child", Integer.toString(i)).setSource("c_field", i).setParent("" + 0));
            }
            for (int i = 0; i < 10; i++) {
                builders.add(client().prepareIndex("test", "child", Integer.toString(i + 10)).setSource("c_field", i + 10).setParent(Integer.toString(i)));
            }

            if (randomBoolean()) {
                break; // randomly break out and dont' have deletes / updates
            }
        }
        indexRandom(true, builders);

        for (int i = 1; i <= 10; i++) {
            logger.info("Round {}", i);
            SearchResponse searchResponse = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(queryFilter(topChildrenQuery("child", matchAllQuery())).cache(true))).execute()
                    .actionGet();
            assertNoFailures(searchResponse);
            searchResponse = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(queryFilter(hasChildQuery("child", matchAllQuery()).scoreType("max")).cache(true)))
                    .get();
            assertNoFailures(searchResponse);
            searchResponse = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(queryFilter(hasParentQuery("parent", matchAllQuery()).scoreType("score")).cache(true)))
                    .get();
            assertNoFailures(searchResponse);
        }
    }

    @Test
    public void testHasParentFilter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();
        Map<String, Set<String>> parentToChildren = newHashMap();
        // Childless parent
        client().prepareIndex("test", "parent", "p0").setSource("p_field", "p0").get();
        parentToChildren.put("p0", new HashSet<String>());

        String previousParentId = null;
        int numChildDocs = 32;
        int numChildDocsPerParent = 0;
        List<IndexRequestBuilder> builders = new ArrayList<>();
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
        indexRandom(true, builders.toArray(new IndexRequestBuilder[builders.size()]));

        assertThat(parentToChildren.isEmpty(), equalTo(false));
        for (Map.Entry<String, Set<String>> parentToChildrenEntry : parentToChildren.entrySet()) {
            SearchResponse searchResponse = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", parentToChildrenEntry.getKey()))))
                    .setSize(numChildDocsPerParent).get();

            assertNoFailures(searchResponse);
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
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data with flushes, so we have many segments
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().admin().indices().prepareFlush().get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().admin().indices().prepareFlush().get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().admin().indices().prepareFlush().get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().admin().indices().prepareFlush().get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().admin().indices().prepareFlush().get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();
        client().admin().indices().prepareFlush().get();
        refresh();

        // TOP CHILDREN QUERY

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow")))
                .get();
        assertNoFailures(searchResponse);
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "blue"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "red"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD QUERY

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "blue"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "red"))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        // HAS CHILD FILTER

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));
    }

    @Test
    public void testScopedFacet() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();

        refresh();

        SearchResponse searchResponse = client()
                .prepareSearch("test")
                .setQuery(topChildrenQuery("child", boolQuery().should(termQuery("c_field", "red")).should(termQuery("c_field", "yellow"))))
                .addAggregation(AggregationBuilders.global("global").subAggregation(
                        AggregationBuilders.filter("filter").filter(boolFilter().should(termFilter("c_field", "red")).should(termFilter("c_field", "yellow"))).subAggregation(
                                AggregationBuilders.terms("facet1").field("c_field")))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        Global global = searchResponse.getAggregations().get("global");
        Filter filter = global.getAggregations().get("filter");
        Terms termsFacet = filter.getAggregations().get("facet1");
        assertThat(termsFacet.getBuckets().size(), equalTo(2));
        assertThat(termsFacet.getBuckets().get(0).getKey(), equalTo("red"));
        assertThat(termsFacet.getBuckets().get(0).getDocCount(), equalTo(2L));
        assertThat(termsFacet.getBuckets().get(1).getKey(), equalTo("yellow"));
        assertThat(termsFacet.getBuckets().get(1).getDocCount(), equalTo(1L));
    }

    @Test
    public void testDeletedParent() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();
        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();

        refresh();

        // TOP CHILDREN QUERY

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow")))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        // update p1 and see what that we get updated values...

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1_updated").get();
        client().admin().indices().prepareRefresh().get();

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "yellow"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1_updated\""));

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow")))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1_updated\""));
    }

    @Test
    public void testDfsSearchType() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(hasChildQuery("child", boolQuery().should(queryString("c_field:*"))))).get();
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
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(topChildrenQuery("child", boolQuery().should(queryString("c_field:*"))))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
    }

    @Test
    public void testTopChildrenReSearchBug() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();
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

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field1", "p3")))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p3"));

        searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field2", "even"))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p4")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p4")));
    }

    @Test
    public void testHasChildAndHasParentFailWhenSomeSegmentsDontContainAnyParentOrChildDocs() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("p_field", 1).get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("c_field", 1).get();
        client().admin().indices().prepareFlush("test").get();

        client().prepareIndex("test", "type1", "1").setSource("p_field", 1).get();
        client().admin().indices().prepareFlush("test").get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasChildFilter("child", matchAllQuery()))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasParentFilter("parent", matchAllQuery()))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testCountApiUsage() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId).setSource("p_field", "1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "1").setParent(parentId).get();
        refresh();

        CountResponse countResponse = client().prepareCount("test").setQuery(topChildrenQuery("child", termQuery("c_field", "1")))
                .get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreType("max"))
                .get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(hasParentQuery("parent", termQuery("p_field", "1")).scoreType("score"))
                .get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "1"))))
                .get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareCount("test").setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", "1"))))
                .get();
        assertHitCount(countResponse, 1l);
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
                .setQuery(topChildrenQuery("child", termQuery("c_field", "1")))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).explanation().getDescription(), equalTo("not implemented yet..."));

        searchResponse = client().prepareSearch("test")
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
        assertThat(explainResponse.getExplanation().getDescription(), equalTo("not implemented yet..."));
    }

    List<IndexRequestBuilder> createDocBuilders() {
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
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
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent")
                .addMapping("child1", "_parent", "type=parent"));
        ensureGreen();

        indexRandom(true, createDocBuilders().toArray(new IndexRequestBuilder[0]));
        SearchResponse response = client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.hasChildQuery(
                                "child",
                                QueryBuilders.functionScoreQuery(matchQuery("c_field2", 0), scriptFunction("doc['c_field1'].value"))
                                        .boostMode(CombineFunction.REPLACE.getName())).scoreType("sum")).get();

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
                                        .boostMode(CombineFunction.REPLACE.getName())).scoreType("max")).get();

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
                                        .boostMode(CombineFunction.REPLACE.getName())).scoreType("avg")).get();

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
                .addSort(SortBuilders.fieldSort("c_field3")).addSort(SortBuilders.scoreSort()).get();

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
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value"))).get();
        assertNoFailures(response);
        assertThat(response.getHits().totalHits(), equalTo(0l));

        client().prepareIndex("test", "child1").setSource(jsonBuilder().startObject().field("text", "value").endObject()).setRefresh(true)
                .get();

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value"))).get();
        assertNoFailures(response);
        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value")).scoreType("max"))
                .get();
        assertNoFailures(response);
        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasParentQuery("child", matchQuery("text", "value"))).get();
        assertNoFailures(response);
        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasParentQuery("child", matchQuery("text", "value")).scoreType("score"))
                .get();
        assertNoFailures(response);
        assertThat(response.getHits().totalHits(), equalTo(0l));
    }

    @Test
    public void testHasChildAndHasParentFilter_withFilter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("p_field", 1).get();
        client().prepareIndex("test", "child", "2").setParent("1").setSource("c_field", 1).get();
        client().admin().indices().prepareFlush("test").get();

        client().prepareIndex("test", "type1", "3").setSource("p_field", "p_value1").get();
        client().admin().indices().prepareFlush("test").get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasChildFilter("child", termFilter("c_field", 1)))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), hasParentFilter("parent", termFilter("p_field", 1)))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("2"));
    }

    @Test
    public void testHasChildAndHasParentWrappedInAQueryFilter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // query filter in case for p/c shouldn't execute per segment, but rather
        client().prepareIndex("test", "parent", "1").setSource("p_field", 1).get();
        client().admin().indices().prepareFlush("test").setForce(true).get();
        client().prepareIndex("test", "child", "2").setParent("1").setSource("c_field", 1).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), queryFilter(hasChildQuery("child", matchQuery("c_field", 1))))).get();
        assertSearchHit(searchResponse, 1, hasId("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), queryFilter(topChildrenQuery("child", matchQuery("c_field", 1))))).get();
        assertSearchHit(searchResponse, 1, hasId("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), queryFilter(hasParentQuery("parent", matchQuery("p_field", 1))))).get();
        assertSearchHit(searchResponse, 1, hasId("2"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), queryFilter(boolQuery().must(hasChildQuery("child", matchQuery("c_field", 1)))))).get();
        assertSearchHit(searchResponse, 1, hasId("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), queryFilter(boolQuery().must(topChildrenQuery("child", matchQuery("c_field", 1)))))).get();
        assertSearchHit(searchResponse, 1, hasId("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(matchAllQuery(), queryFilter(boolQuery().must(hasParentQuery("parent", matchQuery("p_field", 1)))))).get();
        assertSearchHit(searchResponse, 1, hasId("2"));
    }

    @Test
    public void testHasChildAndHasParentWrappedInAQueryFilterShouldNeverGetCached() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(ImmutableSettings.builder().put("index.cache.filter.type", "weighted"))
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("p_field", 1).get();
        client().prepareIndex("test", "child", "2").setParent("1").setSource("c_field", 1).get();
        refresh();

        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch("test")
                    .setExplain(true)
                    .setQuery(constantScoreQuery(boolFilter()
                                    .must(queryFilter(hasChildQuery("child", matchQuery("c_field", 1))))
                                    .cache(true)
                    )).get();
            assertSearchHit(searchResponse, 1, hasId("1"));
            // Can't start with ConstantScore(cache(BooleanFilter(
            assertThat(searchResponse.getHits().getAt(0).explanation().getDescription(), startsWith("ConstantScore(BooleanFilter("));

            searchResponse = client().prepareSearch("test")
                    .setExplain(true)
                    .setQuery(constantScoreQuery(boolFilter()
                                    .must(queryFilter(boolQuery().must(matchAllQuery()).must(hasChildQuery("child", matchQuery("c_field", 1)))))
                                    .cache(true)
                    )).get();
            assertSearchHit(searchResponse, 1, hasId("1"));
            // Can't start with ConstantScore(cache(BooleanFilter(
            assertThat(searchResponse.getHits().getAt(0).explanation().getDescription(), startsWith("ConstantScore(BooleanFilter("));
        }
    }

    @Test
    public void testSimpleQueryRewrite() throws Exception {
        assertAcked(prepareCreate("test")
                //top_children query needs at least 2 shards for the totalHits to be accurate
                .setSettings(settingsBuilder()
                        .put(indexSettings())
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, between(2, DEFAULT_MAX_NUM_SHARDS)))
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        int childId = 0;
        for (int i = 0; i < 10; i++) {
            String parentId = String.format(Locale.ROOT, "p%03d", i);
            client().prepareIndex("test", "parent", parentId).setSource("p_field", parentId).get();
            int j = childId;
            for (; j < childId + 50; j++) {
                String childUid = String.format(Locale.ROOT, "c%03d", j);
                client().prepareIndex("test", "child", childUid).setSource("c_field", childUid).setParent(parentId).get();
            }
            childId = j;
        }
        refresh();

        SearchType[] searchTypes = new SearchType[]{SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH};
        for (SearchType searchType : searchTypes) {
            SearchResponse searchResponse = client().prepareSearch("test").setSearchType(searchType)
                    .setQuery(hasChildQuery("child", prefixQuery("c_field", "c")).scoreType("max")).addSort("p_field", SortOrder.ASC)
                    .setSize(5).get();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(10L));
            assertThat(searchResponse.getHits().hits()[0].id(), equalTo("p000"));
            assertThat(searchResponse.getHits().hits()[1].id(), equalTo("p001"));
            assertThat(searchResponse.getHits().hits()[2].id(), equalTo("p002"));
            assertThat(searchResponse.getHits().hits()[3].id(), equalTo("p003"));
            assertThat(searchResponse.getHits().hits()[4].id(), equalTo("p004"));

            searchResponse = client().prepareSearch("test").setSearchType(searchType)
                    .setQuery(hasParentQuery("parent", prefixQuery("p_field", "p")).scoreType("score")).addSort("c_field", SortOrder.ASC)
                    .setSize(5).get();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(500L));
            assertThat(searchResponse.getHits().hits()[0].id(), equalTo("c000"));
            assertThat(searchResponse.getHits().hits()[1].id(), equalTo("c001"));
            assertThat(searchResponse.getHits().hits()[2].id(), equalTo("c002"));
            assertThat(searchResponse.getHits().hits()[3].id(), equalTo("c003"));
            assertThat(searchResponse.getHits().hits()[4].id(), equalTo("c004"));

            searchResponse = client().prepareSearch("test").setSearchType(searchType)
                    .setQuery(topChildrenQuery("child", prefixQuery("c_field", "c")).factor(10)).addSort("p_field", SortOrder.ASC).setSize(5)
                    .get();
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
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "x").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "x").setParent("p2").get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("c_field", "yellow")).scoreType("sum")).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        boolQuery().must(matchQuery("c_field", "x")).must(
                                hasParentQuery("parent", termQuery("p_field", "p_value2")).scoreType("score"))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c3"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c4"));

        // re-index
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
            client().prepareIndex("test", "child", "d" + i).setSource("c_field", "red").setParent("p1").get();
            client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
            client().prepareIndex("test", "child", "c3").setSource("c_field", "x").setParent("p2").get();
            client().admin().indices().prepareRefresh("test").get();
        }

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow")).scoreType("sum"))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        boolQuery().must(matchQuery("c_field", "x")).must(
                                hasParentQuery("parent", termQuery("p_field", "p_value2")).scoreType("score"))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), Matchers.anyOf(equalTo("c3"), equalTo("c4")));
        assertThat(searchResponse.getHits().getAt(1).id(), Matchers.anyOf(equalTo("c3"), equalTo("c4")));
    }

    @Test
    // See also issue:
    // https://github.com/elasticsearch/elasticsearch/issues/3203
    public void testHasChildQueryWithMinimumScore() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "x").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "x").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "x").setParent("p2").get();
        client().prepareIndex("test", "child", "c5").setSource("c_field", "x").setParent("p2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", matchAllQuery()).scoreType("sum"))
                .setMinScore(3) // Score needs to be 3 or above!
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(3.0f));
    }

    @Test
    public void testParentFieldFilter() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder().put(indexSettings())
                        .put("index.refresh_interval", -1))
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent")
                .addMapping("child2", "_parent", "type=parent"));
        ensureGreen();

        // test term filter
        SearchResponse response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("_parent", "p1")))
                .get();
        assertHitCount(response, 0l);

        client().prepareIndex("test", "some_type", "1").setSource("field", "value").get();
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "value").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "value").setParent("p1").get();

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

        client().prepareIndex("test", "parent2", "p1").setSource("p_field", "value").setRefresh(true).get();

        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        response = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), termFilter("_parent", "parent#p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        // test terms filter
        client().prepareIndex("test", "child2", "c1").setSource("c_field", "value").setParent("p1").get();
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
                .setQuery(filteredQuery(matchAllQuery(), termsFilter("_parent", "parent#p1", "parent2#p1"))).get();
        assertHitCount(response, 2l);
    }

    @Test
    public void testHasChildNotBeingCached() throws ElasticsearchException, IOException {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "parent", "p3").setSource("p_field", "p_value3").get();
        client().prepareIndex("test", "parent", "p4").setSource("p_field", "p_value4").get();
        client().prepareIndex("test", "parent", "p5").setSource("p_field", "p_value5").get();
        client().prepareIndex("test", "parent", "p6").setSource("p_field", "p_value6").get();
        client().prepareIndex("test", "parent", "p7").setSource("p_field", "p_value7").get();
        client().prepareIndex("test", "parent", "p8").setSource("p_field", "p_value8").get();
        client().prepareIndex("test", "parent", "p9").setSource("p_field", "p_value9").get();
        client().prepareIndex("test", "parent", "p10").setSource("p_field", "p_value10").get();
        client().prepareIndex("test", "child", "c1").setParent("p1").setSource("c_field", "blue").get();
        client().admin().indices().prepareFlush("test").get();
        client().admin().indices().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue")).cache(true)))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        client().prepareIndex("test", "child", "c2").setParent("p2").setSource("c_field", "blue").get();
        client().admin().indices().prepareRefresh("test").get();

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue")).cache(true)))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }

    @Test
    public void testDeleteByQuery_has_child() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(
                        settingsBuilder().put(indexSettings())
                                .put("index.refresh_interval", "-1")
                )
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().admin().indices().prepareFlush("test").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();
        client().prepareIndex("test", "parent", "p3").setSource("p_field", "p_value3").get();
        client().admin().indices().prepareFlush("test").get();
        client().prepareIndex("test", "child", "c5").setSource("c_field", "blue").setParent("p3").get();
        client().prepareIndex("test", "child", "c6").setSource("c_field", "red").setParent("p3").get();
        client().admin().indices().prepareRefresh().get();
        // p4 will not be found via search api, but will be deleted via delete_by_query api!
        client().prepareIndex("test", "parent", "p4").setSource("p_field", "p_value4").get();
        client().prepareIndex("test", "child", "c7").setSource("c_field", "blue").setParent("p4").get();
        client().prepareIndex("test", "child", "c8").setSource("c_field", "red").setParent("p4").get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomHasChild("child", "c_field", "blue"))
                .get();
        assertHitCount(searchResponse, 2l);

        // Delete by query doesn't support p/c queries. If the delete by query has a different execution mode
        // that doesn't rely on IW#deleteByQuery() then this test can be changed.
        DeleteByQueryResponse deleteByQueryResponse = client().prepareDeleteByQuery("test").setQuery(randomHasChild("child", "c_field", "blue")).get();
        assertThat(deleteByQueryResponse.getIndex("test").getSuccessfulShards(), equalTo(0));
        assertThat(deleteByQueryResponse.getIndex("test").getFailedShards(), equalTo(getNumShards("test").numPrimaries));
        assertThat(deleteByQueryResponse.getIndex("test").getFailures()[0].reason(), containsString("[has_child] unsupported in delete_by_query api"));
        client().admin().indices().prepareRefresh("test").get();

        searchResponse = client().prepareSearch("test")
                .setQuery(randomHasChild("child", "c_field", "blue"))
                .get();
        assertHitCount(searchResponse, 3l);
    }

    @Test
    public void testDeleteByQuery_has_child_SingleRefresh() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(
                        settingsBuilder()
                                .put(indexSettings())
                                .put("index.refresh_interval", "-1")
                )
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().admin().indices().prepareFlush().get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();
        client().prepareIndex("test", "parent", "p3").setSource("p_field", "p_value3").get();
        client().prepareIndex("test", "child", "c5").setSource("c_field", "blue").setParent("p3").get();
        client().prepareIndex("test", "child", "c6").setSource("c_field", "red").setParent("p3").get();
        client().prepareIndex("test", "parent", "p4").setSource("p_field", "p_value4").get();
        client().prepareIndex("test", "child", "c7").setSource("c_field", "blue").setParent("p4").get();
        client().prepareIndex("test", "child", "c8").setSource("c_field", "red").setParent("p4").get();
        client().admin().indices().prepareRefresh().get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomHasChild("child", "c_field", "blue"))
                .get();
        assertHitCount(searchResponse, 3l);

        DeleteByQueryResponse deleteByQueryResponse = client().prepareDeleteByQuery("test").setQuery(randomHasChild("child", "c_field", "blue")).get();
        assertThat(deleteByQueryResponse.getIndex("test").getSuccessfulShards(), equalTo(0));
        assertThat(deleteByQueryResponse.getIndex("test").getFailedShards(), equalTo(getNumShards("test").numPrimaries));
        assertThat(deleteByQueryResponse.getIndex("test").getFailures()[0].reason(), containsString("[has_child] unsupported in delete_by_query api"));
        client().admin().indices().prepareRefresh("test").get();

        searchResponse = client().prepareSearch("test")
                .setQuery(randomHasChild("child", "c_field", "blue"))
                .get();
        assertHitCount(searchResponse, 3l);
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
        assertAcked(prepareCreate("test")
                .setSettings(
                        settingsBuilder()
                                .put(indexSettings())
                                .put("index.refresh_interval", "-1")
                )
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("c_field", "yellow").setParent("p1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().admin().indices().prepareFlush("test").get();
        client().prepareIndex("test", "child", "c3").setSource("c_field", "blue").setParent("p2").get();
        client().prepareIndex("test", "child", "c4").setSource("c_field", "red").setParent("p2").get();
        client().admin().indices().prepareRefresh().get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(randomHasParent("parent", "p_field", "p_value2"))
                .get();
        assertHitCount(searchResponse, 2l);

        DeleteByQueryResponse deleteByQueryResponse = client().prepareDeleteByQuery("test")
                .setQuery(randomHasParent("parent", "p_field", "p_value2"))
                .get();
        assertThat(deleteByQueryResponse.getIndex("test").getSuccessfulShards(), equalTo(0));
        assertThat(deleteByQueryResponse.getIndex("test").getFailedShards(), equalTo(getNumShards("test").numPrimaries));
        assertThat(deleteByQueryResponse.getIndex("test").getFailures()[0].reason(), containsString("[has_parent] unsupported in delete_by_query api"));
        client().admin().indices().prepareRefresh("test").get();
        client().admin().indices().prepareRefresh("test").get();
        client().admin().indices().prepareRefresh("test").get();

        searchResponse = client().prepareSearch("test")
                .setQuery(randomHasParent("parent", "p_field", "p_value2"))
                .get();
        assertHitCount(searchResponse, 2l);
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
        assertAcked(prepareCreate("grandissue")
                .addMapping("grandparent", "name", "type=string")
                .addMapping("parent", "_parent", "type=grandparent")
                .addMapping("child_type_one", "_parent", "type=parent")
                .addMapping("child_type_two", "_parent", "type=parent"));

        client().prepareIndex("grandissue", "grandparent", "1").setSource("name", "Grandpa").get();
        client().prepareIndex("grandissue", "parent", "2").setParent("1").setSource("name", "Dana").get();
        client().prepareIndex("grandissue", "child_type_one", "3").setParent("2").setRouting("1")
                .setSource("name", "William")
                .get();
        client().prepareIndex("grandissue", "child_type_two", "4").setParent("2").setRouting("1")
                .setSource("name", "Kate")
                .get();
        refresh();

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
        ).get();
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
        ).get();
        assertHitCount(searchResponse, 0l);
    }

    @Test
    public void indexChildDocWithNoParentMapping() throws ElasticsearchException, IOException {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child1"));
        ensureGreen();

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1", "_parent", "bla").get();
        try {
            client().prepareIndex("test", "child1", "c1").setParent("p1").setSource("c_field", "blue").get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Can't specify parent if no parent field has been configured"));
        }
        try {
            client().prepareIndex("test", "child2", "c2").setParent("p1").setSource("c_field", "blue").get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Can't specify parent if no parent field has been configured"));
        }

        refresh();
    }

    @Test
    public void testAddingParentToExistingMapping() throws ElasticsearchException, IOException {
        createIndex("test");
        ensureGreen();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping("test").setType("child").setSource("number", "type=integer")
                .get();
        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        Map<String, Object> mapping = getMappingsResponse.getMappings().get("test").get("child").getSourceAsMap();
        assertThat(mapping.size(), greaterThanOrEqualTo(1)); // there are potentially some meta fields configured randomly
        assertThat(mapping.get("properties"), notNullValue());

        try {
            // Adding _parent metadata field to existing mapping is prohibited:
            client().admin().indices().preparePutMapping("test").setType("child").setSource(jsonBuilder().startObject().startObject("child")
                    .startObject("_parent").field("type", "parent").endObject()
                    .endObject().endObject()).get();
            fail();
        } catch (MergeMappingException e) {
            assertThat(e.getMessage(), equalTo("Merge failed with failures {[The _parent field's type option can't be changed]}"));
        }
    }

    @Test
    // The SimpleIdReaderTypeCache#docById method used lget, which can't be used if a map is shared.
    public void testTopChildrenBug_concurrencyIssue() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c1").setParent("p1").setSource("c_field", "blue").get();
        client().prepareIndex("test", "child", "c2").setParent("p1").setSource("c_field", "red").get();
        client().prepareIndex("test", "child", "c3").setParent("p2").setSource("c_field", "red").get();
        client().admin().indices().prepareRefresh("test").get();

        int numThreads = 10;
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final AtomicReference<AssertionError> holder = new AtomicReference<>();
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < 100; i++) {
                        SearchResponse searchResponse = client().prepareSearch("test")
                                .setQuery(topChildrenQuery("child", termQuery("c_field", "blue")))
                                .get();
                        assertNoFailures(searchResponse);
                        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

                        searchResponse = client().prepareSearch("test")
                                .setQuery(topChildrenQuery("child", termQuery("c_field", "red")))
                                .get();
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
        assertAcked(prepareCreate("test")
                .addMapping("parent", "objects", "type=nested")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        client().prepareIndex("test", "parent", "p1")
                .setSource(jsonBuilder().startObject().field("p_field", "1").startArray("objects")
                        .startObject().field("i_field", "1").endObject()
                        .startObject().field("i_field", "2").endObject()
                        .startObject().field("i_field", "3").endObject()
                        .startObject().field("i_field", "4").endObject()
                        .startObject().field("i_field", "5").endObject()
                        .startObject().field("i_field", "6").endObject()
                        .endArray().endObject())
                .get();
        client().prepareIndex("test", "parent", "p2")
                .setSource(jsonBuilder().startObject().field("p_field", "2").startArray("objects")
                        .startObject().field("i_field", "1").endObject()
                        .startObject().field("i_field", "2").endObject()
                        .endArray().endObject())
                .get();
        client().prepareIndex("test", "child", "c1").setParent("p1").setSource("c_field", "blue").get();
        client().prepareIndex("test", "child", "c2").setParent("p1").setSource("c_field", "red").get();
        client().prepareIndex("test", "child", "c3").setParent("p2").setSource("c_field", "red").get();
        refresh();

        String scoreMode = ScoreType.values()[getRandom().nextInt(ScoreType.values().length)].name().toLowerCase(Locale.ROOT);
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(QueryBuilders.hasChildQuery("child", termQuery("c_field", "blue")).scoreType(scoreMode), notFilter(termFilter("p_field", "3"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test")
                .setQuery(filteredQuery(QueryBuilders.hasChildQuery("child", termQuery("c_field", "red")).scoreType(scoreMode), notFilter(termFilter("p_field", "3"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }

    @Test
    public void testNamedFilters() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId).setSource("p_field", "1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "1").setParent(parentId).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(topChildrenQuery("child", termQuery("c_field", "1")).queryName("test"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreType("max").queryName("test"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));

        searchResponse = client().prepareSearch("test").setQuery(hasParentQuery("parent", termQuery("p_field", "1")).scoreType("score").queryName("test"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "1")).filterName("test")))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasParentFilter("parent", termQuery("p_field", "1")).filterName("test")))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));
    }

    @Test
    public void testParentChildQueriesNoParentType() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(indexSettings())
                        .put("index.refresh_interval", -1)));
        ensureGreen();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId).setSource("p_field", "1").get();
        refresh();

        try {
            client().prepareSearch("test")
                    .setQuery(hasChildQuery("child", termQuery("c_field", "1")))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            client().prepareSearch("test")
                    .setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreType("max"))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            client().prepareSearch("test")
                    .setPostFilter(hasChildFilter("child", termQuery("c_field", "1")))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            client().prepareSearch("test")
                    .setQuery(topChildrenQuery("child", termQuery("c_field", "1")).score("max"))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            client().prepareSearch("test")
                    .setQuery(hasParentQuery("parent", termQuery("p_field", "1")).scoreType("score"))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            client().prepareSearch("test")
                    .setPostFilter(hasParentFilter("parent", termQuery("p_field", "1")))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }
    }

    @Test
    public void testAdd_ParentFieldAfterIndexingParentDocButBeforeIndexingChildDoc() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder()
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
                .setPostFilter(hasChildFilter("child", termQuery("c_field", "1")))
                .get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, parentId);

        searchResponse = client().prepareSearch("test")
                .setQuery(topChildrenQuery("child", termQuery("c_field", "1")).score("max"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertSearchHits(searchResponse, parentId);

        searchResponse = client().prepareSearch("test")
                .setPostFilter(hasParentFilter("parent", termQuery("p_field", "1")))
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
    public void testParentChildCaching() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(
                        settingsBuilder()
                                .put(indexSettings())
                                .put("index.refresh_interval", -1)
                )
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        client().prepareIndex("test", "parent", "p2").setSource("p_field", "p_value2").get();
        client().prepareIndex("test", "child", "c1").setParent("p1").setSource("c_field", "blue").get();
        client().prepareIndex("test", "child", "c2").setParent("p1").setSource("c_field", "red").get();
        client().prepareIndex("test", "child", "c3").setParent("p2").setSource("c_field", "red").get();
        client().admin().indices().prepareOptimize("test").setFlush(true).setWaitForMerge(true).get();
        client().prepareIndex("test", "parent", "p3").setSource("p_field", "p_value3").get();
        client().prepareIndex("test", "parent", "p4").setSource("p_field", "p_value4").get();
        client().prepareIndex("test", "child", "c4").setParent("p3").setSource("c_field", "green").get();
        client().prepareIndex("test", "child", "c5").setParent("p3").setSource("c_field", "blue").get();
        client().prepareIndex("test", "child", "c6").setParent("p4").setSource("c_field", "blue").get();
        client().admin().indices().prepareFlush("test").get();
        client().admin().indices().prepareRefresh("test").get();

        for (int i = 0; i < 2; i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(filteredQuery(matchAllQuery(), boolFilter()
                            .must(FilterBuilders.hasChildFilter("child", matchQuery("c_field", "red")))
                            .must(matchAllFilter())
                            .cache(true)))
                    .get();
            assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        }


        client().prepareIndex("test", "child", "c3").setParent("p2").setSource("c_field", "blue").get();
        client().admin().indices().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(filteredQuery(matchAllQuery(), boolFilter()
                        .must(FilterBuilders.hasChildFilter("child", matchQuery("c_field", "red")))
                        .must(matchAllFilter())
                        .cache(true)))
                .get();

        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void testParentChildQueriesViaScrollApi() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "parent", "p" + i).setSource("{}").get();
            client().prepareIndex("test", "child", "c" + i).setSource("{}").setParent("p" + i).get();
        }

        refresh();

        QueryBuilder[] queries = new QueryBuilder[]{
                hasChildQuery("child", matchAllQuery()),
                filteredQuery(matchAllQuery(), hasChildFilter("child", matchAllQuery())),
                hasParentQuery("parent", matchAllQuery()),
                filteredQuery(matchAllQuery(), hasParentFilter("parent", matchAllQuery())),
                topChildrenQuery("child", matchAllQuery()).factor(10)
        };

        for (QueryBuilder query : queries) {
            SearchResponse scrollResponse = client().prepareSearch("test")
                    .setScroll(TimeValue.timeValueSeconds(30))
                    .setSize(1)
                    .addField("_id")
                    .setQuery(query)
                    .setSearchType("scan")
                    .execute()
                    .actionGet();

            assertNoFailures(scrollResponse);
            assertThat(scrollResponse.getHits().totalHits(), equalTo(10l));

            int scannedDocs = 0;
            do {
                scrollResponse = client()
                        .prepareSearchScroll(scrollResponse.getScrollId())
                        .setScroll(TimeValue.timeValueSeconds(30)).get();
                assertThat(scrollResponse.getHits().totalHits(), equalTo(10l));
                scannedDocs += scrollResponse.getHits().getHits().length;
            } while (scrollResponse.getHits().getHits().length > 0);
            assertThat(scannedDocs, equalTo(10));
        }
    }

    @Test
    public void testValidateThatHasChildAndHasParentFilterAreNeverCached() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(builder().put(indexSettings())
                        //we need 0 replicas here to make sure we always hit the very same shards
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("field", "value")
                .get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("field", "value")
                .setRefresh(true)
                .get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery()))
                .get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test")
                .setQuery(hasParentQuery("parent", matchAllQuery()))
                .get();
        assertHitCount(searchResponse, 1l);

        // Internally the has_child and has_parent use filter for the type field, which end up in the filter cache,
        // so by first checking how much they take by executing has_child and has_parent *query* we can set a base line
        // for the filter cache size in this test.
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats("test").clear().setFilterCache(true).get();
        long initialCacheSize = statsResponse.getIndex("test").getTotal().getFilterCache().getMemorySizeInBytes();

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.filteredQuery(matchAllQuery(), FilterBuilders.hasChildFilter("child", matchAllQuery()).cache(true)))
                .get();
        assertHitCount(searchResponse, 1l);

        statsResponse = client().admin().indices().prepareStats("test").clear().setFilterCache(true).get();
        assertThat(statsResponse.getIndex("test").getTotal().getFilterCache().getMemorySizeInBytes(), equalTo(initialCacheSize));

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.filteredQuery(matchAllQuery(), FilterBuilders.hasParentFilter("parent", matchAllQuery()).cache(true)))
                .get();
        assertHitCount(searchResponse, 1l);

        // filter cache should not contain any thing, b/c has_child and has_parent can't be cached.
        statsResponse = client().admin().indices().prepareStats("test").clear().setFilterCache(true).get();
        assertThat(statsResponse.getIndex("test").getTotal().getFilterCache().getMemorySizeInBytes(), equalTo(initialCacheSize));

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.filteredQuery(
                        matchAllQuery(),
                        FilterBuilders.boolFilter().cache(true)
                                .must(FilterBuilders.matchAllFilter())
                                .must(FilterBuilders.hasChildFilter("child", matchAllQuery()).cache(true))
                ))
                .get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.filteredQuery(
                        matchAllQuery(),
                        FilterBuilders.boolFilter().cache(true)
                                .must(FilterBuilders.matchAllFilter())
                                .must(FilterBuilders.hasParentFilter("parent", matchAllQuery()).cache(true))
                ))
                .get();
        assertHitCount(searchResponse, 1l);

        // filter cache should not contain any thing, b/c has_child and has_parent can't be cached.
        statsResponse = client().admin().indices().prepareStats("test").clear().setFilterCache(true).get();
        assertThat(statsResponse.getIndex("test").getTotal().getFilterCache().getMemorySizeInBytes(), equalTo(initialCacheSize));

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.filteredQuery(
                        matchAllQuery(),
                        FilterBuilders.boolFilter().cache(true)
                                .must(FilterBuilders.termFilter("field", "value").cache(true))
                                .must(FilterBuilders.hasChildFilter("child", matchAllQuery()).cache(true))
                ))
                .get();
        assertHitCount(searchResponse, 1l);

        searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.filteredQuery(
                        matchAllQuery(),
                        FilterBuilders.boolFilter().cache(true)
                                .must(FilterBuilders.termFilter("field", "value").cache(true))
                                .must(FilterBuilders.hasParentFilter("parent", matchAllQuery()).cache(true))
                ))
                .get();
        assertHitCount(searchResponse, 1l);

        // filter cache should not contain any thing, b/c has_child and has_parent can't be cached.
        statsResponse = client().admin().indices().prepareStats("test").clear().setFilterCache(true).get();
        assertThat(statsResponse.getIndex("test").getTotal().getFilterCache().getMemorySizeInBytes(), cluster().hasFilterCache() ? greaterThan(initialCacheSize) : is(initialCacheSize));
    }

    // https://github.com/elasticsearch/elasticsearch/issues/5783
    @Test
    public void testQueryBeforeChildType() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("features")
                .addMapping("posts", "_parent", "type=features")
                .addMapping("specials"));
        ensureGreen();

        client().prepareIndex("test", "features", "1").setSource("field", "foo").get();
        client().prepareIndex("test", "posts", "1").setParent("1").setSource("field", "bar").get();
        refresh();

        SearchResponse resp;
        resp = client().prepareSearch("test")
                .setSource("{\"query\": {\"has_child\": {\"type\": \"posts\", \"query\": {\"match\": {\"field\": \"bar\"}}}}}").get();
        assertHitCount(resp, 1L);

        // Now reverse the order for the type after the query
        resp = client().prepareSearch("test")
                .setSource("{\"query\": {\"has_child\": {\"query\": {\"match\": {\"field\": \"bar\"}}, \"type\": \"posts\"}}}").get();
        assertHitCount(resp, 1L);

    }

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/6256
    public void testParentFieldInMultiMatchField() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1")
                .addMapping("type2", "_parent", "type=type1")
        );
        ensureGreen();

        client().prepareIndex("test", "type2", "1").setParent("1").setSource("field", "value").get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(multiMatchQuery("1", "_parent"))
                .get();

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().getAt(0).id(), equalTo("1"));
    }

    List<IndexRequestBuilder> createMinMaxDocBuilders() {
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        // Parent 1 and its children
        indexBuilders.add(client().prepareIndex().setType("parent").setId("1").setIndex("test").setSource("id",1));
        indexBuilders.add(client().prepareIndex().setType("child").setId("10").setIndex("test")
                .setSource("foo", "one").setParent("1"));

        // Parent 2 and its children
        indexBuilders.add(client().prepareIndex().setType("parent").setId("2").setIndex("test").setSource("id",2));
        indexBuilders.add(client().prepareIndex().setType("child").setId("11").setIndex("test")
                .setSource("foo", "one").setParent("2"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("12").setIndex("test")
                .setSource("foo", "one two").setParent("2"));

        // Parent 3 and its children
        indexBuilders.add(client().prepareIndex().setType("parent").setId("3").setIndex("test").setSource("id",3));
        indexBuilders.add(client().prepareIndex().setType("child").setId("13").setIndex("test")
                .setSource("foo", "one").setParent("3"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("14").setIndex("test")
                .setSource("foo", "one two").setParent("3"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("15").setIndex("test")
                .setSource("foo", "one two three").setParent("3"));

        // Parent 4 and its children
        indexBuilders.add(client().prepareIndex().setType("parent").setId("4").setIndex("test").setSource("id",4));
        indexBuilders.add(client().prepareIndex().setType("child").setId("16").setIndex("test")
                .setSource("foo", "one").setParent("4"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("17").setIndex("test")
                .setSource("foo", "one two").setParent("4"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("18").setIndex("test")
                .setSource("foo", "one two three").setParent("4"));
        indexBuilders.add(client().prepareIndex().setType("child").setId("19").setIndex("test")
                .setSource("foo", "one two three four").setParent("4"));

        return indexBuilders;
    }

    SearchResponse MinMaxQuery(String scoreType, int minChildren, int maxChildren, int cutoff) throws SearchPhaseExecutionException {
        return client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders
                                .hasChildQuery(
                                        "child",
                                        QueryBuilders.functionScoreQuery(constantScoreQuery(FilterBuilders.termFilter("foo", "two"))).boostMode("replace").scoreMode("sum")
                                                .add(FilterBuilders.matchAllFilter(), factorFunction(1))
                                                .add(FilterBuilders.termFilter("foo", "three"), factorFunction(1))
                                                .add(FilterBuilders.termFilter("foo", "four"), factorFunction(1))).scoreType(scoreType)
                                .minChildren(minChildren).maxChildren(maxChildren).setShortCircuitCutoff(cutoff))
                .addSort("_score", SortOrder.DESC).addSort("id", SortOrder.ASC).get();
    }

    SearchResponse MinMaxFilter( int minChildren, int maxChildren, int cutoff) throws SearchPhaseExecutionException {
        return client()
                .prepareSearch("test")
                .setQuery(
                        QueryBuilders.constantScoreQuery(FilterBuilders.hasChildFilter("child", termFilter("foo", "two"))
                                .minChildren(minChildren).maxChildren(maxChildren).setShortCircuitCutoff(cutoff)))
                .addSort("id", SortOrder.ASC).setTrackScores(true).get();
    }


    @Test
    public void testMinMaxChildren() throws Exception {
        assertAcked(prepareCreate("test").addMapping("parent").addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        indexRandom(true, createMinMaxDocBuilders().toArray(new IndexRequestBuilder[0]));
        SearchResponse response;
        int cutoff = getRandom().nextInt(4);

        // Score mode = NONE
        response = MinMaxQuery("none", 0, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("none", 1, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("none", 2, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("4"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = MinMaxQuery("none", 3, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));

        response = MinMaxQuery("none", 4, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = MinMaxQuery("none", 0, 4, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("none", 0, 3, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("none", 0, 2, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = MinMaxQuery("none", 2, 2, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));

        try {
            response = MinMaxQuery("none", 3, 2, cutoff);
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), containsString("[has_child] 'max_children' is less than 'min_children'"));
        }

        // Score mode = SUM
        response = MinMaxQuery("sum", 0, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("sum", 1, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("sum", 2, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));

        response = MinMaxQuery("sum", 3, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));

        response = MinMaxQuery("sum", 4, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = MinMaxQuery("sum", 0, 4, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("sum", 0, 3, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("sum", 0, 2, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = MinMaxQuery("sum", 2, 2, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));

        try {
            response = MinMaxQuery("sum", 3, 2, cutoff);
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), containsString("[has_child] 'max_children' is less than 'min_children'"));
        }

        // Score mode = MAX
        response = MinMaxQuery("max", 0, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(2f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("max", 1, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(2f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("max", 2, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(2f));

        response = MinMaxQuery("max", 3, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));

        response = MinMaxQuery("max", 4, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = MinMaxQuery("max", 0, 4, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(2f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("max", 0, 3, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(2f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("max", 0, 2, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = MinMaxQuery("max", 2, 2, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));

        try {
            response = MinMaxQuery("max", 3, 2, cutoff);
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), containsString("[has_child] 'max_children' is less than 'min_children'"));
        }

        // Score mode = AVG
        response = MinMaxQuery("avg", 0, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1.5f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("avg", 1, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1.5f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("avg", 2, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1.5f));

        response = MinMaxQuery("avg", 3, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));

        response = MinMaxQuery("avg", 4, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = MinMaxQuery("avg", 0, 4, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1.5f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("avg", 0, 3, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1.5f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxQuery("avg", 0, 2, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1.5f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = MinMaxQuery("avg", 2, 2, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1.5f));

        try {
            response = MinMaxQuery("avg", 3, 2, cutoff);
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), containsString("[has_child] 'max_children' is less than 'min_children'"));
        }

        // HasChildFilter
        response = MinMaxFilter(0, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxFilter(1, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxFilter(2, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("4"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = MinMaxFilter(3, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));

        response = MinMaxFilter(4, 0, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = MinMaxFilter(0, 4, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxFilter(0, 3, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = MinMaxFilter(0, 2, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = MinMaxFilter(2, 2, cutoff);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));

        try {
            response = MinMaxFilter(3, 2, cutoff);
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), containsString("[has_child] 'max_children' is less than 'min_children'"));
        }

    }

    @Test
    public void testParentFieldToNonExistingType() {
        assertAcked(prepareCreate("test").addMapping("parent").addMapping("child", "_parent", "type=parent2"));
        client().prepareIndex("test", "parent", "1").setSource("{}").get();
        client().prepareIndex("test", "child", "1").setParent("1").setSource("{}").get();
        refresh();

        try {
            client().prepareSearch("test")
                    .setQuery(QueryBuilders.hasChildQuery("child", matchAllQuery()))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
        }

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.hasParentQuery("parent", matchAllQuery()))
                .get();
        assertHitCount(response, 0);

        try {
            client().prepareSearch("test")
                    .setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.hasChildFilter("child", matchAllQuery())))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
        }

        response = client().prepareSearch("test")
                .setQuery(QueryBuilders.constantScoreQuery(FilterBuilders.hasParentFilter("parent", matchAllQuery())))
                .get();
        assertHitCount(response, 0);
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
