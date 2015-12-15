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

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.HasChildQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasParentQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE)
public class ChildQuerySearchIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                // aggressive filter caching so that we can assert on the filter cache size
                .put(IndexModule.QUERY_CACHE_TYPE, IndexModule.INDEX_QUERY_CACHE)
                .put(IndexModule.QUERY_CACHE_EVERYTHING, true)
                .build();
    }

    public void testSelfReferentialIsForbidden() {
        try {
            prepareCreate("test").addMapping("type", "_parent", "type=type").get();
            fail("self referential should be forbidden");
        } catch (Exception e) {
            Throwable cause = e.getCause();
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), equalTo("The [_parent.type] option can't point to the same type"));
        }
    }

    public void testMultiLevelChild() throws Exception {
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
                        boolQuery()
                                .must(matchAllQuery())
                                .filter(hasChildQuery(
                                        "child",
                                        boolQuery().must(termQuery("c_field", "c_value1"))
                                                .filter(hasChildQuery("grandchild", termQuery("gc_field", "gc_value1")))))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(boolQuery().must(matchAllQuery()).filter(hasParentQuery("parent", termQuery("p_field", "p_value1")))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(boolQuery().must(matchAllQuery()).filter(hasParentQuery("child", termQuery("c_field", "c_value1")))).execute()
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

    // see #2744
    public void test2744() throws IOException {
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

    public void testSimpleChildQuery() throws Exception {
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
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(idsQuery("child").addIds("c1")).fields("_parent").execute()
                .actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));

        // TEST matching on parent
        searchResponse = client().prepareSearch("test").setQuery(termQuery("_parent", "p1")).fields("_parent").get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(1).field("_parent").value().toString(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(queryStringQuery("_parent:p1")).fields("_parent").get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(0).field("_parent").value().toString(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("c1"), equalTo("c2")));
        assertThat(searchResponse.getHits().getAt(1).field("_parent").value().toString(), equalTo("p1"));

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

    // Issue #3290
    public void testCachingBugWithFqueryFilter() throws Exception {
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
                    .setQuery(constantScoreQuery(hasChildQuery("child", matchAllQuery()).scoreMode(ScoreMode.Max)))
                    .get();
            assertNoFailures(searchResponse);
            searchResponse = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(hasParentQuery("parent", matchAllQuery()).score(true)))
                    .get();
            assertNoFailures(searchResponse);
        }
    }

    public void testHasParentFilter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();
        Map<String, Set<String>> parentToChildren = new HashMap<>();
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
                    .setQuery(constantScoreQuery(hasParentQuery("parent", termQuery("p_field", parentToChildrenEntry.getKey()))))
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

    public void testSimpleChildQueryWithFlush() throws Exception {
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

        // HAS CHILD QUERY

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow"))).execute()
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
                .setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "yellow")))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "blue"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "red"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));
    }

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
                .setQuery(hasChildQuery("child", boolQuery().should(termQuery("c_field", "red")).should(termQuery("c_field", "yellow"))))
                .addAggregation(AggregationBuilders.global("global").subAggregation(
                        AggregationBuilders.filter("filter").filter(boolQuery().should(termQuery("c_field", "red")).should(termQuery("c_field", "yellow"))).subAggregation(
                                AggregationBuilders.terms("facet1").field("c_field")))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), anyOf(equalTo("p2"), equalTo("p1")));
        assertThat(searchResponse.getHits().getAt(1).id(), anyOf(equalTo("p2"), equalTo("p1")));

        Global global = searchResponse.getAggregations().get("global");
        Filter filter = global.getAggregations().get("filter");
        Terms termsFacet = filter.getAggregations().get("facet1");
        assertThat(termsFacet.getBuckets().size(), equalTo(2));
        assertThat(termsFacet.getBuckets().get(0).getKeyAsString(), equalTo("red"));
        assertThat(termsFacet.getBuckets().get(0).getDocCount(), equalTo(2L));
        assertThat(termsFacet.getBuckets().get(1).getKeyAsString(), equalTo("yellow"));
        assertThat(termsFacet.getBuckets().get(1).getDocCount(), equalTo(1L));
    }

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

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "yellow")))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        // update p1 and see what that we get updated values...

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1_updated").get();
        client().admin().indices().prepareRefresh().get();

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "yellow")))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1_updated\""));
    }

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
                .setQuery(boolQuery().mustNot(hasChildQuery("child", boolQuery().should(queryStringQuery("c_field:*"))))).get();
        assertNoFailures(searchResponse);

        searchResponse = client().prepareSearch("test").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery().mustNot(hasParentQuery("parent", boolQuery().should(queryStringQuery("p_field:*"))))).execute()
                .actionGet();
        assertNoFailures(searchResponse);
    }

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
                .setQuery(boolQuery().must(matchAllQuery()).filter(hasChildQuery("child", matchAllQuery()))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test")
                .setQuery(boolQuery().must(matchAllQuery()).filter(hasParentQuery("parent", matchAllQuery()))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    public void testCountApiUsage() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId).setSource("p_field", "1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "1").setParent(parentId).get();
        refresh();

        SearchResponse countResponse = client().prepareSearch("test").setSize(0).setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreMode(ScoreMode.Max))
                .get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareSearch("test").setSize(0).setQuery(hasParentQuery("parent", termQuery("p_field", "1")).score(true))
                .get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareSearch("test").setSize(0).setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "1"))))
                .get();
        assertHitCount(countResponse, 1l);

        countResponse = client().prepareSearch("test").setSize(0).setQuery(constantScoreQuery(hasParentQuery("parent", termQuery("p_field", "1"))))
                .get();
        assertHitCount(countResponse, 1l);
    }

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
                .setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreMode(ScoreMode.Max))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).explanation().getDescription(), equalTo("Score based on join value p1"));

        searchResponse = client().prepareSearch("test")
                .setExplain(true)
                .setQuery(hasParentQuery("parent", termQuery("p_field", "1")).score(true))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).explanation().getDescription(), equalTo("Score based on join value p1"));

        ExplainResponse explainResponse = client().prepareExplain("test", "parent", parentId)
                .setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreMode(ScoreMode.Max))
                .get();
        assertThat(explainResponse.isExists(), equalTo(true));
        assertThat(explainResponse.getExplanation().getDetails()[0].getDescription(), equalTo("Score based on join value p1"));
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

    public void testScoreForParentChildQueriesWithFunctionScore() throws Exception {
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
                                QueryBuilders.functionScoreQuery(matchQuery("c_field2", 0),
                                        fieldValueFactorFunction("c_field1"))
                                        .boostMode(CombineFunction.REPLACE)).scoreMode(ScoreMode.Total)).get();

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
                                QueryBuilders.functionScoreQuery(matchQuery("c_field2", 0),
                                        fieldValueFactorFunction("c_field1"))
                                        .boostMode(CombineFunction.REPLACE)).scoreMode(ScoreMode.Max)).get();

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
                                QueryBuilders.functionScoreQuery(matchQuery("c_field2", 0),
                                        fieldValueFactorFunction("c_field1"))
                                        .boostMode(CombineFunction.REPLACE)).scoreMode(ScoreMode.Avg)).get();

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
                                QueryBuilders.functionScoreQuery(matchQuery("p_field1", "p_value3"),
                                        fieldValueFactorFunction("p_field2"))
                                        .boostMode(CombineFunction.REPLACE)).score(true))
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

    // Issue #2536
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

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasChildQuery("child", matchQuery("text", "value")).scoreMode(ScoreMode.Max))
                .get();
        assertNoFailures(response);
        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasParentQuery("child", matchQuery("text", "value"))).get();
        assertNoFailures(response);
        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = client().prepareSearch("test").setQuery(QueryBuilders.hasParentQuery("child", matchQuery("text", "value")).score(true))
                .get();
        assertNoFailures(response);
        assertThat(response.getHits().totalHits(), equalTo(0l));
    }

    public void testHasChildAndHasParentFilter_withFilter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        client().prepareIndex("test", "parent", "1").setSource("p_field", 1).get();
        client().prepareIndex("test", "child", "2").setParent("1").setSource("c_field", 1).get();
        client().admin().indices().prepareFlush("test").get();

        client().prepareIndex("test", "type1", "3").setSource("p_field", 2).get();
        client().admin().indices().prepareFlush("test").get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(boolQuery().must(matchAllQuery()).filter(hasChildQuery("child", termQuery("c_field", 1)))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(boolQuery().must(matchAllQuery()).filter(hasParentQuery("parent", termQuery("p_field", 1)))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("2"));
    }

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
                .setQuery(boolQuery().must(matchAllQuery()).filter(hasChildQuery("child", matchQuery("c_field", 1)))).get();
        assertSearchHit(searchResponse, 1, hasId("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(boolQuery().must(matchAllQuery()).filter(hasParentQuery("parent", matchQuery("p_field", 1)))).get();
        assertSearchHit(searchResponse, 1, hasId("2"));

        searchResponse = client().prepareSearch("test")
                .setQuery(boolQuery().must(matchAllQuery()).filter(boolQuery().must(hasChildQuery("child", matchQuery("c_field", 1))))).get();
        assertSearchHit(searchResponse, 1, hasId("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(boolQuery().must(matchAllQuery()).filter(boolQuery().must(hasParentQuery("parent", matchQuery("p_field", 1))))).get();
        assertSearchHit(searchResponse, 1, hasId("2"));
    }

    public void testSimpleQueryRewrite() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent", "p_field", "type=string")
                .addMapping("child", "_parent", "type=parent", "c_field", "type=string"));
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
                    .setQuery(hasChildQuery("child", prefixQuery("c_field", "c")).scoreMode(ScoreMode.Max)).addSort("p_field", SortOrder.ASC)
                    .setSize(5).get();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(10L));
            assertThat(searchResponse.getHits().hits()[0].id(), equalTo("p000"));
            assertThat(searchResponse.getHits().hits()[1].id(), equalTo("p001"));
            assertThat(searchResponse.getHits().hits()[2].id(), equalTo("p002"));
            assertThat(searchResponse.getHits().hits()[3].id(), equalTo("p003"));
            assertThat(searchResponse.getHits().hits()[4].id(), equalTo("p004"));

            searchResponse = client().prepareSearch("test").setSearchType(searchType)
                    .setQuery(hasParentQuery("parent", prefixQuery("p_field", "p")).score(true)).addSort("c_field", SortOrder.ASC)
                    .setSize(5).get();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().totalHits(), equalTo(500L));
            assertThat(searchResponse.getHits().hits()[0].id(), equalTo("c000"));
            assertThat(searchResponse.getHits().hits()[1].id(), equalTo("c001"));
            assertThat(searchResponse.getHits().hits()[2].id(), equalTo("c002"));
            assertThat(searchResponse.getHits().hits()[3].id(), equalTo("c003"));
            assertThat(searchResponse.getHits().hits()[4].id(), equalTo("c004"));
        }
    }

    // Issue #3144
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
                .setQuery(hasChildQuery("child", termQuery("c_field", "yellow")).scoreMode(ScoreMode.Total)).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        boolQuery().must(matchQuery("c_field", "x")).must(
                                hasParentQuery("parent", termQuery("p_field", "p_value2")).score(true))).get();
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

        searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "yellow")).scoreMode(ScoreMode.Total))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));
        assertThat(searchResponse.getHits().getAt(0).sourceAsString(), containsString("\"p_value1\""));

        searchResponse = client()
                .prepareSearch("test")
                .setQuery(
                        boolQuery().must(matchQuery("c_field", "x")).must(
                                hasParentQuery("parent", termQuery("p_field", "p_value2")).score(true))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).id(), Matchers.anyOf(equalTo("c3"), equalTo("c4")));
        assertThat(searchResponse.getHits().getAt(1).id(), Matchers.anyOf(equalTo("c3"), equalTo("c4")));
    }

    // Issue #3203
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

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", matchAllQuery()).scoreMode(ScoreMode.Total))
                .setMinScore(3) // Score needs to be 3 or above!
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p2"));
        assertThat(searchResponse.getHits().getAt(0).score(), equalTo(3.0f));
    }

    public void testParentFieldFilter() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder().put(indexSettings())
                        .put("index.refresh_interval", -1))
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent")
                .addMapping("child2", "_parent", "type=parent"));
        ensureGreen();

        // test term filter
        SearchResponse response = client().prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(termQuery("_parent", "p1")))
                .get();
        assertHitCount(response, 0l);

        client().prepareIndex("test", "some_type", "1").setSource("field", "value").get();
        client().prepareIndex("test", "parent", "p1").setSource("p_field", "value").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "value").setParent("p1").get();

        response = client().prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(termQuery("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 0l);
        refresh();

        response = client().prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(termQuery("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        response = client().prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(termQuery("_parent", "parent#p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        client().prepareIndex("test", "parent2", "p1").setSource("p_field", "value").setRefresh(true).get();

        response = client().prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(termQuery("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        response = client().prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(termQuery("_parent", "parent#p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        // test terms filter
        client().prepareIndex("test", "child2", "c1").setSource("c_field", "value").setParent("p1").get();
        response = client().prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(termsQuery("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        response = client().prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(termsQuery("_parent", "parent#p1"))).execute()
                .actionGet();
        assertHitCount(response, 1l);

        refresh();
        response = client().prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(termsQuery("_parent", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 2l);

        refresh();
        response = client().prepareSearch("test").setQuery(boolQuery().must(matchAllQuery()).filter(termsQuery("_parent", "p1", "p1"))).execute()
                .actionGet();
        assertHitCount(response, 2l);

        response = client().prepareSearch("test")
                .setQuery(boolQuery().must(matchAllQuery()).filter(termsQuery("_parent", "parent#p1", "parent2#p1"))).get();
        assertHitCount(response, 2l);
    }

    public void testHasChildNotBeingCached() throws IOException {
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
                .setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "blue"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        client().prepareIndex("test", "child", "c2").setParent("p2").setSource("c_field", "blue").get();
        client().admin().indices().prepareRefresh("test").get();

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "blue"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }

    private QueryBuilder randomHasChild(String type, String field, String value) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return constantScoreQuery(hasChildQuery(type, termQuery(field, value)));
            } else {
                return boolQuery().must(matchAllQuery()).filter(hasChildQuery(type, termQuery(field, value)));
            }
        } else {
            return hasChildQuery(type, termQuery(field, value));
        }
    }

    private QueryBuilder randomHasParent(String type, String field, String value) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return constantScoreQuery(hasParentQuery(type, termQuery(field, value)));
            } else {
                return boolQuery().must(matchAllQuery()).filter(hasParentQuery(type, termQuery(field, value)));
            }
        } else {
            return hasParentQuery(type, termQuery(field, value));
        }
    }

    // Issue #3818
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
                                                        queryStringQuery("name:William*").analyzeWildcard(true)
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
                                                        queryStringQuery("name:William*").analyzeWildcard(true)
                                                )
                                        )
                                )
                        )
                )
        ).get();
        assertHitCount(searchResponse, 0l);
    }

    public void testIndexChildDocWithNoParentMapping() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child1"));
        ensureGreen();

        client().prepareIndex("test", "parent", "p1").setSource("p_field", "p_value1").get();
        try {
            client().prepareIndex("test", "child1", "c1").setParent("p1").setSource("c_field", "blue").get();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.toString(), containsString("Can't specify parent if no parent field has been configured"));
        }
        try {
            client().prepareIndex("test", "child2", "c2").setParent("p1").setSource("c_field", "blue").get();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.toString(), containsString("Can't specify parent if no parent field has been configured"));
        }

        refresh();
    }

    public void testAddingParentToExistingMapping() throws IOException {
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
        } catch (IllegalArgumentException e) {
            assertThat(e.toString(), containsString("The _parent field's type option can't be changed: [null]->[parent]"));
        }
    }

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

        ScoreMode scoreMode = randomFrom(ScoreMode.values());
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(boolQuery().must(QueryBuilders.hasChildQuery("child", termQuery("c_field", "blue")).scoreMode(scoreMode)).filter(boolQuery().mustNot(termQuery("p_field", "3"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test")
                .setQuery(boolQuery().must(QueryBuilders.hasChildQuery("child", termQuery("c_field", "red")).scoreMode(scoreMode)).filter(boolQuery().mustNot(termQuery("p_field", "3"))))
                .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }

    public void testNamedFilters() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId).setSource("p_field", "1").get();
        client().prepareIndex("test", "child", "c1").setSource("c_field", "1").setParent(parentId).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreMode(ScoreMode.Max).queryName("test"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));

        searchResponse = client().prepareSearch("test").setQuery(hasParentQuery("parent", termQuery("p_field", "1")).score(true).queryName("test"))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasChildQuery("child", termQuery("c_field", "1")).queryName("test")))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));

        searchResponse = client().prepareSearch("test").setQuery(constantScoreQuery(hasParentQuery("parent", termQuery("p_field", "1")).queryName("test")))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries()[0], equalTo("test"));
    }

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
                    .setQuery(hasChildQuery("child", termQuery("c_field", "1")).scoreMode(ScoreMode.Max))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            client().prepareSearch("test")
                    .setPostFilter(hasChildQuery("child", termQuery("c_field", "1")))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            client().prepareSearch("test")
                    .setQuery(hasParentQuery("parent", termQuery("p_field", "1")).score(true))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }

        try {
            client().prepareSearch("test")
                    .setPostFilter(hasParentQuery("parent", termQuery("p_field", "1")))
                    .get();
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        }
    }

    public void testAddParentFieldAfterIndexingParentDocButBeforeIndexingChildDoc() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(indexSettings())
                        .put("index.refresh_interval", -1)));
        ensureGreen();

        String parentId = "p1";
        client().prepareIndex("test", "parent", parentId).setSource("p_field", "1").get();
        refresh();

        try {
            assertAcked(client().admin()
                    .indices()
                    .preparePutMapping("test")
                    .setType("child")
                    .setSource("_parent", "type=parent"));
            fail("Shouldn't be able the add the _parent field pointing to an already existing parent type");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("can't add a _parent field that points to an already existing type"));
        }
    }

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
        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(1).setFlush(true).get();
        client().prepareIndex("test", "parent", "p3").setSource("p_field", "p_value3").get();
        client().prepareIndex("test", "parent", "p4").setSource("p_field", "p_value4").get();
        client().prepareIndex("test", "child", "c4").setParent("p3").setSource("c_field", "green").get();
        client().prepareIndex("test", "child", "c5").setParent("p3").setSource("c_field", "blue").get();
        client().prepareIndex("test", "child", "c6").setParent("p4").setSource("c_field", "blue").get();
        client().admin().indices().prepareFlush("test").get();
        client().admin().indices().prepareRefresh("test").get();

        for (int i = 0; i < 2; i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(boolQuery().must(matchAllQuery()).filter(boolQuery()
                            .must(QueryBuilders.hasChildQuery("child", matchQuery("c_field", "red")))
                            .must(matchAllQuery())))
                    .get();
            assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        }


        client().prepareIndex("test", "child", "c3").setParent("p2").setSource("c_field", "blue").get();
        client().admin().indices().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(boolQuery().must(matchAllQuery()).filter(boolQuery()
                        .must(QueryBuilders.hasChildQuery("child", matchQuery("c_field", "red")))
                        .must(matchAllQuery())))
                .get();

        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

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
                boolQuery().must(matchAllQuery()).filter(hasChildQuery("child", matchAllQuery())),
                hasParentQuery("parent", matchAllQuery()),
                boolQuery().must(matchAllQuery()).filter(hasParentQuery("parent", matchAllQuery()))
        };

        for (QueryBuilder query : queries) {
            SearchResponse scrollResponse = client().prepareSearch("test")
                    .setScroll(TimeValue.timeValueSeconds(30))
                    .setSize(1)
                    .addField("_id")
                    .setQuery(query)
                    .execute()
                    .actionGet();

            assertNoFailures(scrollResponse);
            assertThat(scrollResponse.getHits().totalHits(), equalTo(10l));
            int scannedDocs = 0;
            do {
                assertThat(scrollResponse.getHits().totalHits(), equalTo(10l));
                scannedDocs += scrollResponse.getHits().getHits().length;
                scrollResponse = client()
                        .prepareSearchScroll(scrollResponse.getScrollId())
                        .setScroll(TimeValue.timeValueSeconds(30)).get();
            } while (scrollResponse.getHits().getHits().length > 0);
            clearScroll(scrollResponse.getScrollId());
            assertThat(scannedDocs, equalTo(10));
        }
    }

    // Issue #5783
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
                .setSource(new SearchSourceBuilder().query(QueryBuilders.hasChildQuery("posts", QueryBuilders.matchQuery("field", "bar"))))
                .get();
        assertHitCount(resp, 1L);
    }

    // Issue #6256
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

    public void testTypeIsAppliedInHasParentInnerQuery() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(client().prepareIndex("test", "parent", "1").setSource("field1", "a"));
        indexRequests.add(client().prepareIndex("test", "child", "1").setParent("1").setSource("{}"));
        indexRequests.add(client().prepareIndex("test", "child", "2").setParent("1").setSource("{}"));
        indexRandom(true, indexRequests);

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasParentQuery("parent", boolQuery().mustNot(termQuery("field1", "a")))))
                .get();
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch("test")
                .setQuery(hasParentQuery("parent", constantScoreQuery(boolQuery().mustNot(termQuery("field1", "a")))))
                .get();
        assertHitCount(searchResponse, 0l);

        searchResponse = client().prepareSearch("test")
                .setQuery(constantScoreQuery(hasParentQuery("parent", termQuery("field1", "a"))))
                .get();
        assertHitCount(searchResponse, 2l);

        searchResponse = client().prepareSearch("test")
                .setQuery(hasParentQuery("parent", constantScoreQuery(termQuery("field1", "a"))))
                .get();
        assertHitCount(searchResponse, 2l);
    }

    private List<IndexRequestBuilder> createMinMaxDocBuilders() {
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

    private SearchResponse minMaxQuery(ScoreMode scoreMode, int minChildren, Integer maxChildren) throws SearchPhaseExecutionException {
        HasChildQueryBuilder hasChildQuery = hasChildQuery(
                "child",
                QueryBuilders.functionScoreQuery(constantScoreQuery(QueryBuilders.termQuery("foo", "two")),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(weightFactorFunction(1)),
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.termQuery("foo", "three"), weightFactorFunction(1)),
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.termQuery("foo", "four"), weightFactorFunction(1))
                        }).boostMode(CombineFunction.REPLACE).scoreMode(FiltersFunctionScoreQuery.ScoreMode.SUM)).scoreMode(scoreMode).minChildren(minChildren);

        if (maxChildren != null) {
            hasChildQuery.maxChildren(maxChildren);
        }

        return client()
                .prepareSearch("test")
                .setQuery(hasChildQuery)
                .addSort("_score", SortOrder.DESC).addSort("id", SortOrder.ASC).get();
    }

    public void testMinMaxChildren() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent", "id", "type=long")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        indexRandom(true, createMinMaxDocBuilders().toArray(new IndexRequestBuilder[0]));
        SearchResponse response;

        // Score mode = NONE
        response = minMaxQuery(ScoreMode.None, 0, 0);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.None, 1, 0);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.None, 2, 0);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("4"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.None, 3, 0);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.None, 4, 0);

        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = minMaxQuery(ScoreMode.None, 0, 4);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.None, 0, 3);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));
        assertThat(response.getHits().hits()[2].id(), equalTo("4"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.None, 0, 2);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.None, 2, 2);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1f));

        try {
            response = minMaxQuery(ScoreMode.None, 3, 2);
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("[has_child] 'max_children' is less than 'min_children'"));
        }

        // Score mode = SUM
        response = minMaxQuery(ScoreMode.Total, 0, 0);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Total, 1, 0);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Total, 2, 0);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));

        response = minMaxQuery(ScoreMode.Total, 3, 0);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));

        response = minMaxQuery(ScoreMode.Total, 4, 0);

        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = minMaxQuery(ScoreMode.Total, 0, 4);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Total, 0, 3);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(6f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(3f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Total, 0, 2);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Total, 2, 2);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));

        try {
            response = minMaxQuery(ScoreMode.Total, 3, 2);
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("[has_child] 'max_children' is less than 'min_children'"));
        }

        // Score mode = MAX
        response = minMaxQuery(ScoreMode.Max, 0, 0);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(2f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Max, 1, 0);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(2f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Max, 2, 0);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(2f));

        response = minMaxQuery(ScoreMode.Max, 3, 0);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));

        response = minMaxQuery(ScoreMode.Max, 4, 0);

        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = minMaxQuery(ScoreMode.Max, 0, 4);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(2f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Max, 0, 3);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(3f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(2f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Max, 0, 2);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Max, 2, 2);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));

        try {
            response = minMaxQuery(ScoreMode.Max, 3, 2);
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("[has_child] 'max_children' is less than 'min_children'"));
        }

        // Score mode = AVG
        response = minMaxQuery(ScoreMode.Avg, 0, 0);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1.5f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Avg, 1, 0);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1.5f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Avg, 2, 0);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1.5f));

        response = minMaxQuery(ScoreMode.Avg, 3, 0);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));

        response = minMaxQuery(ScoreMode.Avg, 4, 0);

        assertThat(response.getHits().totalHits(), equalTo(0l));

        response = minMaxQuery(ScoreMode.Avg, 0, 4);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1.5f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Avg, 0, 3);

        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().hits()[0].id(), equalTo("4"));
        assertThat(response.getHits().hits()[0].score(), equalTo(2f));
        assertThat(response.getHits().hits()[1].id(), equalTo("3"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1.5f));
        assertThat(response.getHits().hits()[2].id(), equalTo("2"));
        assertThat(response.getHits().hits()[2].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Avg, 0, 2);

        assertThat(response.getHits().totalHits(), equalTo(2l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1.5f));
        assertThat(response.getHits().hits()[1].id(), equalTo("2"));
        assertThat(response.getHits().hits()[1].score(), equalTo(1f));

        response = minMaxQuery(ScoreMode.Avg, 2, 2);

        assertThat(response.getHits().totalHits(), equalTo(1l));
        assertThat(response.getHits().hits()[0].id(), equalTo("3"));
        assertThat(response.getHits().hits()[0].score(), equalTo(1.5f));

        try {
            response = minMaxQuery(ScoreMode.Avg, 3, 2);
            fail();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.toString(), containsString("[has_child] 'max_children' is less than 'min_children'"));
        }
    }

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
    }

    static HasChildQueryBuilder hasChildQuery(String type, QueryBuilder queryBuilder) {
        HasChildQueryBuilder hasChildQueryBuilder = QueryBuilders.hasChildQuery(type, queryBuilder);
        return hasChildQueryBuilder;
    }

    public void testHasParentInnerQueryType() {
        assertAcked(prepareCreate("test").addMapping("parent-type").addMapping("child-type", "_parent", "type=parent-type"));
        client().prepareIndex("test", "child-type", "child-id").setParent("parent-id").setSource("{}").get();
        client().prepareIndex("test", "parent-type", "parent-id").setSource("{}").get();
        refresh();
        //make sure that when we explicitly set a type, the inner query is executed in the context of the parent type instead
        SearchResponse searchResponse = client().prepareSearch("test").setTypes("child-type").setQuery(
                QueryBuilders.hasParentQuery("parent-type", new IdsQueryBuilder().addIds("parent-id"))).get();
        assertSearchHits(searchResponse, "child-id");
    }

    public void testHasChildInnerQueryType() {
        assertAcked(prepareCreate("test").addMapping("parent-type").addMapping("child-type", "_parent", "type=parent-type"));
        client().prepareIndex("test", "child-type", "child-id").setParent("parent-id").setSource("{}").get();
        client().prepareIndex("test", "parent-type", "parent-id").setSource("{}").get();
        refresh();
        //make sure that when we explicitly set a type, the inner query is executed in the context of the child type instead
        SearchResponse searchResponse = client().prepareSearch("test").setTypes("parent-type").setQuery(
                QueryBuilders.hasChildQuery("child-type", new IdsQueryBuilder().addIds("child-id"))).get();
        assertSearchHits(searchResponse, "parent-id");
    }
}
