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

package org.elasticsearch.nested;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.filter.FilterFacet;
import org.elasticsearch.search.facet.statistical.StatisticalFacet;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Assert;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

public class SimpleNestedTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleNested() throws Exception {
        XContentBuilder builder = jsonBuilder().
                startObject().
                field("type1").
                startObject().
                field("properties").
                startObject().
                field("nested1").
                startObject().
                field("type").
                value("nested").
                endObject().
                endObject().
                endObject().
                endObject();
        assertAcked(prepareCreate("test").addMapping("type1", builder));
        ensureGreen();

        // check on no data, see it works
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(termQuery("_all", "n_value1_1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("nested1.n_field1", "n_value1_1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested1")
                .startObject()
                .field("n_field1", "n_value1_1")
                .field("n_field2", "n_value2_1")
                .endObject()
                .startObject()
                .field("n_field1", "n_value1_2")
                .field("n_field2", "n_value2_2")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();

        waitForRelocation(ClusterHealthStatus.GREEN);
        // flush, so we fetch it from the index (as see that we filter nested docs)
        flush();
        GetResponse getResponse = client().prepareGet("test", "type1", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getSourceAsBytes(), notNullValue());

        // check the numDocs
        IndicesStatusResponse statusResponse = admin().indices().prepareStatus().get();
        assertNoFailures(statusResponse);
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(3l));

        // check that _all is working on nested docs
        searchResponse = client().prepareSearch("test").setQuery(termQuery("_all", "n_value1_1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("nested1.n_field1", "n_value1_1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        // search for something that matches the nested doc, and see that we don't find the nested doc
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("nested1.n_field1", "n_value1_1")).get();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        // now, do a nested query
        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"))).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"))).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // add another doc, one that would match if it was not nested...

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested1")
                .startObject()
                .field("n_field1", "n_value1_1")
                .field("n_field2", "n_value2_2")
                .endObject()
                .startObject()
                .field("n_field1", "n_value1_2")
                .field("n_field2", "n_value2_1")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();
        waitForRelocation(ClusterHealthStatus.GREEN);
        // flush, so we fetch it from the index (as see that we filter nested docs)
        flush();
        statusResponse = client().admin().indices().prepareStatus().get();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(6l));

        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // filter
        searchResponse = client().prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), nestedFilter("nested1",
                boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1"))))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // check with type prefix
        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("type1.nested1",
                boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1")))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // check delete, so all is gone...
        DeleteResponse deleteResponse = client().prepareDelete("test", "type1", "2").execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(true));

        // flush, so we fetch it from the index (as see that we filter nested docs)
        flush();
        statusResponse = client().admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(3l));

        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
    }

    @Test
    public void simpleNestedDeletedByQuery1() throws Exception {
        simpleNestedDeleteByQuery(3, 0);
    }

    @Test
    public void simpleNestedDeletedByQuery2() throws Exception {
        simpleNestedDeleteByQuery(3, 1);
    }

    @Test
    public void simpleNestedDeletedByQuery3() throws Exception {
        simpleNestedDeleteByQuery(3, 2);
    }

    private void simpleNestedDeleteByQuery(int total, int docToDelete) throws Exception {

        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder().put(indexSettings()).put("index.referesh_interval", -1).build())
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();

        for (int i = 0; i < total; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("field1", "value1")
                    .startArray("nested1")
                    .startObject()
                    .field("n_field1", "n_value1_1")
                    .field("n_field2", "n_value2_1")
                    .endObject()
                    .startObject()
                    .field("n_field1", "n_value1_2")
                    .field("n_field2", "n_value2_2")
                    .endObject()
                    .endArray()
                    .endObject()).execute().actionGet();
        }


        flush();
        IndicesStatusResponse statusResponse = client().admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(total * 3l));

        client().prepareDeleteByQuery("test").setQuery(QueryBuilders.idsQuery("type1").ids(Integer.toString(docToDelete))).execute().actionGet();
        flush();
        refresh();
        statusResponse = client().admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo((total * 3l) - 3));

        for (int i = 0; i < total; i++) {
            assertThat(client().prepareGet("test", "type1", Integer.toString(i)).execute().actionGet().isExists(), equalTo(i != docToDelete));
        }
    }

    @Test
    public void noChildrenNestedDeletedByQuery1() throws Exception {
        noChildrenNestedDeleteByQuery(3, 0);
    }

    @Test
    public void noChildrenNestedDeletedByQuery2() throws Exception {
        noChildrenNestedDeleteByQuery(3, 1);
    }

    @Test
    public void noChildrenNestedDeletedByQuery3() throws Exception {
        noChildrenNestedDeleteByQuery(3, 2);
    }

    private void noChildrenNestedDeleteByQuery(long total, int docToDelete) throws Exception {

        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder().put(indexSettings()).put("index.referesh_interval", -1).build())
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();


        for (int i = 0; i < total; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("field1", "value1")
                    .endObject()).execute().actionGet();
        }


        flush();
        refresh();

        IndicesStatusResponse statusResponse = client().admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(total));

        client().prepareDeleteByQuery("test").setQuery(QueryBuilders.idsQuery("type1").ids(Integer.toString(docToDelete))).execute().actionGet();
        flush();
        refresh();
        statusResponse = client().admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo((total) - 1));

        for (int i = 0; i < total; i++) {
            assertThat(client().prepareGet("test", "type1", Integer.toString(i)).execute().actionGet().isExists(), equalTo(i != docToDelete));
        }
    }

    @Test
    public void multiNested() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested").startObject("properties")
                        .startObject("nested2").field("type", "nested").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1", "1").startArray("nested2").startObject().field("field2", "2").endObject().startObject().field("field2", "3").endObject().endArray().endObject()
                .startObject().field("field1", "4").startArray("nested2").startObject().field("field2", "5").endObject().startObject().field("field2", "6").endObject().endArray().endObject()
                .endArray()
                .endObject()).execute().actionGet();

        // flush, so we fetch it from the index (as see that we filter nested docs)
        flush();
        GetResponse getResponse = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        waitForRelocation(ClusterHealthStatus.GREEN);
        // check the numDocs
        IndicesStatusResponse statusResponse = client().admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(7l));

        // do some multi nested queries
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1",
                termQuery("nested1.field1", "1"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1.nested2",
                termQuery("nested1.nested2.field2", "2"))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "1")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "2"))))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "1")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "3"))))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "1")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "4"))))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "1")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "5"))))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "4")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "5"))))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client().prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "4")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "2"))))).execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));
    }

    @Test
    public void testFacetsSingleShard() throws Exception {
        testFacets(1);
    }

    @Test
    public void testFacetsMultiShards() throws Exception {
        testFacets(between(2, DEFAULT_MAX_NUM_SHARDS));
    }

    private void testFacets(int numberOfShards) throws Exception {

        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(indexSettings())
                        .put("index.number_of_shards", numberOfShards))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested").startObject("properties")
                        .startObject("nested2").field("type", "nested")
                        .startObject("properties")
                        .startObject("field2_1").field("type", "string").endObject()
                        .startObject("field2_2").field("type", "long").endObject()
                        .endObject()
                        .endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1_1", "1").startArray("nested2").startObject().field("field2_1", "blue").field("field2_2", 5).endObject().startObject().field("field2_1", "yellow").field("field2_2", 3).endObject().endArray().endObject()
                .startObject().field("field1_1", "4").startArray("nested2").startObject().field("field2_1", "green").field("field2_2", 6).endObject().startObject().field("field2_1", "blue").field("field2_2", 1).endObject().endArray().endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1_1", "2").startArray("nested2").startObject().field("field2_1", "yellow").field("field2_2", 10).endObject().startObject().field("field2_1", "green").field("field2_2", 8).endObject().endArray().endObject()
                .startObject().field("field1_1", "1").startArray("nested2").startObject().field("field2_1", "blue").field("field2_2", 2).endObject().startObject().field("field2_1", "red").field("field2_2", 12).endObject().endArray().endObject()
                .endArray()
                .endObject()).execute().actionGet();

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery())
                .addFacet(FacetBuilders.termsStatsFacet("facet1").keyField("nested1.nested2.field2_1").valueField("nested1.nested2.field2_2").nested("nested1.nested2"))
                .addFacet(FacetBuilders.statisticalFacet("facet2").field("field2_2").nested("nested1.nested2"))
                .addFacet(FacetBuilders.statisticalFacet("facet2_blue").field("field2_2").nested("nested1.nested2")
                        .facetFilter(boolFilter().must(termFilter("field2_1", "blue"))))
                .execute().actionGet();

        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));

        TermsStatsFacet termsStatsFacet = searchResponse.getFacets().facet("facet1");
        assertThat(termsStatsFacet.getEntries().size(), equalTo(4));
        assertThat(termsStatsFacet.getEntries().get(0).getTerm().string(), equalTo("blue"));
        assertThat(termsStatsFacet.getEntries().get(0).getCount(), equalTo(3l));
        assertThat(termsStatsFacet.getEntries().get(0).getTotal(), equalTo(8d));
        assertThat(termsStatsFacet.getEntries().get(1).getTerm().string(), equalTo("yellow"));
        assertThat(termsStatsFacet.getEntries().get(1).getCount(), equalTo(2l));
        assertThat(termsStatsFacet.getEntries().get(1).getTotal(), equalTo(13d));
        assertThat(termsStatsFacet.getEntries().get(2).getTerm().string(), equalTo("green"));
        assertThat(termsStatsFacet.getEntries().get(2).getCount(), equalTo(2l));
        assertThat(termsStatsFacet.getEntries().get(2).getTotal(), equalTo(14d));
        assertThat(termsStatsFacet.getEntries().get(3).getTerm().string(), equalTo("red"));
        assertThat(termsStatsFacet.getEntries().get(3).getCount(), equalTo(1l));
        assertThat(termsStatsFacet.getEntries().get(3).getTotal(), equalTo(12d));

        StatisticalFacet statsFacet = searchResponse.getFacets().facet("facet2");
        assertThat(statsFacet.getCount(), equalTo(8l));
        assertThat(statsFacet.getMin(), equalTo(1d));
        assertThat(statsFacet.getMax(), equalTo(12d));
        assertThat(statsFacet.getTotal(), equalTo(47d));

        StatisticalFacet blueFacet = searchResponse.getFacets().facet("facet2_blue");
        assertThat(blueFacet.getCount(), equalTo(3l));
        assertThat(blueFacet.getMin(), equalTo(1d));
        assertThat(blueFacet.getMax(), equalTo(5d));
        assertThat(blueFacet.getTotal(), equalTo(8d));

        // test scope ones (collector based)
        searchResponse = client().prepareSearch("test")
                .setQuery(
                        nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2_1", "blue"))
                )
                .addFacet(
                        FacetBuilders.termsStatsFacet("facet1")
                                .keyField("nested1.nested2.field2_1")
                                .valueField("nested1.nested2.field2_2")
                                .nested("nested1.nested2")
                                        // Maybe remove the `join` option?
                                        // The following also works:
                                        // .facetFilter(termFilter("nested1.nested2.field2_1", "blue"))
                                .facetFilter(nestedFilter("nested1.nested2", termFilter("nested1.nested2.field2_1", "blue")).join(false))
                )
                .execute().actionGet();

        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));

        termsStatsFacet = searchResponse.getFacets().facet("facet1");
        assertThat(termsStatsFacet.getEntries().size(), equalTo(1));
        assertThat(termsStatsFacet.getEntries().get(0).getTerm().string(), equalTo("blue"));
        assertThat(termsStatsFacet.getEntries().get(0).getCount(), equalTo(3l));
        assertThat(termsStatsFacet.getEntries().get(0).getTotal(), equalTo(8d));

        // test scope ones (post based)
        searchResponse = client().prepareSearch("test")
                .setQuery(
                        nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2_1", "blue"))
                )
                .addFacet(
                        FacetBuilders.filterFacet("facet1")
                                .global(true)
                                .filter(rangeFilter("nested1.nested2.field2_2").gte(0).lte(2))
                                .nested("nested1.nested2")
                                .facetFilter(nestedFilter("nested1.nested2", termFilter("nested1.nested2.field2_1", "blue")).join(false))
                )
                .execute().actionGet();

        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));

        FilterFacet filterFacet = searchResponse.getFacets().facet("facet1");
        assertThat(filterFacet.getCount(), equalTo(2l));
        assertThat(filterFacet.getName(), equalTo("facet1"));
    }

    @Test
    // When IncludeNestedDocsQuery is wrapped in a FilteredQuery then a in-finite loop occurs b/c of a bug in IncludeNestedDocsQuery#advance()
    // This IncludeNestedDocsQuery also needs to be aware of the filter from alias
    public void testDeleteNestedDocsWithAlias() throws Exception {

        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder().put(indexSettings()).put("index.referesh_interval", -1).build())
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject().endObject().endObject()));

        client().admin().indices().prepareAliases()
                .addAlias("test", "alias1", FilterBuilders.termFilter("field1", "value1")).execute().actionGet();

        ensureGreen();


        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested1")
                .startObject()
                .field("n_field1", "n_value1_1")
                .field("n_field2", "n_value2_1")
                .endObject()
                .startObject()
                .field("n_field1", "n_value1_2")
                .field("n_field2", "n_value2_2")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();


        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("field1", "value2")
                .startArray("nested1")
                .startObject()
                .field("n_field1", "n_value1_1")
                .field("n_field2", "n_value2_1")
                .endObject()
                .startObject()
                .field("n_field1", "n_value1_2")
                .field("n_field2", "n_value2_2")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();

        flush();
        refresh();
        IndicesStatusResponse statusResponse = client().admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(6l));

        client().prepareDeleteByQuery("alias1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        flush();
        refresh();
        statusResponse = client().admin().indices().prepareStatus().execute().actionGet();

        // This must be 3, otherwise child docs aren't deleted.
        // If this is 5 then only the parent has been removed
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(3l));
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
    }

    @Test
    public void testExplain() throws Exception {

        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested1")
                .startObject()
                .field("n_field1", "n_value1")
                .endObject()
                .startObject()
                .field("n_field1", "n_value1")
                .endObject()
                .endArray()
                .endObject())
                .setRefresh(true)
                .execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1")).scoreMode("total"))
                .setExplain(true)
                .execute().actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        Explanation explanation = searchResponse.getHits().hits()[0].explanation();
        assertThat(explanation.getValue(), equalTo(2f));
        assertThat(explanation.getDescription(), equalTo("Score based on child doc range from 0 to 1"));
        // TODO: Enable when changes from BlockJoinQuery#explain are added to Lucene (Most likely version 4.2)
//        assertThat(explanation.getDetails().length, equalTo(2));
//        assertThat(explanation.getDetails()[0].getValue(), equalTo(1f));
//        assertThat(explanation.getDetails()[0].getDescription(), equalTo("Child[0]"));
//        assertThat(explanation.getDetails()[1].getValue(), equalTo(1f));
//        assertThat(explanation.getDetails()[1].getDescription(), equalTo("Child[1]"));
    }

    @Test
    public void testSimpleNestedSorting() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(indexSettings())
                        .put("index.refresh_interval", -1))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "long")
                        .field("store", "yes")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", 1)
                .startArray("nested1")
                .startObject()
                .field("field1", 5)
                .endObject()
                .startObject()
                .field("field1", 4)
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("field1", 2)
                .startArray("nested1")
                .startObject()
                .field("field1", 1)
                .endObject()
                .startObject()
                .field("field1", 2)
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("field1", 3)
                .startArray("nested1")
                .startObject()
                .field("field1", 3)
                .endObject()
                .startObject()
                .field("field1", 4)
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setTypes("type1")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.fieldSort("nested1.field1").order(SortOrder.ASC))
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("2"));
        assertThat(searchResponse.getHits().hits()[0].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[1].id(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[1].sortValues()[0].toString(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[2].id(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[2].sortValues()[0].toString(), equalTo("4"));

        searchResponse = client().prepareSearch("test")
                .setTypes("type1")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.fieldSort("nested1.field1").order(SortOrder.DESC))
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[0].sortValues()[0].toString(), equalTo("5"));
        assertThat(searchResponse.getHits().hits()[1].id(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[1].sortValues()[0].toString(), equalTo("4"));
        assertThat(searchResponse.getHits().hits()[2].id(), equalTo("2"));
        assertThat(searchResponse.getHits().hits()[2].sortValues()[0].toString(), equalTo("2"));

        searchResponse = client().prepareSearch("test")
                .setTypes("type1")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.scriptSort("_fields['nested1.field1'].value + 1", "number").setNestedPath("nested1").order(SortOrder.DESC))
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[0].sortValues()[0].toString(), equalTo("6.0"));
        assertThat(searchResponse.getHits().hits()[1].id(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[1].sortValues()[0].toString(), equalTo("5.0"));
        assertThat(searchResponse.getHits().hits()[2].id(), equalTo("2"));
        assertThat(searchResponse.getHits().hits()[2].sortValues()[0].toString(), equalTo("3.0"));

        searchResponse = client().prepareSearch("test")
                .setTypes("type1")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.scriptSort("_fields['nested1.field1'].value + 1", "number").setNestedPath("nested1").sortMode("sum").order(SortOrder.DESC))
                .execute().actionGet();

        // B/c of sum it is actually +2
        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[0].sortValues()[0].toString(), equalTo("11.0"));
        assertThat(searchResponse.getHits().hits()[1].id(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[1].sortValues()[0].toString(), equalTo("9.0"));
        assertThat(searchResponse.getHits().hits()[2].id(), equalTo("2"));
        assertThat(searchResponse.getHits().hits()[2].sortValues()[0].toString(), equalTo("5.0"));

        searchResponse = client().prepareSearch("test")
                .setTypes("type1")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.scriptSort("_fields['nested1.field1'].value", "number")
                        .setNestedFilter(rangeFilter("nested1.field1").from(1).to(3))
                        .setNestedPath("nested1").sortMode("avg").order(SortOrder.DESC))
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[0].sortValues()[0].toString(), equalTo(Double.toString(Double.MAX_VALUE)));
        assertThat(searchResponse.getHits().hits()[1].id(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[1].sortValues()[0].toString(), equalTo("3.0"));
        assertThat(searchResponse.getHits().hits()[2].id(), equalTo("2"));
        assertThat(searchResponse.getHits().hits()[2].sortValues()[0].toString(), equalTo("1.5"));

        searchResponse = client().prepareSearch("test")
                .setTypes("type1")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.scriptSort("_fields['nested1.field1'].value", "string")
                        .setNestedPath("nested1").order(SortOrder.DESC))
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[0].sortValues()[0].toString(), equalTo("5"));
        assertThat(searchResponse.getHits().hits()[1].id(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[1].sortValues()[0].toString(), equalTo("4"));
        assertThat(searchResponse.getHits().hits()[2].id(), equalTo("2"));
        assertThat(searchResponse.getHits().hits()[2].sortValues()[0].toString(), equalTo("2"));

        searchResponse = client().prepareSearch("test")
                .setTypes("type1")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.scriptSort("_fields['nested1.field1'].value", "string")
                        .setNestedPath("nested1").order(SortOrder.ASC))
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("2"));
        assertThat(searchResponse.getHits().hits()[0].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[1].id(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[1].sortValues()[0].toString(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[2].id(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[2].sortValues()[0].toString(), equalTo("4"));

        try {
            client().prepareSearch("test")
                    .setTypes("type1")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.scriptSort("_fields['nested1.field1'].value", "string")
                            .setNestedPath("nested1").sortMode("sum").order(SortOrder.ASC))
                    .execute().actionGet();
            Assert.fail("SearchPhaseExecutionException should have been thrown");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), containsString("type [string] doesn't support mode [SUM]"));
        }
    }

    @Test
    public void testSimpleNestedSorting_withNestedFilterMissing() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(indexSettings())
                        .put("index.referesh_interval", -1))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                            .startObject("properties")
                                .startObject("field1")
                                    .field("type", "long")
                                .endObject()
                                .startObject("field2")
                                    .field("type", "boolean")
                                .endObject()
                            .endObject()
                        .endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", 1)
                .startArray("nested1")
                .startObject()
                .field("field1", 5)
                .field("field2", true)
                .endObject()
                .startObject()
                .field("field1", 4)
                .field("field2", true)
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("field1", 2)
                .startArray("nested1")
                .startObject()
                .field("field1", 1)
                .field("field2", true)
                .endObject()
                .startObject()
                .field("field1", 2)
                .field("field2", true)
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();
        // Doc with missing nested docs if nested filter is used
        refresh();
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("field1", 3)
                .startArray("nested1")
                .startObject()
                .field("field1", 3)
                .field("field2", false)
                .endObject()
                .startObject()
                .field("field1", 4)
                .field("field2", false)
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();
        refresh();

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("test").setTypes("type1")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.fieldSort("nested1.field1").setNestedFilter(termFilter("nested1.field2", true)).missing(10).order(SortOrder.ASC));

        if (randomBoolean()) {
            searchRequestBuilder.setScroll("10m");
        }

        SearchResponse searchResponse = searchRequestBuilder.get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("2"));
        assertThat(searchResponse.getHits().hits()[0].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[1].id(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[1].sortValues()[0].toString(), equalTo("4"));
        assertThat(searchResponse.getHits().hits()[2].id(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[2].sortValues()[0].toString(), equalTo("10"));

        searchRequestBuilder = client().prepareSearch("test").setTypes("type1").setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.fieldSort("nested1.field1").setNestedFilter(termFilter("nested1.field2", true)).missing(10).order(SortOrder.DESC));

        if (randomBoolean()) {
            searchRequestBuilder.setScroll("10m");
        }

        searchResponse = searchRequestBuilder.get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().hits()[0].id(), equalTo("3"));
        assertThat(searchResponse.getHits().hits()[0].sortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().hits()[1].id(), equalTo("1"));
        assertThat(searchResponse.getHits().hits()[1].sortValues()[0].toString(), equalTo("5"));
        assertThat(searchResponse.getHits().hits()[2].id(), equalTo("2"));
        assertThat(searchResponse.getHits().hits()[2].sortValues()[0].toString(), equalTo("2"));
        client().prepareClearScroll().addScrollId("_all").get();
    }

    @Test
    public void testSortNestedWithNestedFilter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject()
                        .startObject("type1")
                        .startObject("properties")
                        .startObject("grand_parent_values").field("type", "long").endObject()
                        .startObject("parent").field("type", "nested")
                        .startObject("properties")
                        .startObject("parent_values").field("type", "long").endObject()
                        .startObject("child").field("type", "nested")
                        .startObject("properties")
                        .startObject("child_values").field("type", "long").endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()));
        ensureGreen();

        // sum: 11
        client().prepareIndex("test", "type1", Integer.toString(1)).setSource(jsonBuilder().startObject()
                .field("grand_parent_values", 1l)
                .startObject("parent")
                .field("filter", false)
                .field("parent_values", 1l)
                .startObject("child")
                .field("filter", true)
                .field("child_values", 1l)
                .startObject("child_obj")
                .field("value", 1l)
                .endObject()
                .endObject()
                .startObject("child")
                .field("filter", false)
                .field("child_values", 6l)
                .endObject()
                .endObject()
                .startObject("parent")
                .field("filter", true)
                .field("parent_values", 2l)
                .startObject("child")
                .field("filter", false)
                .field("child_values", -1l)
                .endObject()
                .startObject("child")
                .field("filter", false)
                .field("child_values", 5l)
                .endObject()
                .endObject()
                .endObject()).execute().actionGet();

        // sum: 7
        client().prepareIndex("test", "type1", Integer.toString(2)).setSource(jsonBuilder().startObject()
                .field("grand_parent_values", 2l)
                .startObject("parent")
                .field("filter", false)
                .field("parent_values", 2l)
                .startObject("child")
                .field("filter", true)
                .field("child_values", 2l)
                .startObject("child_obj")
                .field("value", 2l)
                .endObject()
                .endObject()
                .startObject("child")
                .field("filter", false)
                .field("child_values", 4l)
                .endObject()
                .endObject()
                .startObject("parent")
                .field("parent_values", 3l)
                .field("filter", true)
                .startObject("child")
                .field("child_values", -2l)
                .field("filter", false)
                .endObject()
                .startObject("child")
                .field("filter", false)
                .field("child_values", 3l)
                .endObject()
                .endObject()
                .endObject()).execute().actionGet();

        // sum: 2
        client().prepareIndex("test", "type1", Integer.toString(3)).setSource(jsonBuilder().startObject()
                .field("grand_parent_values", 3l)
                .startObject("parent")
                .field("parent_values", 3l)
                .field("filter", false)
                .startObject("child")
                .field("filter", true)
                .field("child_values", 3l)
                .startObject("child_obj")
                .field("value", 3l)
                .endObject()
                .endObject()
                .startObject("child")
                .field("filter", false)
                .field("child_values", 1l)
                .endObject()
                .endObject()
                .startObject("parent")
                .field("parent_values", 4l)
                .field("filter", true)
                .startObject("child")
                .field("filter", false)
                .field("child_values", -3l)
                .endObject()
                .startObject("child")
                .field("filter", false)
                .field("child_values", 1l)
                .endObject()
                .endObject()
                .endObject()).execute().actionGet();
        refresh();

        // Without nested filter
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_values")
                                .setNestedPath("parent.child")
                                .order(SortOrder.ASC)
                )
                .execute().actionGet();
        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("-3"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("-2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("-1"));

        // With nested filter
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_values")
                                .setNestedPath("parent.child")
                                .setNestedFilter(FilterBuilders.termFilter("parent.child.filter", true))
                                .order(SortOrder.ASC)
                )
                .execute().actionGet();
        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("3"));

        // Nested path should be automatically detected, expect same results as above search request
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_values")
                                .setNestedFilter(FilterBuilders.termFilter("parent.child.filter", true))
                                .order(SortOrder.ASC)
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("3"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.parent_values")
                                .setNestedPath("parent.child")
                                .setNestedFilter(FilterBuilders.termFilter("parent.filter", false))
                                .order(SortOrder.ASC)
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("3"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_values")
                                .setNestedPath("parent.child")
                                .setNestedFilter(FilterBuilders.termFilter("parent.filter", false))
                                .order(SortOrder.ASC)
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        // TODO: If we expose ToChildBlockJoinQuery we can filter sort values based on a higher level nested objects
//        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
//        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("-3"));
//        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
//        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("-2"));
//        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
//        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("-1"));

        // Check if closest nested type is resolved
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_obj.value")
                                .setNestedFilter(FilterBuilders.termFilter("parent.child.filter", true))
                                .order(SortOrder.ASC)
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("3"));

        // Sort mode: sum
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_values")
                                .setNestedPath("parent.child")
                                .sortMode("sum")
                                .order(SortOrder.ASC)
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("7"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("11"));


        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_values")
                                .setNestedPath("parent.child")
                                .sortMode("sum")
                                .order(SortOrder.DESC)
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("11"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("7"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("2"));

        // Sort mode: sum with filter
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_values")
                                .setNestedPath("parent.child")
                                .setNestedFilter(FilterBuilders.termFilter("parent.child.filter", true))
                                .sortMode("sum")
                                .order(SortOrder.ASC)
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("3"));

        // Sort mode: avg
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_values")
                                .setNestedPath("parent.child")
                                .sortMode("avg")
                                .order(SortOrder.ASC)
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("0"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("2"));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_values")
                                .setNestedPath("parent.child")
                                .sortMode("avg")
                                .order(SortOrder.DESC)
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("0"));

        // Sort mode: avg with filter
        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(
                        SortBuilders.fieldSort("parent.child.child_values")
                                .setNestedPath("parent.child")
                                .setNestedFilter(FilterBuilders.termFilter("parent.child.filter", true))
                                .sortMode("avg")
                                .order(SortOrder.ASC)
                )
                .execute().actionGet();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].sortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].sortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].sortValues()[0].toString(), equalTo("3"));
    }

}
