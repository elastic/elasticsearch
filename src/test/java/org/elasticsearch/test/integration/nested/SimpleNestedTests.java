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

package org.elasticsearch.test.integration.nested;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@Test
public class SimpleNestedTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = client("node1");
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @Test
    public void simpleNested() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();


        // check on no data, see it works
        SearchResponse searchResponse = client.prepareSearch("test").setQuery(termQuery("_all", "n_value1_1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));
        searchResponse = client.prepareSearch("test").setQuery(termQuery("nested1.n_field1", "n_value1_1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
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

        // flush, so we fetch it from the index (as see that we filter nested docs)
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        GetResponse getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));
        assertThat(getResponse.source(), notNullValue());

        // check the numDocs
        IndicesStatusResponse statusResponse = client.admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(3l));

        // check that _all is working on nested docs
        searchResponse = client.prepareSearch("test").setQuery(termQuery("_all", "n_value1_1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client.prepareSearch("test").setQuery(termQuery("nested1.n_field1", "n_value1_1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        // search for something that matches the nested doc, and see that we don't find the nested doc
        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery()).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        searchResponse = client.prepareSearch("test").setQuery(termQuery("nested1.n_field1", "n_value1_1")).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        // now, do a nested query
        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"))).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // add another doc, one that would match if it was not nested...

        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
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

        // flush, so we fetch it from the index (as see that we filter nested docs)
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        statusResponse = client.admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(6l));

        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1")))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // filter
        searchResponse = client.prepareSearch("test").setQuery(filteredQuery(matchAllQuery(), nestedFilter("nested1",
                boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1"))))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // check with type prefix
        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("type1.nested1",
                boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1")))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // check delete, so all is gone...
        DeleteResponse deleteResponse = client.prepareDelete("test", "type1", "2").execute().actionGet();
        assertThat(deleteResponse.notFound(), equalTo(false));

        // flush, so we fetch it from the index (as see that we filter nested docs)
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        statusResponse = client.admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(3l));

        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
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
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.referesh_interval", -1).build())
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();


        for (int i = 0; i < total; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
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


        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        IndicesStatusResponse statusResponse = client.admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(total * 3l));

        client.prepareDeleteByQuery("test").setQuery(QueryBuilders.idsQuery("type1").ids(Integer.toString(docToDelete))).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        statusResponse = client.admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo((total * 3l) - 3));

        for (int i = 0; i < total; i++) {
            assertThat(client.prepareGet("test", "type1", Integer.toString(i)).execute().actionGet().exists(), equalTo(i != docToDelete));
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
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.referesh_interval", -1).build())
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();


        for (int i = 0; i < total; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("field1", "value1")
                    .endObject()).execute().actionGet();
        }


        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        IndicesStatusResponse statusResponse = client.admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(total));

        client.prepareDeleteByQuery("test").setQuery(QueryBuilders.idsQuery("type1").ids(Integer.toString(docToDelete))).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        statusResponse = client.admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo((total) - 1));

        for (int i = 0; i < total; i++) {
            assertThat(client.prepareGet("test", "type1", Integer.toString(i)).execute().actionGet().exists(), equalTo(i != docToDelete));
        }
    }

    @Test
    public void multiNested() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested").startObject("properties")
                        .startObject("nested2").field("type", "nested").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1", "1").startArray("nested2").startObject().field("field2", "2").endObject().startObject().field("field2", "3").endObject().endArray().endObject()
                .startObject().field("field1", "4").startArray("nested2").startObject().field("field2", "5").endObject().startObject().field("field2", "6").endObject().endArray().endObject()
                .endArray()
                .endObject()).execute().actionGet();

        // flush, so we fetch it from the index (as see that we filter nested docs)
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        GetResponse getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));

        // check the numDocs
        IndicesStatusResponse statusResponse = client.admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(7l));

        // do some multi nested queries
        SearchResponse searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1",
                termQuery("nested1.field1", "1"))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1.nested2",
                termQuery("nested1.nested2.field2", "2"))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "1")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "2"))))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "1")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "3"))))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "1")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "4"))))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "1")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "5"))))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));

        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "4")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "5"))))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        searchResponse = client.prepareSearch("test").setQuery(nestedQuery("nested1",
                boolQuery().must(termQuery("nested1.field1", "4")).must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "2"))))).execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(0l));
    }

    @Test
    public void testFacetsSingleShard() throws Exception {
        testFacets(1);
    }

    @Test
    public void testFacetsMultiShards() throws Exception {
        testFacets(3);
    }

    private void testFacets(int numberOfShards) throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", numberOfShards))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested").startObject("properties")
                        .startObject("nested2").field("type", "nested").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1_1", "1").startArray("nested2").startObject().field("field2_1", "blue").field("field2_2", 5).endObject().startObject().field("field2_1", "yellow").field("field2_2", 3).endObject().endArray().endObject()
                .startObject().field("field1_1", "4").startArray("nested2").startObject().field("field2_1", "green").field("field2_2", 6).endObject().startObject().field("field2_1", "blue").field("field2_2", 1).endObject().endArray().endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder()
                .startObject()
                .field("field", "value")
                .startArray("nested1")
                .startObject().field("field1_1", "2").startArray("nested2").startObject().field("field2_1", "yellow").field("field2_2", 10).endObject().startObject().field("field2_1", "green").field("field2_2", 8).endObject().endArray().endObject()
                .startObject().field("field1_1", "1").startArray("nested2").startObject().field("field2_1", "blue").field("field2_2", 2).endObject().startObject().field("field2_1", "red").field("field2_2", 12).endObject().endArray().endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch("test").setQuery(matchAllQuery())
                .addFacet(FacetBuilders.termsStatsFacet("facet1").keyField("nested1.nested2.field2_1").valueField("nested1.nested2.field2_2").nested("nested1.nested2"))
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
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

        // test scope ones
//        searchResponse = client.prepareSearch("test")
//                .setQuery(
//                        nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2_1", "blue"))
//                )
//                .addFacet(
//                        FacetBuilders.termsStatsFacet("facet1")
//                                .keyField("nested1.nested2.field2_1")
//                                .valueField("nested1.nested2.field2_2")
//                                .nested("nested1.nested2")
//                                .facetFilter(nestedFilter("nested1.nested2", termQuery("nested1.nested2.field2_1", "blue")).join(false))
//                )
//                .execute().actionGet();
//
//        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));
//        assertThat(searchResponse.hits().totalHits(), equalTo(2l));
//
//        termsStatsFacet = searchResponse.facets().facet("facet1");
//        assertThat(termsStatsFacet.getEntries().size(), equalTo(1));
//        assertThat(termsStatsFacet.getEntries().get(0).getTerm().string(), equalTo("blue"));
//        assertThat(termsStatsFacet.getEntries().get(0).getCount(), equalTo(3l));
//        assertThat(termsStatsFacet.getEntries().get(0).getTotal(), equalTo(8d));
    }

    @Test
    // When IncludeNestedDocsQuery is wrapped in a FilteredQuery then a in-finite loop occurs b/c of a bug in IncludeNestedDocsQuery#advance()
    // This IncludeNestedDocsQuery also needs to be aware of the filter from alias
    public void testDeleteNestedDocsWithAlias() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.referesh_interval", -1).build())
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().indices().prepareAliases()
                .addAlias("test", "alias1", FilterBuilders.termFilter("field1", "value1")).execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();


        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
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


        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
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

        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        IndicesStatusResponse statusResponse = client.admin().indices().prepareStatus().execute().actionGet();
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(6l));

        client.prepareDeleteByQuery("alias1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        client.admin().indices().prepareFlush().setRefresh(true).execute().actionGet();
        statusResponse = client.admin().indices().prepareStatus().execute().actionGet();

        // This must be 3, otherwise child docs aren't deleted.
        // If this is 5 then only the parent has been removed
        assertThat(statusResponse.getIndex("test").getDocs().getNumDocs(), equalTo(3l));
        assertThat(client.prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(false));
    }

    @Test
    public void testExplain() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
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

        SearchResponse searchResponse = client.prepareSearch("test")
                .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1")).scoreMode("total"))
                .setExplain(true)
                .execute().actionGet();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));
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

}