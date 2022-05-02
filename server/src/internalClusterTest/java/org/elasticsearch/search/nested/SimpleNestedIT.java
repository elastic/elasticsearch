/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.nested;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class SimpleNestedIT extends ESIntegTestCase {
    public void testSimpleNested() throws Exception {
        assertAcked(prepareCreate("test").setMapping("nested1", "type=nested"));
        ensureGreen();

        // check on no data, see it works
        SearchResponse searchResponse = client().prepareSearch("test").get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("n_field1", "n_value1_1")).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
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
                    .endObject()
            )
            .get();

        waitForRelocation(ClusterHealthStatus.GREEN);
        GetResponse getResponse = client().prepareGet("test", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getSourceAsBytes(), notNullValue());
        refresh();
        // check the numDocs
        assertDocumentCount("test", 3);

        searchResponse = client().prepareSearch("test").setQuery(termQuery("n_field1", "n_value1_1")).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        // search for something that matches the nested doc, and see that we don't find the nested doc
        searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch("test").setQuery(termQuery("n_field1", "n_value1_1")).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        // now, do a nested query
        searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"), ScoreMode.Avg))
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"), ScoreMode.Avg))
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        // add another doc, one that would match if it was not nested...

        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
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
                    .endObject()
            )
            .get();
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
        assertDocumentCount("test", 6);

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1")),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        // filter
        searchResponse = client().prepareSearch("test")
            .setQuery(
                boolQuery().must(matchAllQuery())
                    .mustNot(
                        nestedQuery(
                            "nested1",
                            boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1")),
                            ScoreMode.Avg
                        )
                    )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        // check with type prefix
        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.n_field1", "n_value1_1")).must(termQuery("nested1.n_field2", "n_value2_1")),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        // check delete, so all is gone...
        DeleteResponse deleteResponse = client().prepareDelete("test", "2").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());

        refresh();
        assertDocumentCount("test", 3);

        searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"), ScoreMode.Avg))
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testMultiNested() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("nested1")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("nested2")
                    .field("type", "nested")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        ensureGreen();
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field", "value")
                    .startArray("nested1")
                    .startObject()
                    .field("field1", "1")
                    .startArray("nested2")
                    .startObject()
                    .field("field2", "2")
                    .endObject()
                    .startObject()
                    .field("field2", "3")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("field1", "4")
                    .startArray("nested2")
                    .startObject()
                    .field("field2", "5")
                    .endObject()
                    .startObject()
                    .field("field2", "6")
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        GetResponse getResponse = client().prepareGet("test", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
        // check the numDocs
        assertDocumentCount("test", 7);

        // do some multi nested queries
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.field1", "1"), ScoreMode.Avg))
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "2"), ScoreMode.Avg))
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "1"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "2"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "1"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "3"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "1"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "4"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "1"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "5"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "4"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "5"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("test")
            .setQuery(
                nestedQuery(
                    "nested1",
                    boolQuery().must(termQuery("nested1.field1", "4"))
                        .must(nestedQuery("nested1.nested2", termQuery("nested1.nested2.field2", "2"), ScoreMode.Avg)),
                    ScoreMode.Avg
                )
            )
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
    }

    // When IncludeNestedDocsQuery is wrapped in a FilteredQuery then a in-finite loop occurs b/c of a bug in
    // IncludeNestedDocsQuery#advance()
    // This IncludeNestedDocsQuery also needs to be aware of the filter from alias
    public void testDeleteNestedDocsWithAlias() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.refresh_interval", -1).build())
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "text")
                        .endObject()
                        .startObject("nested1")
                        .field("type", "nested")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        client().admin().indices().prepareAliases().addAlias("test", "alias1", QueryBuilders.termQuery("field1", "value1")).get();

        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
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
                    .endObject()
            )
            .get();

        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
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
                    .endObject()
            )
            .get();

        flush();
        refresh();
        assertDocumentCount("test", 6);
    }

    public void testExplain() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("nested1")
                    .field("type", "nested")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value1")
                    .startArray("nested1")
                    .startObject()
                    .field("n_field1", "n_value1")
                    .endObject()
                    .startObject()
                    .field("n_field1", "n_value1")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1"), ScoreMode.Total))
            .setExplain(true)
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        Explanation explanation = searchResponse.getHits().getHits()[0].getExplanation();
        assertThat(explanation.getValue(), equalTo(searchResponse.getHits().getHits()[0].getScore()));
        assertThat(explanation.toString(), startsWith("0.36464313 = Score based on 2 child docs in range from 0 to 1"));
    }

    public void testSimpleNestedSorting() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.refresh_interval", -1))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "long")
                        .field("store", true)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", 1)
                    .startArray("nested1")
                    .startObject()
                    .field("field1", 5)
                    .endObject()
                    .startObject()
                    .field("field1", 4)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", 2)
                    .startArray("nested1")
                    .startObject()
                    .field("field1", 1)
                    .endObject()
                    .startObject()
                    .field("field1", 2)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        client().prepareIndex("test")
            .setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", 3)
                    .startArray("nested1")
                    .startObject()
                    .field("field1", 3)
                    .endObject()
                    .startObject()
                    .field("field1", 4)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")

            .setQuery(QueryBuilders.matchAllQuery())
            .addSort(SortBuilders.fieldSort("nested1.field1").order(SortOrder.ASC).setNestedSort(new NestedSortBuilder("nested1")))
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("4"));

        searchResponse = client().prepareSearch("test")

            .setQuery(QueryBuilders.matchAllQuery())
            .addSort(SortBuilders.fieldSort("nested1.field1").order(SortOrder.DESC).setNestedSort(new NestedSortBuilder("nested1")))
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("5"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("4"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("2"));
    }

    public void testSimpleNestedSortingWithNestedFilterMissing() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.refresh_interval", -1))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("properties")
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
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
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
                    .endObject()
            )
            .get();
        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
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
                    .endObject()
            )
            .get();
        // Doc with missing nested docs if nested filter is used
        refresh();
        client().prepareIndex("test")
            .setId("3")
            .setSource(
                jsonBuilder().startObject()
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
                    .endObject()
            )
            .get();
        refresh();

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("nested1.field1")
                    .setNestedSort(new NestedSortBuilder("nested1").setFilter(termQuery("nested1.field2", true)))
                    .missing(10)
                    .order(SortOrder.ASC)
            );

        if (randomBoolean()) {
            searchRequestBuilder.setScroll("10m");
        }

        SearchResponse searchResponse = searchRequestBuilder.get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("4"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("10"));

        searchRequestBuilder = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("nested1.field1")
                    .setNestedSort(new NestedSortBuilder("nested1").setFilter(termQuery("nested1.field2", true)))
                    .missing(10)
                    .order(SortOrder.DESC)
            );

        if (randomBoolean()) {
            searchRequestBuilder.setScroll("10m");
        }

        searchResponse = searchRequestBuilder.get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("5"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("2"));
        client().prepareClearScroll().addScrollId("_all").get();
    }

    public void testNestedSortWithMultiLevelFiltering() throws Exception {
        assertAcked(prepareCreate("test").setMapping("""
            {
                "properties": {
                  "acl": {
                    "type": "nested",
                    "properties": {
                      "access_id": {"type": "keyword"},
                      "operation": {
                        "type": "nested",
                        "properties": {
                          "name": {"type": "keyword"},
                          "user": {
                            "type": "nested",
                            "properties": {
                              "username": {"type": "keyword"},
                              "id": {"type": "integer"}
                            }
                          }
                        }
                      }
                    }
                  }
                }
            }"""));
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("""
            {
              "acl": [
                {
                  "access_id": 1,
                  "operation": [
                    {
                      "name": "read",
                      "user": [
                        {"username": "matt", "id": 1},
                        {"username": "shay", "id": 2},
                        {"username": "adrien", "id": 3}
                      ]
                    },
                    {
                      "name": "write",
                      "user": [
                        {"username": "shay", "id": 2},
                        {"username": "adrien", "id": 3}
                      ]
                    }
                  ]
                },
                {
                  "access_id": 2,
                  "operation": [
                    {
                      "name": "read",
                      "user": [
                        {"username": "jim", "id": 4},
                        {"username": "shay", "id": 2}
                      ]
                    },
                    {
                      "name": "write",
                      "user": [
                        {"username": "shay", "id": 2}
                      ]
                    },
                    {
                      "name": "execute",
                      "user": [
                        {"username": "shay", "id": 2}
                      ]
                    }
                  ]
                }
              ]
            }""", XContentType.JSON).get();

        client().prepareIndex("test").setId("2").setSource("""
            {
              "acl": [
                {
                  "access_id": 1,
                  "operation": [
                    {
                      "name": "read",
                      "user": [
                        {"username": "matt", "id": 1},
                        {"username": "luca", "id": 5}
                      ]
                    },
                    {
                      "name": "execute",
                      "user": [
                        {"username": "luca", "id": 5}
                      ]
                    }
                  ]
                },
                {
                  "access_id": 3,
                  "operation": [
                    {
                      "name": "read",
                      "user": [
                        {"username": "matt", "id": 1}
                      ]
                    },
                    {
                      "name": "write",
                      "user": [
                        {"username": "matt", "id": 1}
                      ]
                    },
                    {
                      "name": "execute",
                      "user": [
                        {"username": "matt", "id": 1}
                      ]
                    }
                  ]
                }
              ]
            }""", XContentType.JSON).get();
        refresh();

        // access id = 1, read, max value, asc, should use matt and shay
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("acl.operation.user.username")
                    .setNestedSort(
                        new NestedSortBuilder("acl").setFilter(QueryBuilders.termQuery("acl.access_id", "1"))
                            .setNestedSort(
                                new NestedSortBuilder("acl.operation").setFilter(QueryBuilders.termQuery("acl.operation.name", "read"))
                                    .setNestedSort(new NestedSortBuilder("acl.operation.user"))
                            )
                    )
                    .sortMode(SortMode.MAX)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("matt"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("shay"));

        // access id = 1, read, min value, asc, should now use adrien and luca
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("acl.operation.user.username")
                    .setNestedSort(
                        new NestedSortBuilder("acl").setFilter(QueryBuilders.termQuery("acl.access_id", "1"))
                            .setNestedSort(
                                new NestedSortBuilder("acl.operation").setFilter(QueryBuilders.termQuery("acl.operation.name", "read"))
                                    .setNestedSort(new NestedSortBuilder("acl.operation.user"))
                            )
                    )
                    .sortMode(SortMode.MIN)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("adrien"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("luca"));

        // execute, by matt or luca, by user id, sort missing first
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("acl.operation.user.id")
                    .setNestedSort(
                        new NestedSortBuilder("acl").setNestedSort(
                            new NestedSortBuilder("acl.operation").setFilter(QueryBuilders.termQuery("acl.operation.name", "execute"))
                                .setNestedSort(
                                    new NestedSortBuilder("acl.operation.user").setFilter(
                                        QueryBuilders.termsQuery("acl.operation.user.username", "matt", "luca")
                                    )
                                )
                        )
                    )
                    .missing("_first")
                    .sortMode(SortMode.MIN)
                    .order(SortOrder.DESC)
            )
            .get();

        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1")); // missing first
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("1"));

        // execute, by matt or luca, by username, sort missing last (default)
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("acl.operation.user.username")
                    .setNestedSort(
                        new NestedSortBuilder("acl").setNestedSort(
                            new NestedSortBuilder("acl.operation").setFilter(QueryBuilders.termQuery("acl.operation.name", "execute"))
                                .setNestedSort(
                                    new NestedSortBuilder("acl.operation.user").setFilter(
                                        QueryBuilders.termsQuery("acl.operation.user.username", "matt", "luca")
                                    )
                                )
                        )
                    )
                    .sortMode(SortMode.MIN)
                    .order(SortOrder.DESC)
            )
            .get();

        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("luca"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("1")); // missing last
    }

    // https://github.com/elastic/elasticsearch/issues/31554
    public void testLeakingSortValues() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", 1)).setMapping("""
            {
              "_doc": {
                "dynamic": "strict",
                "properties": {
                  "nested1": {
                    "type": "nested",
                    "properties": {
                      "nested2": {
                        "type": "nested",
                        "properties": {
                          "nested2_keyword": {
                            "type": "keyword"
                          },
                          "sortVal": {
                            "type": "integer"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """));
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("""
            {
              "nested1": [
                {
                  "nested2": [
                    {
                      "nested2_keyword": "nested2_bar",
                      "sortVal": 1
                    }
                  ]
                }
             ]
            }""", XContentType.JSON).get();

        client().prepareIndex("test").setId("2").setSource("""
            {
              "nested1": [
                {
                  "nested2": [
                    {
                      "nested2_keyword": "nested2_bar",
                      "sortVal": 2
                    }
                  ]
                }
              ]
            }""", XContentType.JSON).get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(termQuery("_id", 2))
            .addSort(
                SortBuilders.fieldSort("nested1.nested2.sortVal")
                    .setNestedSort(
                        new NestedSortBuilder("nested1").setNestedSort(
                            new NestedSortBuilder("nested1.nested2").setFilter(termQuery("nested1.nested2.nested2_keyword", "nested2_bar"))
                        )
                    )
            )
            .get();

        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("2"));

    }

    public void testSortNestedWithNestedFilter() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("grand_parent_values")
                    .field("type", "long")
                    .endObject()
                    .startObject("parent")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("parent_values")
                    .field("type", "long")
                    .endObject()
                    .startObject("child")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("child_values")
                    .field("type", "long")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        // sum: 11
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("grand_parent_values", 1L)
                    .startArray("parent")
                    .startObject()
                    .field("filter", false)
                    .field("parent_values", 1L)
                    .startArray("child")
                    .startObject()
                    .field("filter", true)
                    .field("child_values", 1L)
                    .startObject("child_obj")
                    .field("value", 1L)
                    .endObject()
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 6L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("filter", true)
                    .field("parent_values", 2L)
                    .startArray("child")
                    .startObject()
                    .field("filter", false)
                    .field("child_values", -1L)
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 5L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        // sum: 7
        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("grand_parent_values", 2L)
                    .startArray("parent")
                    .startObject()
                    .field("filter", false)
                    .field("parent_values", 2L)
                    .startArray("child")
                    .startObject()
                    .field("filter", true)
                    .field("child_values", 2L)
                    .startObject("child_obj")
                    .field("value", 2L)
                    .endObject()
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 4L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("parent_values", 3L)
                    .field("filter", true)
                    .startArray("child")
                    .startObject()
                    .field("child_values", -2L)
                    .field("filter", false)
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 3L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        // sum: 2
        client().prepareIndex("test")
            .setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("grand_parent_values", 3L)
                    .startArray("parent")
                    .startObject()
                    .field("parent_values", 3L)
                    .field("filter", false)
                    .startArray("child")
                    .startObject()
                    .field("filter", true)
                    .field("child_values", 3L)
                    .startObject("child_obj")
                    .field("value", 3L)
                    .endObject()
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 1L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("parent_values", 4L)
                    .field("filter", true)
                    .startArray("child")
                    .startObject()
                    .field("filter", false)
                    .field("child_values", -3L)
                    .endObject()
                    .startObject()
                    .field("filter", false)
                    .field("child_values", 1L)
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();

        // Without nested filter
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedSort(new NestedSortBuilder("parent.child"))
                    .order(SortOrder.ASC)
            )
            .get();
        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("-3"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("-2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("-1"));

        // With nested filter
        NestedSortBuilder nestedSort = new NestedSortBuilder("parent.child");
        nestedSort.setFilter(QueryBuilders.termQuery("parent.child.filter", true));
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("parent.child.child_values").setNestedSort(nestedSort).order(SortOrder.ASC))
            .get();
        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        // Nested path should be automatically detected, expect same results as above search request
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("parent.child.child_values").setNestedSort(nestedSort).order(SortOrder.ASC))
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        nestedSort.setFilter(QueryBuilders.termQuery("parent.filter", false));
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("parent.parent_values").setNestedSort(nestedSort).order(SortOrder.ASC))
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedSort(
                        new NestedSortBuilder("parent").setFilter(QueryBuilders.termQuery("parent.filter", false))
                            .setNestedSort(new NestedSortBuilder("parent.child"))
                    )
                    .sortMode(SortMode.MAX)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("4"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("6"));

        // Check if closest nested type is resolved
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_obj.value")
                    .setNestedSort(new NestedSortBuilder("parent.child").setFilter(QueryBuilders.termQuery("parent.child.filter", true)))
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        // Sort mode: sum
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedSort(new NestedSortBuilder("parent.child"))
                    .sortMode(SortMode.SUM)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("7"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("11"));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedSort(new NestedSortBuilder("parent.child"))
                    .sortMode(SortMode.SUM)
                    .order(SortOrder.DESC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("11"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("7"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("2"));

        // Sort mode: sum with filter
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedSort(new NestedSortBuilder("parent.child").setFilter(QueryBuilders.termQuery("parent.child.filter", true)))
                    .sortMode(SortMode.SUM)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        // Sort mode: avg
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedSort(new NestedSortBuilder("parent.child"))
                    .sortMode(SortMode.AVG)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedSort(new NestedSortBuilder("parent.child"))
                    .sortMode(SortMode.AVG)
                    .order(SortOrder.DESC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("1"));

        // Sort mode: avg with filter
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("parent.child.child_values")
                    .setNestedSort(new NestedSortBuilder("parent.child").setFilter(QueryBuilders.termQuery("parent.child.filter", true)))
                    .sortMode(SortMode.AVG)
                    .order(SortOrder.ASC)
            )
            .get();

        assertHitCount(searchResponse, 3);
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getHits()[0].getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[0].getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getHits()[1].getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[1].getSortValues()[0].toString(), equalTo("2"));
        assertThat(searchResponse.getHits().getHits()[2].getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getHits()[2].getSortValues()[0].toString(), equalTo("3"));
    }

    // Issue #9305
    public void testNestedSortingWithNestedFilterAsFilter() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("officelocation")
                    .field("type", "text")
                    .endObject()
                    .startObject("users")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("first")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("last")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("workstations")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("stationid")
                    .field("type", "text")
                    .endObject()
                    .startObject("phoneid")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        IndexResponse indexResponse1 = client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("officelocation", "gendale")
                    .startArray("users")
                    .startObject()
                    .field("first", "fname1")
                    .field("last", "lname1")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s1")
                    .field("phoneid", "p1")
                    .endObject()
                    .startObject()
                    .field("stationid", "s2")
                    .field("phoneid", "p2")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("first", "fname2")
                    .field("last", "lname2")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s3")
                    .field("phoneid", "p3")
                    .endObject()
                    .startObject()
                    .field("stationid", "s4")
                    .field("phoneid", "p4")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("first", "fname3")
                    .field("last", "lname3")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s5")
                    .field("phoneid", "p5")
                    .endObject()
                    .startObject()
                    .field("stationid", "s6")
                    .field("phoneid", "p6")
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        assertTrue(indexResponse1.getShardInfo().getSuccessful() > 0);

        IndexResponse indexResponse2 = client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("officelocation", "gendale")
                    .startArray("users")
                    .startObject()
                    .field("first", "fname4")
                    .field("last", "lname4")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s1")
                    .field("phoneid", "p1")
                    .endObject()
                    .startObject()
                    .field("stationid", "s2")
                    .field("phoneid", "p2")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("first", "fname5")
                    .field("last", "lname5")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s3")
                    .field("phoneid", "p3")
                    .endObject()
                    .startObject()
                    .field("stationid", "s4")
                    .field("phoneid", "p4")
                    .endObject()
                    .endArray()
                    .endObject()
                    .startObject()
                    .field("first", "fname1")
                    .field("last", "lname1")
                    .startArray("workstations")
                    .startObject()
                    .field("stationid", "s5")
                    .field("phoneid", "p5")
                    .endObject()
                    .startObject()
                    .field("stationid", "s6")
                    .field("phoneid", "p6")
                    .endObject()
                    .endArray()
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        assertTrue(indexResponse2.getShardInfo().getSuccessful() > 0);
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
            .addSort(SortBuilders.fieldSort("users.first").setNestedSort(new NestedSortBuilder("users")).order(SortOrder.ASC))
            .addSort(
                SortBuilders.fieldSort("users.first")
                    .order(SortOrder.ASC)
                    .setNestedSort(
                        new NestedSortBuilder("users").setFilter(
                            nestedQuery("users.workstations", termQuery("users.workstations.stationid", "s5"), ScoreMode.Avg)
                        )
                    )
            )
            .get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("fname1"));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[1].toString(), equalTo("fname1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("fname1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[1].toString(), equalTo("fname3"));
    }

    public void testCheckFixedBitSetCache() throws Exception {
        boolean loadFixedBitSeLazily = randomBoolean();
        Settings.Builder settingsBuilder = Settings.builder().put(indexSettings()).put("index.refresh_interval", -1);
        if (loadFixedBitSeLazily) {
            settingsBuilder.put("index.load_fixed_bitset_filters_eagerly", false);
        }
        assertAcked(prepareCreate("test").setSettings(settingsBuilder));

        client().prepareIndex("test").setId("0").setSource("field", "value").get();
        client().prepareIndex("test").setId("1").setSource("field", "value").get();
        refresh();
        ensureSearchable("test");

        // No nested mapping yet, there shouldn't be anything in the fixed bit set cache
        ClusterStatsResponse clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsResponse.getIndicesStats().getSegments().getBitsetMemoryInBytes(), equalTo(0L));

        // Now add nested mapping
        assertAcked(client().admin().indices().preparePutMapping("test").setSource("array1", "type=nested"));

        XContentBuilder builder = jsonBuilder().startObject()
            .startArray("array1")
            .startObject()
            .field("field1", "value1")
            .endObject()
            .endArray()
            .endObject();
        // index simple data
        client().prepareIndex("test").setId("2").setSource(builder).get();
        client().prepareIndex("test").setId("3").setSource(builder).get();
        client().prepareIndex("test").setId("4").setSource(builder).get();
        client().prepareIndex("test").setId("5").setSource(builder).get();
        client().prepareIndex("test").setId("6").setSource(builder).get();
        refresh();
        ensureSearchable("test");

        if (loadFixedBitSeLazily) {
            clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
            assertThat(clusterStatsResponse.getIndicesStats().getSegments().getBitsetMemoryInBytes(), equalTo(0L));

            // only when querying with nested the fixed bitsets are loaded
            SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(nestedQuery("array1", termQuery("array1.field1", "value1"), ScoreMode.Avg))
                .get();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(5L));
        }
        clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsResponse.getIndicesStats().getSegments().getBitsetMemoryInBytes(), greaterThan(0L));

        assertAcked(client().admin().indices().prepareDelete("test"));
        clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsResponse.getIndicesStats().getSegments().getBitsetMemoryInBytes(), equalTo(0L));
    }

    private void assertDocumentCount(String index, long numdocs) {
        IndicesStatsResponse stats = admin().indices().prepareStats(index).clear().setDocs(true).get();
        assertNoFailures(stats);
        assertThat(stats.getIndex(index).getPrimaries().docs.getCount(), is(numdocs));

    }

}
