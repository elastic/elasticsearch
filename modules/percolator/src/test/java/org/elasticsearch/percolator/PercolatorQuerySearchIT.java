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
package org.elasticsearch.percolator;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.yamlBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.commonTermsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanNearQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanNotQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanTermQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;

public class PercolatorQuerySearchIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(PercolatorPlugin.class, CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("1==1", vars -> Boolean.TRUE);
            return scripts;
        }
    }

    public void testPercolateScriptQuery() throws IOException {
        client().admin().indices().prepareCreate("index").addMapping("type", "query", "type=percolator").get();
        client().prepareIndex("index", "type", "1")
            .setSource(jsonBuilder().startObject().field("query", QueryBuilders.scriptQuery(
                new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "1==1", Collections.emptyMap()))).endObject())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute().actionGet();
        SearchResponse response = client().prepareSearch("index")
            .setQuery(new PercolateQueryBuilder("query", "type", jsonBuilder().startObject().field("field1", "b").endObject().bytes(),
                XContentType.JSON))
            .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");
    }

    public void testPercolatorQuery() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("type", "field1", "type=keyword", "field2", "type=keyword")
                .addMapping("queries", "query", "type=percolator")
        );

        client().prepareIndex("test", "queries", "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .get();
        client().prepareIndex("test", "queries", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "value")).endObject())
                .get();
        client().prepareIndex("test", "queries", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "value"))
                        .must(matchQuery("field2", "value"))
                ).endObject()).get();
        client().admin().indices().prepareRefresh().get();

        BytesReference source = jsonBuilder().startObject().endObject().bytes();
        logger.info("percolating empty doc");
        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", source, XContentType.JSON))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));

        source = jsonBuilder().startObject().field("field1", "value").endObject().bytes();
        logger.info("percolating doc with 1 field");
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", source, XContentType.JSON))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));

        source = jsonBuilder().startObject().field("field1", "value").field("field2", "value").endObject().bytes();
        logger.info("percolating doc with 2 fields");
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", source, XContentType.JSON))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertHitCount(response, 3);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testPercolatorRangeQueries() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("type", "field1", "type=long", "field2", "type=double", "field3", "type=ip")
                .addMapping("queries", "query", "type=percolator")
        );

        client().prepareIndex("test", "queries", "1")
                .setSource(jsonBuilder().startObject().field("query", rangeQuery("field1").from(10).to(12)).endObject())
                .get();
        client().prepareIndex("test", "queries", "2")
                .setSource(jsonBuilder().startObject().field("query", rangeQuery("field1").from(20).to(22)).endObject())
                .get();
        client().prepareIndex("test", "queries", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(rangeQuery("field1").from(10).to(12))
                        .must(rangeQuery("field1").from(12).to(14))
                ).endObject()).get();
        client().admin().indices().prepareRefresh().get();
        client().prepareIndex("test", "queries", "4")
                .setSource(jsonBuilder().startObject().field("query", rangeQuery("field2").from(10).to(12)).endObject())
                .get();
        client().prepareIndex("test", "queries", "5")
                .setSource(jsonBuilder().startObject().field("query", rangeQuery("field2").from(20).to(22)).endObject())
                .get();
        client().prepareIndex("test", "queries", "6")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(rangeQuery("field2").from(10).to(12))
                        .must(rangeQuery("field2").from(12).to(14))
                ).endObject()).get();
        client().admin().indices().prepareRefresh().get();
        client().prepareIndex("test", "queries", "7")
                .setSource(jsonBuilder().startObject()
                        .field("query", rangeQuery("field3").from("192.168.1.0").to("192.168.1.5"))
                        .endObject())
                .get();
        client().prepareIndex("test", "queries", "8")
                .setSource(jsonBuilder().startObject()
                        .field("query", rangeQuery("field3").from("192.168.1.20").to("192.168.1.30"))
                        .endObject())
                .get();
        client().prepareIndex("test", "queries", "9")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(rangeQuery("field3").from("192.168.1.0").to("192.168.1.5"))
                        .must(rangeQuery("field3").from("192.168.1.5").to("192.168.1.10"))
                ).endObject()).get();
        client().admin().indices().prepareRefresh().get();

        // Test long range:
        BytesReference source = jsonBuilder().startObject().field("field1", 12).endObject().bytes();
        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", source, XContentType.JSON))
                .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("3"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("1"));

        source = jsonBuilder().startObject().field("field1", 11).endObject().bytes();
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", source, XContentType.JSON))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));

        // Test double range:
        source = jsonBuilder().startObject().field("field2", 12).endObject().bytes();
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", source, XContentType.JSON))
                .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("6"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("4"));

        source = jsonBuilder().startObject().field("field2", 11).endObject().bytes();
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", source, XContentType.JSON))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("4"));

        // Test IP range:
        source = jsonBuilder().startObject().field("field3", "192.168.1.5").endObject().bytes();
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", source, XContentType.JSON))
                .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("9"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("7"));

        source = jsonBuilder().startObject().field("field3", "192.168.1.4").endObject().bytes();
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", source, XContentType.JSON))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("7"));
    }

    public void testPercolatorQueryExistingDocument() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("type", "field1", "type=keyword", "field2", "type=keyword")
                .addMapping("queries", "query", "type=percolator")
        );

        client().prepareIndex("test", "queries", "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .get();
        client().prepareIndex("test", "queries", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "value")).endObject())
                .get();
        client().prepareIndex("test", "queries", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "value"))
                        .must(matchQuery("field2", "value"))
                ).endObject()).get();

        client().prepareIndex("test", "type", "1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("test", "type", "2").setSource("field1", "value").get();
        client().prepareIndex("test", "type", "3").setSource("field1", "value", "field2", "value").get();
        client().admin().indices().prepareRefresh().get();

        logger.info("percolating empty doc");
        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", "test", "type", "1", null, null, null))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));

        logger.info("percolating doc with 1 field");
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", "test", "type", "2", null, null, null))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));

        logger.info("percolating doc with 2 fields");
        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", "test", "type", "3", null, null, null))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertHitCount(response, 3);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testPercolatorQueryExistingDocumentSourceDisabled() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
            .addMapping("type", "_source", "enabled=false", "field1", "type=keyword")
            .addMapping("queries", "query", "type=percolator")
        );

        client().prepareIndex("test", "queries", "1")
            .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
            .get();

        client().prepareIndex("test", "type", "1").setSource("{}", XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        logger.info("percolating empty doc with source disabled");
        Throwable e = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", "test", "type", "1", null, null, null))
                .get();
        }).getRootCause();
        assertThat(e, instanceOf(IllegalArgumentException.class));
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    public void testPercolatorSpecificQueries()  throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("type", "field1", "type=text", "field2", "type=text")
                .addMapping("queries", "query", "type=percolator")
        );

        client().prepareIndex("test", "queries", "1")
                .setSource(jsonBuilder().startObject().field("query", commonTermsQuery("field1", "quick brown fox")).endObject())
                .get();
        client().prepareIndex("test", "queries", "2")
                .setSource(jsonBuilder().startObject().field("query", multiMatchQuery("quick brown fox", "field1", "field2")
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)).endObject())
                .get();
        client().prepareIndex("test", "queries", "3")
                .setSource(jsonBuilder().startObject().field("query",
                        spanNearQuery(spanTermQuery("field1", "quick"), 0)
                                .addClause(spanTermQuery("field1", "brown"))
                                .addClause(spanTermQuery("field1", "fox"))
                                .inOrder(true)
                ).endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        client().prepareIndex("test", "queries", "4")
                .setSource(jsonBuilder().startObject().field("query",
                        spanNotQuery(
                                spanNearQuery(spanTermQuery("field1", "quick"), 0)
                                        .addClause(spanTermQuery("field1", "brown"))
                                        .addClause(spanTermQuery("field1", "fox"))
                                        .inOrder(true),
                                spanNearQuery(spanTermQuery("field1", "the"), 0)
                                        .addClause(spanTermQuery("field1", "lazy"))
                                        .addClause(spanTermQuery("field1", "dog"))
                                        .inOrder(true)).dist(2)
                ).endObject())
                .get();

        // doesn't match
        client().prepareIndex("test", "queries", "5")
                .setSource(jsonBuilder().startObject().field("query",
                        spanNotQuery(
                                spanNearQuery(spanTermQuery("field1", "quick"), 0)
                                        .addClause(spanTermQuery("field1", "brown"))
                                        .addClause(spanTermQuery("field1", "fox"))
                                        .inOrder(true),
                                spanNearQuery(spanTermQuery("field1", "the"), 0)
                                        .addClause(spanTermQuery("field1", "lazy"))
                                        .addClause(spanTermQuery("field1", "dog"))
                                        .inOrder(true)).dist(3)
                ).endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        BytesReference source = jsonBuilder().startObject()
                .field("field1", "the quick brown fox jumps over the lazy dog")
                .field("field2", "the quick brown fox falls down into the well")
                .endObject().bytes();
        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", source, XContentType.JSON))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertHitCount(response, 4);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getScore(), equalTo(Float.NaN));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(1).getScore(), equalTo(Float.NaN));
        assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(response.getHits().getAt(2).getScore(), equalTo(Float.NaN));
        assertThat(response.getHits().getAt(3).getId(), equalTo("4"));
        assertThat(response.getHits().getAt(3).getScore(), equalTo(Float.NaN));
    }

    public void testPercolatorQueryWithHighlighting() throws Exception {
        StringBuilder fieldMapping = new StringBuilder("type=text")
                .append(",store=").append(randomBoolean());
        if (randomBoolean()) {
            fieldMapping.append(",term_vector=with_positions_offsets");
        } else if (randomBoolean()) {
            fieldMapping.append(",index_options=offsets");
        }
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("type", "field1", fieldMapping)
                .addMapping("queries", "query", "type=percolator")
        );
        client().prepareIndex("test", "queries", "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "brown fox")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "queries", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "lazy dog")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "queries", "3")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "jumps")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "queries", "4")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "dog")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "queries", "5")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "fox")).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh().get();

        BytesReference document = jsonBuilder().startObject()
                .field("field1", "The quick brown fox jumps over the lazy dog")
                .endObject().bytes();
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "type", document, XContentType.JSON))
                .highlighter(new HighlightBuilder().field("field1"))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertHitCount(searchResponse, 5);

        assertThat(searchResponse.getHits().getAt(0).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog"));
        assertThat(searchResponse.getHits().getAt(1).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown fox jumps over the <em>lazy</em> <em>dog</em>"));
        assertThat(searchResponse.getHits().getAt(2).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(searchResponse.getHits().getAt(3).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown fox jumps over the lazy <em>dog</em>"));
        assertThat(searchResponse.getHits().getAt(4).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown <em>fox</em> jumps over the lazy dog"));
    }

    public void testTakePositionOffsetGapIntoAccount() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("type", "field", "type=text,position_increment_gap=5")
                .addMapping("queries", "query", "type=percolator")
        );
        client().prepareIndex("test", "queries", "1")
                .setSource(jsonBuilder().startObject().field("query",
                        new MatchPhraseQueryBuilder("field", "brown fox").slop(4)).endObject())
                .get();
        client().prepareIndex("test", "queries", "2")
                .setSource(jsonBuilder().startObject().field("query",
                        new MatchPhraseQueryBuilder("field", "brown fox").slop(5)).endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        SearchResponse response = client().prepareSearch().setQuery(
                new PercolateQueryBuilder("query", "type", new BytesArray("{\"field\" : [\"brown\", \"fox\"]}"), XContentType.JSON)
        ).get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
    }


    public void testManyPercolatorFields() throws Exception {
        String queryFieldName = randomAsciiOfLength(8);
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("doc_type", "field", "type=keyword")
                .addMapping("query_type1", queryFieldName, "type=percolator")
                .addMapping("query_type2", queryFieldName, "type=percolator", "second_query_field", "type=percolator")
                .addMapping("query_type3", jsonBuilder().startObject().startObject("query_type3").startObject("properties")
                        .startObject("object_field")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject(queryFieldName)
                        .field("type", "percolator")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject().endObject())
        );
    }

    public void testWithMultiplePercolatorFields() throws Exception {
        String queryFieldName = randomAsciiOfLength(8);
        createIndex("test1", client().admin().indices().prepareCreate("test1")
                .addMapping("doc_type", "field", "type=keyword")
                .addMapping("query_type", queryFieldName, "type=percolator"));
        createIndex("test2", client().admin().indices().prepareCreate("test2")
                .addMapping("doc_type", "field", "type=keyword")
                .addMapping("query_type", jsonBuilder().startObject().startObject("query_type").startObject("properties")
                        .startObject("object_field")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject(queryFieldName)
                        .field("type", "percolator")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject().endObject())
        );

        // Acceptable:
        client().prepareIndex("test1", "query_type", "1")
                .setSource(jsonBuilder().startObject().field(queryFieldName, matchQuery("field", "value")).endObject())
                .get();
        client().prepareIndex("test2", "query_type", "1")
                .setSource(jsonBuilder().startObject().startObject("object_field")
                        .field(queryFieldName, matchQuery("field", "value"))
                        .endObject().endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        BytesReference source = jsonBuilder().startObject().field("field", "value").endObject().bytes();
        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder(queryFieldName, "doc_type", source, XContentType.JSON))
                .setIndices("test1")
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getType(), equalTo("query_type"));
        assertThat(response.getHits().getAt(0).getIndex(), equalTo("test1"));

        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("object_field." + queryFieldName, "doc_type", source, XContentType.JSON))
                .setIndices("test2")
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getType(), equalTo("query_type"));
        assertThat(response.getHits().getAt(0).getIndex(), equalTo("test2"));

        // Unacceptable:
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            client().prepareIndex("test2", "query_type", "1")
                    .setSource(jsonBuilder().startObject().startArray("object_field")
                            .startObject().field(queryFieldName, matchQuery("field", "value")).endObject()
                            .startObject().field(queryFieldName, matchQuery("field", "value")).endObject()
                            .endArray().endObject())
                    .get();
        });
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), equalTo("a document can only contain one percolator query"));
    }

    public void testPercolateQueryWithNestedDocuments() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject().startObject("properties").startObject("companyname").field("type", "text").endObject()
                .startObject("employee").field("type", "nested").startObject("properties")
                .startObject("name").field("type", "text").endObject().endObject().endObject().endObject()
                .endObject();
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("employee", mapping)
                .addMapping("queries", "query", "type=percolator")
        );
        client().prepareIndex("test", "queries", "q1").setSource(jsonBuilder().startObject()
                .field("query", QueryBuilders.nestedQuery("employee",
                        QueryBuilders.matchQuery("employee.name", "virginia potts").operator(Operator.AND), ScoreMode.Avg)
                ).endObject())
                .get();
        // this query should never match as it doesn't use nested query:
        client().prepareIndex("test", "queries", "q2").setSource(jsonBuilder().startObject()
                .field("query", QueryBuilders.matchQuery("employee.name", "virginia")).endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "employee",
                        XContentFactory.jsonBuilder()
                            .startObject().field("companyname", "stark")
                                .startArray("employee")
                                    .startObject().field("name", "virginia potts").endObject()
                                    .startObject().field("name", "tony stark").endObject()
                                .endArray()
                            .endObject().bytes(), XContentType.JSON))
                .addSort("_doc", SortOrder.ASC)
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("q1"));

        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "employee",
                        XContentFactory.jsonBuilder()
                            .startObject().field("companyname", "notstark")
                                .startArray("employee")
                                    .startObject().field("name", "virginia stark").endObject()
                                    .startObject().field("name", "tony stark").endObject()
                                .endArray()
                            .endObject().bytes(), XContentType.JSON))
                .addSort("_doc", SortOrder.ASC)
                .get();
        assertHitCount(response, 0);

        response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", "employee",
                        XContentFactory.jsonBuilder().startObject().field("companyname", "notstark").endObject().bytes(),
                    XContentType.JSON))
                .addSort("_doc", SortOrder.ASC)
                .get();
        assertHitCount(response, 0);
    }

    public void testPercolatorQueryViaMultiSearch() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
            .addMapping("type", "field1", "type=text")
            .addMapping("queries", "query", "type=percolator")
        );

        client().prepareIndex("test", "queries", "1")
            .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
            .execute().actionGet();
        client().prepareIndex("test", "queries", "2")
            .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
            .execute().actionGet();
        client().prepareIndex("test", "queries", "3")
            .setSource(jsonBuilder().startObject().field("query", boolQuery()
                .must(matchQuery("field1", "b"))
                .must(matchQuery("field1", "c"))
            ).endObject())
            .execute().actionGet();
        client().prepareIndex("test", "queries", "4")
            .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
            .execute().actionGet();
        client().prepareIndex("test", "type", "1")
            .setSource(jsonBuilder().startObject().field("field1", "c").endObject())
            .execute().actionGet();
        client().admin().indices().prepareRefresh().get();

        MultiSearchResponse response = client().prepareMultiSearch()
            .add(client().prepareSearch("test")
                .setQuery(new PercolateQueryBuilder("query", "type",
                    jsonBuilder().startObject().field("field1", "b").endObject().bytes(), XContentType.JSON)))
            .add(client().prepareSearch("test")
                .setQuery(new PercolateQueryBuilder("query", "type",
                    yamlBuilder().startObject().field("field1", "c").endObject().bytes(), XContentType.JSON)))
            .add(client().prepareSearch("test")
                .setQuery(new PercolateQueryBuilder("query", "type",
                    smileBuilder().startObject().field("field1", "b c").endObject().bytes(), XContentType.JSON)))
            .add(client().prepareSearch("test")
                .setQuery(new PercolateQueryBuilder("query", "type",
                    jsonBuilder().startObject().field("field1", "d").endObject().bytes(), XContentType.JSON)))
            .add(client().prepareSearch("test")
                .setQuery(new PercolateQueryBuilder("query", "type", "test", "type", "1", null, null, null)))
            .add(client().prepareSearch("test") // non existing doc, so error element
                .setQuery(new PercolateQueryBuilder("query", "type", "test", "type", "2", null, null, null)))
            .get();

        MultiSearchResponse.Item item = response.getResponses()[0];
        assertHitCount(item.getResponse(), 2L);
        assertSearchHits(item.getResponse(), "1", "4");
        assertThat(item.getFailureMessage(), nullValue());

        item = response.getResponses()[1];
        assertHitCount(item.getResponse(), 2L);
        assertSearchHits(item.getResponse(), "2", "4");
        assertThat(item.getFailureMessage(), nullValue());

        item = response.getResponses()[2];
        assertHitCount(item.getResponse(), 4L);
        assertSearchHits(item.getResponse(), "1", "2", "3", "4");
        assertThat(item.getFailureMessage(), nullValue());

        item = response.getResponses()[3];
        assertHitCount(item.getResponse(), 1L);
        assertSearchHits(item.getResponse(), "4");
        assertThat(item.getFailureMessage(), nullValue());

        item = response.getResponses()[4];
        assertHitCount(item.getResponse(), 2L);
        assertSearchHits(item.getResponse(), "2", "4");
        assertThat(item.getFailureMessage(), nullValue());

        item = response.getResponses()[5];
        assertThat(item.getResponse(), nullValue());
        assertThat(item.getFailureMessage(), notNullValue());
        assertThat(item.getFailureMessage(), equalTo("all shards failed"));
        assertThat(ExceptionsHelper.unwrapCause(item.getFailure().getCause()).getMessage(),
            containsString("[test/type/2] couldn't be found"));
    }

}
