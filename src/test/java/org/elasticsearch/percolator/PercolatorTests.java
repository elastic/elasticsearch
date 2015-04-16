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

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.percolator.PercolatorException;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscore.factor.FactorBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.settings.ImmutableSettings.builder;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.yamlBuilder;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertMatchCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class PercolatorTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSimple1() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> Add dummy doc");
        client().prepareIndex("test", "type", "1").setSource("field1", "value").execute().actionGet();
        waitForConcreteMappingsOnAll("test", "type", "field1");

        logger.info("--> register a queries");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Percolate doc with field1=b");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "4"));

        logger.info("--> Percolate doc with field1=c");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(yamlBuilder().startObject().field("field1", "c").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate doc with field1=b c");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(smileBuilder().startObject().field("field1", "b c").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 4l);
        assertThat(response.getMatches(), arrayWithSize(4));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4"));

        logger.info("--> Percolate doc with field1=d");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "d").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("4"));

        logger.info("--> Search dummy doc, percolate queries must not be included");
        SearchResponse searchResponse = client().prepareSearch("test", "test").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getAt(0).type(), equalTo("type"));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

        logger.info("--> Percolate non existing doc");
        try {
            client().preparePercolate()
                    .setIndices("test").setDocumentType("type")
                    .setGetRequest(Requests.getRequest("test").type("type").id("5"))
                    .execute().actionGet();
            fail("Exception should have been thrown");
        } catch (DocumentMissingException e) {
        }
    }

    @Test
    public void testSimple2() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", "field1", "type=long,doc_values=true"));
        ensureGreen();

        // introduce the doc
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", 1)
                .field("field2", "value")
                .endObject().endObject();

        PercolateResponse response = client().preparePercolate().setSource(doc)
                .setIndices("test").setDocumentType("type1")
                .execute().actionGet();
        assertMatchCount(response, 0l);
        assertThat(response.getMatches(), emptyArray());
        waitForConcreteMappingsOnAll("test", "type1", "field1", "field2");

        // add first query...
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "test1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field2", "value")).endObject())
                .execute().actionGet();

        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(doc).execute().actionGet();
        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("test1"));

        // add second query...
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "test2")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field1", 1)).endObject())
                .execute().actionGet();

        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(doc)
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("test1", "test2"));


        client().prepareDelete("test", PercolatorService.TYPE_NAME, "test2").execute().actionGet();
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(doc).execute().actionGet();
        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("test1"));

        // add a range query (cached)
        // add a query
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "test3")
                .setSource(
                        XContentFactory.jsonBuilder().startObject().field("query",
                                constantScoreQuery(rangeFilter("field1").from(1).to(5).includeLower(true).setExecution("fielddata"))
                        ).endObject()
                )
                .execute().actionGet();

        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(doc).execute().actionGet();
        ElasticsearchAssertions.assertFailures(response);
        // TODO: with doc values by default, fielddata execution mode causes a doc values
        // lookup, but memory index doesn't have doc values. we should consider just removing
        // execution mode.
        //assertMatchCount(response, 2l);
        //assertThat(response.getMatches(), arrayWithSize(2));
        //assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("test1", "test3"));
    }

    @Test
    public void testRangeFilterThatUsesFD() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=long,doc_values=false")
                .get();


        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(
                        XContentFactory.jsonBuilder().startObject().field("query",
                                constantScoreQuery(rangeFilter("field1").from(1).to(5).setExecution("fielddata"))
                        ).endObject()
                ).get();

        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setPercolateDoc(PercolateSourceBuilder.docBuilder().setDoc("field1", 3)).get();
        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("1"));
    }

    @Test
    public void testPercolateQueriesWithRouting() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .execute().actionGet();
        ensureGreen();

        logger.info("--> register a queries");
        for (int i = 1; i <= 100; i++) {
            client().prepareIndex("test", PercolatorService.TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .setRouting(Integer.toString(i % 2))
                    .execute().actionGet();
        }

        logger.info("--> Percolate doc with no routing");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 100l);
        assertThat(response.getMatches(), arrayWithSize(100));

        logger.info("--> Percolate doc with routing=0");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .setRouting("0")
                .execute().actionGet();
        assertMatchCount(response, 50l);
        assertThat(response.getMatches(), arrayWithSize(50));

        logger.info("--> Percolate doc with routing=1");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .setRouting("1")
                .execute().actionGet();
        assertMatchCount(response, 50l);
        assertThat(response.getMatches(), arrayWithSize(50));
    }

    @Test
    public void percolateOnRecreatedIndex() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("my-queries-index", "test", "1").setSource("field1", "value1").execute().actionGet();
        waitForConcreteMappingsOnAll("my-queries-index", "test", "field1");
        logger.info("--> register a query");
        client().prepareIndex("my-queries-index", PercolatorService.TYPE_NAME, "kuku1")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        cluster().wipeIndices("test");
        createIndex("test");
        ensureGreen();

        client().prepareIndex("my-queries-index", "test", "1").setSource("field1", "value1").execute().actionGet();
        waitForConcreteMappingsOnAll("my-queries-index", "test", "field1");
        logger.info("--> register a query");
        client().prepareIndex("my-queries-index", PercolatorService.TYPE_NAME, "kuku2")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
    }

    @Test
    // see #2814
    public void percolateCustomAnalyzer() throws Exception {
        Builder builder = builder();
        builder.put("index.analysis.analyzer.lwhitespacecomma.tokenizer", "whitespacecomma");
        builder.putArray("index.analysis.analyzer.lwhitespacecomma.filter", "lowercase");
        builder.put("index.analysis.tokenizer.whitespacecomma.type", "pattern");
        builder.put("index.analysis.tokenizer.whitespacecomma.pattern", "(,|\\s+)");

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .startObject("properties")
                .startObject("filingcategory").field("type", "string").field("analyzer", "lwhitespacecomma").endObject()
                .endObject()
                .endObject().endObject();

        assertAcked(prepareCreate("test").setSettings(builder).addMapping("doc", mapping));
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject()
                        .field("source", "productizer")
                        .field("query", QueryBuilders.constantScoreQuery(QueryBuilders.queryStringQuery("filingcategory:s")))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateResponse percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("doc")
                .setSource(jsonBuilder().startObject()
                        .startObject("doc").field("filingcategory", "s").endObject()
                        .field("query", termQuery("source", "productizer"))
                        .endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1l);
        assertThat(percolate.getMatches(), arrayWithSize(1));

    }

    @Test
    public void createIndexAndThenRegisterPercolator() throws Exception {
        prepareCreate("test")
                .addMapping("type1", "field1", "type=string")
                .get();
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .execute().actionGet();

        refresh();
        CountResponse countResponse = client().prepareCount()
                .setQuery(matchAllQuery()).setTypes(PercolatorService.TYPE_NAME)
                .execute().actionGet();
        assertThat(countResponse.getCount(), equalTo(1l));


        for (int i = 0; i < 10; i++) {
            PercolateResponse percolate = client().preparePercolate()
                    .setIndices("test").setDocumentType("type1")
                    .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                    .execute().actionGet();
            assertMatchCount(percolate, 1l);
            assertThat(percolate.getMatches(), arrayWithSize(1));
        }

        for (int i = 0; i < 10; i++) {
            PercolateResponse percolate = client().preparePercolate()
                    .setIndices("test").setDocumentType("type1")
                    .setPreference("_local")
                    .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                    .execute().actionGet();
            assertMatchCount(percolate, 1l);
            assertThat(percolate.getMatches(), arrayWithSize(1));
        }


        logger.info("--> delete the index");
        client().admin().indices().prepareDelete("test").execute().actionGet();
        logger.info("--> make sure percolated queries for it have been deleted as well");
        countResponse = client().prepareCount()
                .setQuery(matchAllQuery()).setTypes(PercolatorService.TYPE_NAME)
                .execute().actionGet();
        assertHitCount(countResponse, 0l);
    }

    @Test
    public void multiplePercolators() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", "field1", "type=string"));
        ensureGreen();

        logger.info("--> register a query 1");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        logger.info("--> register a query 2");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "bubu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "green")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateResponse percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1l);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("kuku"));

        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value2").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1l);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("bubu"));

    }

    @Test
    public void dynamicAddingRemovingQueries() throws Exception {
        assertAcked(
                prepareCreate("test")
                        .addMapping("type1", "field1", "type=string")
        );
        ensureGreen();

        logger.info("--> register a query 1");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateResponse percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1l);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("kuku"));

        logger.info("--> register a query 2");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "bubu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "green")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value2").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1l);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("bubu"));

        logger.info("--> register a query 3");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "susu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "red")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateSourceBuilder sourceBuilder = new PercolateSourceBuilder()
                .setDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "value2").endObject()))
                .setQueryBuilder(termQuery("color", "red"));
        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(sourceBuilder)
                .execute().actionGet();
        assertMatchCount(percolate, 1l);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("susu"));

        logger.info("--> deleting query 1");
        client().prepareDelete("test", PercolatorService.TYPE_NAME, "kuku").setRefresh(true).execute().actionGet();

        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").startObject("type1")
                        .field("field1", "value1")
                        .endObject().endObject().endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 0l);
        assertThat(percolate.getMatches(), emptyArray());
    }

    @Test
    public void percolateWithSizeField() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_size").field("enabled", true).endObject()
                .startObject("properties").startObject("field1").field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        logger.info("--> percolate a document");
        PercolateResponse percolate = client().preparePercolate().setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject()
                        .startObject("doc")
                        .field("field1", "value1")
                        .endObject()
                        .endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1l);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("kuku"));
    }

    @Test
    public void testPercolateStatistics() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> First percolate request");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field", "val").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 1l);
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("1"));

        NumShards numShards = getNumShards("test");

        IndicesStatsResponse indicesResponse = client().admin().indices().prepareStats("test").execute().actionGet();
        assertThat(indicesResponse.getTotal().getPercolate().getCount(), equalTo((long) numShards.numPrimaries));
        assertThat(indicesResponse.getTotal().getPercolate().getCurrent(), equalTo(0l));
        assertThat(indicesResponse.getTotal().getPercolate().getNumQueries(), equalTo((long)numShards.dataCopies)); //number of copies
        assertThat(indicesResponse.getTotal().getPercolate().getMemorySizeInBytes(), equalTo(-1l));

        NodesStatsResponse nodesResponse = client().admin().cluster().prepareNodesStats().execute().actionGet();
        long percolateCount = 0;
        for (NodeStats nodeStats : nodesResponse) {
            percolateCount += nodeStats.getIndices().getPercolate().getCount();
        }
        assertThat(percolateCount, equalTo((long) numShards.numPrimaries));

        logger.info("--> Second percolate request");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field", "val").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("1"));

        indicesResponse = client().admin().indices().prepareStats().setPercolate(true).execute().actionGet();
        assertThat(indicesResponse.getTotal().getPercolate().getCount(), equalTo((long) numShards.numPrimaries * 2));
        assertThat(indicesResponse.getTotal().getPercolate().getCurrent(), equalTo(0l));
        assertThat(indicesResponse.getTotal().getPercolate().getNumQueries(), equalTo((long)numShards.dataCopies)); //number of copies
        assertThat(indicesResponse.getTotal().getPercolate().getMemorySizeInBytes(), equalTo(-1l));

        percolateCount = 0;
        nodesResponse = client().admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats nodeStats : nodesResponse) {
            percolateCount += nodeStats.getIndices().getPercolate().getCount();
        }
        assertThat(percolateCount, equalTo((long) numShards.numPrimaries *2));

        // We might be faster than 1 ms, so run upto 1000 times until have spend 1ms or more on percolating
        boolean moreThanOneMs = false;
        int counter = 3; // We already ran two times.
        do {
            indicesResponse = client().admin().indices().prepareStats("test").execute().actionGet();
            if (indicesResponse.getTotal().getPercolate().getTimeInMillis() > 0) {
                moreThanOneMs = true;
                break;
            }

            logger.info("--> {}th percolate request", counter);
            response = client().preparePercolate()
                    .setIndices("test").setDocumentType("type")
                    .setSource(jsonBuilder().startObject().startObject("doc").field("field", "val").endObject().endObject())
                    .execute().actionGet();
            assertThat(response.getMatches(), arrayWithSize(1));
            assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("1"));
        } while (++counter <= 1000);
        assertTrue("Something is off, we should have spent at least 1ms on percolating...", moreThanOneMs);

        long percolateSumTime = 0;
        nodesResponse = client().admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats nodeStats : nodesResponse) {
            percolateCount += nodeStats.getIndices().getPercolate().getCount();
            percolateSumTime += nodeStats.getIndices().getPercolate().getTimeInMillis();
        }
        assertThat(percolateSumTime, greaterThan(0l));
    }

    @Test
    public void testPercolatingExistingDocs() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> Adding docs");
        client().prepareIndex("test", "type", "1").setSource("field1", "b").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field1", "c").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field1", "b c").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field1", "d").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Percolate existing doc with id 1");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("1"))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "4"));

        logger.info("--> Percolate existing doc with id 2");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("2"))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate existing doc with id 3");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("3"))
                .execute().actionGet();
        assertMatchCount(response, 4l);
        assertThat(response.getMatches(), arrayWithSize(4));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4"));

        logger.info("--> Percolate existing doc with id 4");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("4"))
                .execute().actionGet();
        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("4"));

        logger.info("--> Search normals docs, percolate queries must not be included");
        SearchResponse searchResponse = client().prepareSearch("test").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(4L));
        assertThat(searchResponse.getHits().getAt(0).type(), equalTo("type"));
        assertThat(searchResponse.getHits().getAt(1).type(), equalTo("type"));
        assertThat(searchResponse.getHits().getAt(2).type(), equalTo("type"));
        assertThat(searchResponse.getHits().getAt(3).type(), equalTo("type"));
    }

    @Test
    public void testPercolatingExistingDocs_routing() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> Adding docs");
        client().prepareIndex("test", "type", "1").setSource("field1", "b").setRouting("4").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field1", "c").setRouting("3").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field1", "b c").setRouting("2").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field1", "d").setRouting("1").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Percolate existing doc with id 1");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("1").routing("4"))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "4"));

        logger.info("--> Percolate existing doc with id 2");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("2").routing("3"))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate existing doc with id 3");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("3").routing("2"))
                .execute().actionGet();
        assertMatchCount(response, 4l);
        assertThat(response.getMatches(), arrayWithSize(4));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4"));

        logger.info("--> Percolate existing doc with id 4");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("4").routing("1"))
                .execute().actionGet();
        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("4"));
    }

    @Test
    public void testPercolatingExistingDocs_versionCheck() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> Adding docs");
        client().prepareIndex("test", "type", "1").setSource("field1", "b").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field1", "c").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field1", "b c").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field1", "d").execute().actionGet();

        logger.info("--> registering queries");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Percolate existing doc with id 2 and version 1");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("2").version(1l))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate existing doc with id 2 and version 2");
        try {
            client().preparePercolate()
                    .setIndices("test").setDocumentType("type")
                    .setGetRequest(Requests.getRequest("test").type("type").id("2").version(2l))
                    .execute().actionGet();
            fail("Error should have been thrown");
        } catch (VersionConflictEngineException e) {
        }

        logger.info("--> Index doc with id for the second time");
        client().prepareIndex("test", "type", "2").setSource("field1", "c").execute().actionGet();

        logger.info("--> Percolate existing doc with id 2 and version 2");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("2").version(2l))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));
    }

    @Test
    public void testPercolateMultipleIndicesAndAliases() throws Exception {
        createIndex("test1", "test2");
        ensureGreen();

        logger.info("--> registering queries");
        for (int i = 1; i <= 10; i++) {
            String index = i % 2 == 0 ? "test1" : "test2";
            client().prepareIndex(index, PercolatorService.TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .execute().actionGet();
        }

        logger.info("--> Percolate doc to index test1");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test1").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 5l);
        assertThat(response.getMatches(), arrayWithSize(5));

        logger.info("--> Percolate doc to index test2");
        response = client().preparePercolate()
                .setIndices("test2").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 5l);
        assertThat(response.getMatches(), arrayWithSize(5));

        logger.info("--> Percolate doc to index test1 and test2");
        response = client().preparePercolate()
                .setIndices("test1", "test2").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 10l);
        assertThat(response.getMatches(), arrayWithSize(10));

        logger.info("--> Percolate doc to index test2 and test3, with ignore missing");
        response = client().preparePercolate()
                .setIndices("test1", "test3").setDocumentType("type")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 5l);
        assertThat(response.getMatches(), arrayWithSize(5));

        logger.info("--> Adding aliases");
        IndicesAliasesResponse aliasesResponse = client().admin().indices().prepareAliases()
                .addAlias("test1", "my-alias1")
                .addAlias("test2", "my-alias1")
                .addAlias("test2", "my-alias2")
                .setTimeout(TimeValue.timeValueHours(10))
                .execute().actionGet();
        assertTrue(aliasesResponse.isAcknowledged());

        logger.info("--> Percolate doc to my-alias1");
        response = client().preparePercolate()
                .setIndices("my-alias1").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 10l);
        assertThat(response.getMatches(), arrayWithSize(10));
        for (PercolateResponse.Match match : response) {
            assertThat(match.getIndex().string(), anyOf(equalTo("test1"), equalTo("test2")));
        }

        logger.info("--> Percolate doc to my-alias2");
        response = client().preparePercolate()
                .setIndices("my-alias2").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 5l);
        assertThat(response.getMatches(), arrayWithSize(5));
        for (PercolateResponse.Match match : response) {
            assertThat(match.getIndex().string(), equalTo("test2"));
        }
    }

    @Test
    public void testPercolateWithAliasFilter() throws Exception {
        assertAcked(prepareCreate("my-index")
                        .addMapping(PercolatorService.TYPE_NAME, "a", "type=string,index=not_analyzed")
                        .addAlias(new Alias("a").filter(FilterBuilders.termFilter("a", "a")))
                        .addAlias(new Alias("b").filter(FilterBuilders.termFilter("a", "b")))
                        .addAlias(new Alias("c").filter(FilterBuilders.termFilter("a", "c")))
        );
        client().prepareIndex("my-index", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("a", "a").endObject())
                .get();
        client().prepareIndex("my-index", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("a", "b").endObject())
                .get();
        refresh();

        // Specifying only the document to percolate and no filter, sorting or aggs, the queries are retrieved from
        // memory directly. Otherwise we need to retrieve those queries from lucene to be able to execute filters,
        // aggregations and sorting on top of them. So this test a different code execution path.
        PercolateResponse response = client().preparePercolate()
                .setIndices("a")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(1l));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));

        response = client().preparePercolate()
                .setIndices("b")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(1l));
        assertThat(response.getMatches()[0].getId().string(), equalTo("2"));


        response = client().preparePercolate()
                .setIndices("c")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(0l));

        // Testing that the alias filter and the filter specified while percolating are both taken into account.
        response = client().preparePercolate()
                .setIndices("a")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .setPercolateFilter(FilterBuilders.matchAllFilter())
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(1l));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));

        response = client().preparePercolate()
                .setIndices("b")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .setPercolateFilter(FilterBuilders.matchAllFilter())
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(1l));
        assertThat(response.getMatches()[0].getId().string(), equalTo("2"));


        response = client().preparePercolate()
                .setIndices("c")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .setPercolateFilter(FilterBuilders.matchAllFilter())
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(0l));
    }

    @Test
    public void testCountPercolation() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> Add dummy doc");
        client().prepareIndex("test", "type", "1").setSource("field1", "value").execute().actionGet();
        waitForConcreteMappingsOnAll("test", "type", "field1");

        logger.info("--> register a queries");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Count percolate doc with field1=b");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate doc with field1=c");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(yamlBuilder().startObject().field("field1", "c").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate doc with field1=b c");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(smileBuilder().startObject().field("field1", "b c").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 4l);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate doc with field1=d");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "d").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate non existing doc");
        try {
            client().preparePercolate()
                    .setIndices("test").setDocumentType("type").setOnlyCount(true)
                    .setGetRequest(Requests.getRequest("test").type("type").id("5"))
                    .execute().actionGet();
            fail("Exception should have been thrown");
        } catch (DocumentMissingException e) {
        }
    }

    @Test
    public void testCountPercolatingExistingDocs() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> Adding docs");
        client().prepareIndex("test", "type", "1").setSource("field1", "b").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field1", "c").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field1", "b c").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field1", "d").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Count percolate existing doc with id 1");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest("test").type("type").id("1"))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate existing doc with id 2");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest("test").type("type").id("2"))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate existing doc with id 3");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest("test").type("type").id("3"))
                .execute().actionGet();
        assertMatchCount(response, 4l);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate existing doc with id 4");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest("test").type("type").id("4"))
                .execute().actionGet();
        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), nullValue());
    }

    @Test
    public void testPercolateSizingWithQueryAndFilter() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        int numLevels = randomIntBetween(1, 25);
        long numQueriesPerLevel = randomIntBetween(10, 250);
        long totalQueries = numLevels * numQueriesPerLevel;
        logger.info("--> register " + totalQueries + " queries");
        for (int level = 1; level <= numLevels; level++) {
            for (int query = 1; query <= numQueriesPerLevel; query++) {
                client().prepareIndex("my-index", PercolatorService.TYPE_NAME, level + "-" + query)
                        .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("level", level).endObject())
                        .execute().actionGet();
            }
        }

        boolean onlyCount = randomBoolean();
        PercolateResponse response = client().preparePercolate()
                .setIndices("my-index").setDocumentType("my-type")
                .setOnlyCount(onlyCount)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .execute().actionGet();
        assertMatchCount(response, totalQueries);
        if (!onlyCount) {
            assertThat(response.getMatches().length, equalTo((int) totalQueries));
        }

        int size = randomIntBetween(0, (int) totalQueries - 1);
        response = client().preparePercolate()
                .setIndices("my-index").setDocumentType("my-type")
                .setOnlyCount(onlyCount)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .setSize(size)
                .execute().actionGet();
        assertMatchCount(response, totalQueries);
        if (!onlyCount) {
            assertThat(response.getMatches().length, equalTo(size));
        }

        // The query / filter capabilities are NOT in realtime
        client().admin().indices().prepareRefresh("my-index").execute().actionGet();

        int runs = randomIntBetween(3, 16);
        for (int i = 0; i < runs; i++) {
            onlyCount = randomBoolean();
            response = client().preparePercolate()
                    .setIndices("my-index").setDocumentType("my-type")
                    .setOnlyCount(onlyCount)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateQuery(termQuery("level", 1 + randomInt(numLevels - 1)))
                    .execute().actionGet();
            assertMatchCount(response, numQueriesPerLevel);
            if (!onlyCount) {
                assertThat(response.getMatches().length, equalTo((int) numQueriesPerLevel));
            }
        }

        for (int i = 0; i < runs; i++) {
            onlyCount = randomBoolean();
            response = client().preparePercolate()
                    .setIndices("my-index").setDocumentType("my-type")
                    .setOnlyCount(onlyCount)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateFilter(termFilter("level", 1 + randomInt(numLevels - 1)))
                    .execute().actionGet();
            assertMatchCount(response, numQueriesPerLevel);
            if (!onlyCount) {
                assertThat(response.getMatches().length, equalTo((int) numQueriesPerLevel));
            }
        }

        for (int i = 0; i < runs; i++) {
            onlyCount = randomBoolean();
            size = randomIntBetween(0, (int) numQueriesPerLevel - 1);
            response = client().preparePercolate()
                    .setIndices("my-index").setDocumentType("my-type")
                    .setOnlyCount(onlyCount)
                    .setSize(size)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateFilter(termFilter("level", 1 + randomInt(numLevels - 1)))
                    .execute().actionGet();
            assertMatchCount(response, numQueriesPerLevel);
            if (!onlyCount) {
                assertThat(response.getMatches().length, equalTo(size));
            }
        }
    }

    @Test
    public void testPercolateScoreAndSorting() throws Exception {
        createIndex("my-index");
        ensureGreen();

        // Add a dummy doc, that shouldn't never interfere with percolate operations.
        client().prepareIndex("my-index", "my-type", "1").setSource("field", "value").execute().actionGet();

        Map<Integer, NavigableSet<Integer>> controlMap = new HashMap<>();
        long numQueries = randomIntBetween(100, 250);
        logger.info("--> register " + numQueries + " queries");
        for (int i = 0; i < numQueries; i++) {
            int value = randomInt(10);
            client().prepareIndex("my-index", PercolatorService.TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("level", i).field("field1", value).endObject())
                    .execute().actionGet();
            if (!controlMap.containsKey(value)) {
                controlMap.put(value, new TreeSet<Integer>());
            }
            controlMap.get(value).add(i);
        }
        List<Integer> usedValues = new ArrayList<>(controlMap.keySet());
        refresh();

        // Only retrieve the score
        int runs = randomInt(27);
        for (int i = 0; i < runs; i++) {
            int size = randomIntBetween(1, 50);
            PercolateResponse response = client().preparePercolate().setIndices("my-index").setDocumentType("my-type")
                    .setScore(true)
                    .setSize(size)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), scriptFunction("doc['level'].value")))
                    .execute().actionGet();
            assertMatchCount(response, numQueries);
            assertThat(response.getMatches().length, equalTo(size));
            for (int j = 0; j < response.getMatches().length; j++) {
                String id = response.getMatches()[j].getId().string();
                assertThat(Integer.valueOf(id), equalTo((int) response.getMatches()[j].getScore()));
            }
        }

        // Sort the queries by the score
        for (int i = 0; i < runs; i++) {
            int size = randomIntBetween(1, 10);
            PercolateResponse response = client().preparePercolate().setIndices("my-index").setDocumentType("my-type")
                    .setSortByScore(true)
                    .setSize(size)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), scriptFunction("doc['level'].value")))
                    .execute().actionGet();
            assertMatchCount(response, numQueries);
            assertThat(response.getMatches().length, equalTo(size));

            int expectedId = (int) (numQueries - 1);
            for (PercolateResponse.Match match : response) {
                assertThat(match.getId().string(), equalTo(Integer.toString(expectedId)));
                assertThat(match.getScore(), equalTo((float) expectedId));
                assertThat(match.getIndex().string(), equalTo("my-index"));
                expectedId--;
            }
        }


        for (int i = 0; i < runs; i++) {
            int value = usedValues.get(randomInt(usedValues.size() - 1));
            NavigableSet<Integer> levels = controlMap.get(value);
            int size = randomIntBetween(1, levels.size());
            PercolateResponse response = client().preparePercolate().setIndices("my-index").setDocumentType("my-type")
                    .setSortByScore(true)
                    .setSize(size)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateQuery(
                            QueryBuilders.functionScoreQuery(matchQuery("field1", value), scriptFunction("doc['level'].value")).boostMode(
                                    CombineFunction.REPLACE))
                    .execute().actionGet();

            assertMatchCount(response, levels.size());
            assertThat(response.getMatches().length, equalTo(Math.min(levels.size(), size)));
            Iterator<Integer> levelIterator = levels.descendingIterator();
            for (PercolateResponse.Match match : response) {
                int controlLevel = levelIterator.next();
                assertThat(match.getId().string(), equalTo(Integer.toString(controlLevel)));
                assertThat(match.getScore(), equalTo((float) controlLevel));
                assertThat(match.getIndex().string(), equalTo("my-index"));
            }
        }
    }

    @Test
    public void testPercolateSortingWithNoSize() throws Exception {
        createIndex("my-index");
        ensureGreen();

        client().prepareIndex("my-index", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("level", 1).endObject())
                .execute().actionGet();
        client().prepareIndex("my-index", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("level", 2).endObject())
                .execute().actionGet();
        refresh();

        PercolateResponse response = client().preparePercolate().setIndices("my-index").setDocumentType("my-type")
                .setSortByScore(true)
                .setSize(2)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), scriptFunction("doc['level'].value")))
                .execute().actionGet();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches()[0].getId().string(), equalTo("2"));
        assertThat(response.getMatches()[0].getScore(), equalTo(2f));
        assertThat(response.getMatches()[1].getId().string(), equalTo("1"));
        assertThat(response.getMatches()[1].getScore(), equalTo(1f));

        response = client().preparePercolate().setIndices("my-index").setDocumentType("my-type")
                .setSortByScore(true)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), scriptFunction("doc['level'].value")))
                .execute().actionGet();
        assertThat(response.getCount(), equalTo(0l));
        assertThat(response.getShardFailures().length, greaterThan(0));
        for (ShardOperationFailedException failure : response.getShardFailures()) {
            assertThat(failure.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(failure.reason(), containsString("Can't sort if size isn't specified"));
        }
    }

    @Test
    public void testPercolateSorting_unsupportedField() throws Exception {
        client().admin().indices().prepareCreate("my-index")
                .addMapping("my-type", "field", "type=string")
                .addMapping(PercolatorService.TYPE_NAME, "level", "type=integer", "query", "type=object,enabled=false")
                .get();
        ensureGreen();

        client().prepareIndex("my-index", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("level", 1).endObject())
                .get();
        client().prepareIndex("my-index", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("level", 2).endObject())
                .get();
        refresh();

        PercolateResponse response = client().preparePercolate().setIndices("my-index").setDocumentType("my-type")
                .setSize(2)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), scriptFunction("doc['level'].value")))
                .addSort(SortBuilders.fieldSort("level"))
                .get();

        assertThat(response.getShardFailures().length, equalTo(getNumShards("my-index").numPrimaries));
        assertThat(response.getShardFailures()[0].status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(response.getShardFailures()[0].reason(), containsString("Only _score desc is supported"));
    }

    @Test
    public void testPercolateOnEmptyIndex() throws Exception {
        client().admin().indices().prepareCreate("my-index").execute().actionGet();
        ensureGreen();

        PercolateResponse response = client().preparePercolate().setIndices("my-index").setDocumentType("my-type")
                .setSortByScore(true)
                .setSize(2)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), scriptFunction("doc['level'].value")))
                .execute().actionGet();
        assertMatchCount(response, 0l);
    }

    @Test
    public void testPercolateNotEmptyIndexButNoRefresh() throws Exception {
        client().admin().indices().prepareCreate("my-index")
                .setSettings(settingsBuilder().put("index.refresh_interval", -1))
                .execute().actionGet();
        ensureGreen();

        client().prepareIndex("my-index", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("level", 1).endObject())
                .execute().actionGet();

        PercolateResponse response = client().preparePercolate().setIndices("my-index").setDocumentType("my-type")
                .setSortByScore(true)
                .setSize(2)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), scriptFunction("doc['level'].value")))
                .execute().actionGet();
        assertMatchCount(response, 0l);
    }

    @Test
    public void testPercolatorWithHighlighting() throws Exception {
        StringBuilder fieldMapping = new StringBuilder("type=string")
                .append(",store=").append(randomBoolean());
        if (randomBoolean()) {
            fieldMapping.append(",term_vector=with_positions_offsets");
        } else if (randomBoolean()) {
            fieldMapping.append(",index_options=offsets");
        }
        assertAcked(prepareCreate("test").addMapping("type", "field1", fieldMapping.toString()));

        logger.info("--> register a queries");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "brown fox")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "lazy dog")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "jumps")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "dog")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "5")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "fox")).endObject())
                .execute().actionGet();

        logger.info("--> Percolate doc with field1=The quick brown fox jumps over the lazy dog");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSize(5)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()))
                .setHighlightBuilder(new HighlightBuilder().field("field1"))
                .execute().actionGet();
        assertMatchCount(response, 5l);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

        PercolateResponse.Match[] matches = response.getMatches();
        Arrays.sort(matches, new Comparator<PercolateResponse.Match>() {
            @Override
            public int compare(PercolateResponse.Match a, PercolateResponse.Match b) {
                return a.getId().compareTo(b.getId());
            }
        });

        assertThat(matches[0].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog"));
        assertThat(matches[1].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the <em>lazy</em> <em>dog</em>"));
        assertThat(matches[2].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(matches[3].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the lazy <em>dog</em>"));
        assertThat(matches[4].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown <em>fox</em> jumps over the lazy dog"));

        // Anything with percolate query isn't realtime
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Query percolate doc with field1=The quick brown fox jumps over the lazy dog");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSize(5)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()))
                .setHighlightBuilder(new HighlightBuilder().field("field1"))
                .setPercolateQuery(matchAllQuery())
                .execute().actionGet();
        assertMatchCount(response, 5l);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

        matches = response.getMatches();
        Arrays.sort(matches, new Comparator<PercolateResponse.Match>() {
            @Override
            public int compare(PercolateResponse.Match a, PercolateResponse.Match b) {
                return a.getId().compareTo(b.getId());
            }
        });

        assertThat(matches[0].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog"));
        assertThat(matches[1].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the <em>lazy</em> <em>dog</em>"));
        assertThat(matches[2].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(matches[3].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the lazy <em>dog</em>"));
        assertThat(matches[4].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown <em>fox</em> jumps over the lazy dog"));

        logger.info("--> Query percolate with score for doc with field1=The quick brown fox jumps over the lazy dog");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSize(5)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()))
                .setHighlightBuilder(new HighlightBuilder().field("field1"))
                .setPercolateQuery(functionScoreQuery(matchAllQuery()).add(new FactorBuilder().boostFactor(5.5f)))
                .setScore(true)
                .execute().actionGet();
        assertNoFailures(response);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

        matches = response.getMatches();
        Arrays.sort(matches, new Comparator<PercolateResponse.Match>() {
            @Override
            public int compare(PercolateResponse.Match a, PercolateResponse.Match b) {
                return a.getId().compareTo(b.getId());
            }
        });

        assertThat(matches[0].getScore(), equalTo(5.5f));
        assertThat(matches[0].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog"));
        assertThat(matches[1].getScore(), equalTo(5.5f));
        assertThat(matches[1].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the <em>lazy</em> <em>dog</em>"));
        assertThat(matches[2].getScore(), equalTo(5.5f));
        assertThat(matches[2].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(matches[3].getScore(), equalTo(5.5f));
        assertThat(matches[3].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the lazy <em>dog</em>"));
        assertThat(matches[4].getScore(), equalTo(5.5f));
        assertThat(matches[4].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown <em>fox</em> jumps over the lazy dog"));

        logger.info("--> Top percolate for doc with field1=The quick brown fox jumps over the lazy dog");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSize(5)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()))
                .setHighlightBuilder(new HighlightBuilder().field("field1"))
                .setPercolateQuery(functionScoreQuery(matchAllQuery()).add(new FactorBuilder().boostFactor(5.5f)))
                .setSortByScore(true)
                .execute().actionGet();
        assertMatchCount(response, 5l);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

        matches = response.getMatches();
        Arrays.sort(matches, new Comparator<PercolateResponse.Match>() {
            @Override
            public int compare(PercolateResponse.Match a, PercolateResponse.Match b) {
                return a.getId().compareTo(b.getId());
            }
        });

        assertThat(matches[0].getScore(), equalTo(5.5f));
        assertThat(matches[0].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog"));
        assertThat(matches[1].getScore(), equalTo(5.5f));
        assertThat(matches[1].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the <em>lazy</em> <em>dog</em>"));
        assertThat(matches[2].getScore(), equalTo(5.5f));
        assertThat(matches[2].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(matches[3].getScore(), equalTo(5.5f));
        assertThat(matches[3].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the lazy <em>dog</em>"));
        assertThat(matches[4].getScore(), equalTo(5.5f));
        assertThat(matches[4].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown <em>fox</em> jumps over the lazy dog"));

        logger.info("--> Top percolate for doc with field1=The quick brown fox jumps over the lazy dog");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSize(5)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()))
                .setHighlightBuilder(new HighlightBuilder().field("field1").highlightQuery(QueryBuilders.matchQuery("field1", "jumps")))
                .setPercolateQuery(functionScoreQuery(matchAllQuery()).add(new FactorBuilder().boostFactor(5.5f)))
                .setSortByScore(true)
                .execute().actionGet();
        assertMatchCount(response, 5l);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

        matches = response.getMatches();
        Arrays.sort(matches, new Comparator<PercolateResponse.Match>() {
            @Override
            public int compare(PercolateResponse.Match a, PercolateResponse.Match b) {
                return a.getId().compareTo(b.getId());
            }
        });

        assertThat(matches[0].getScore(), equalTo(5.5f));
        assertThat(matches[0].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(matches[1].getScore(), equalTo(5.5f));
        assertThat(matches[1].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(matches[2].getScore(), equalTo(5.5f));
        assertThat(matches[2].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(matches[3].getScore(), equalTo(5.5f));
        assertThat(matches[3].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(matches[4].getScore(), equalTo(5.5f));
        assertThat(matches[4].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));

        // Highlighting an existing doc
        client().prepareIndex("test", "type", "1")
                .setSource(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject())
                .get();

        logger.info("--> Top percolate for doc with field1=The quick brown fox jumps over the lazy dog");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSize(5)
                .setGetRequest(Requests.getRequest("test").type("type").id("1"))
                .setHighlightBuilder(new HighlightBuilder().field("field1"))
                .setPercolateQuery(functionScoreQuery(matchAllQuery()).add(new FactorBuilder().boostFactor(5.5f)))
                .setSortByScore(true)
                .execute().actionGet();
        assertMatchCount(response, 5l);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

        matches = response.getMatches();
        Arrays.sort(matches, new Comparator<PercolateResponse.Match>() {
            @Override
            public int compare(PercolateResponse.Match a, PercolateResponse.Match b) {
                return a.getId().compareTo(b.getId());
            }
        });

        assertThat(matches[0].getScore(), equalTo(5.5f));
        assertThat(matches[0].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog"));
        assertThat(matches[1].getScore(), equalTo(5.5f));
        assertThat(matches[1].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the <em>lazy</em> <em>dog</em>"));
        assertThat(matches[2].getScore(), equalTo(5.5f));
        assertThat(matches[2].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(matches[3].getScore(), equalTo(5.5f));
        assertThat(matches[3].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the lazy <em>dog</em>"));
        assertThat(matches[4].getScore(), equalTo(5.5f));
        assertThat(matches[4].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown <em>fox</em> jumps over the lazy dog"));
    }

    public static String[] convertFromTextArray(PercolateResponse.Match[] matches, String index) {
        if (matches.length == 0) {
            return Strings.EMPTY_ARRAY;
        }
        String[] strings = new String[matches.length];
        for (int i = 0; i < matches.length; i++) {
            assertEquals(index, matches[i].getIndex().string());
            strings[i] = matches[i].getId().string();
        }
        return strings;
    }

    @Test
    public void percolateNonMatchingConstantScoreQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("doc", "message", "type=string"));
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject()
                        .field("query", QueryBuilders.constantScoreQuery(FilterBuilders.andFilter(
                                FilterBuilders.queryFilter(QueryBuilders.queryStringQuery("root")),
                                FilterBuilders.termFilter("message", "tree"))))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateResponse percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("doc")
                .setSource(jsonBuilder().startObject()
                        .startObject("doc").field("message", "A new bonsai tree ").endObject()
                        .endObject())
                .execute().actionGet();
        assertNoFailures(percolate);
        assertMatchCount(percolate, 0l);
    }

    @Test
    public void testNestedPercolation() throws IOException {
        initNestedIndexAndPercolation();
        PercolateResponse response = client().preparePercolate().setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(getNotMatchingNestedDoc())).setIndices("nestedindex").setDocumentType("company").get();
        assertEquals(response.getMatches().length, 0);
        response = client().preparePercolate().setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(getMatchingNestedDoc())).setIndices("nestedindex").setDocumentType("company").get();
        assertEquals(response.getMatches().length, 1);
        assertEquals(response.getMatches()[0].getId().string(), "Q");
    }

    @Test
    public void makeSureNonNestedDocumentDoesNotTriggerAssertion() throws IOException {
        initNestedIndexAndPercolation();
        XContentBuilder doc = jsonBuilder();
        doc.startObject();
        doc.field("some_unnested_field", "value");
        PercolateResponse response = client().preparePercolate().setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(doc)).setIndices("nestedindex").setDocumentType("company").get();
        assertNoFailures(response);
    }

    @Test
    public void testNestedPercolationOnExistingDoc() throws IOException {
        initNestedIndexAndPercolation();
        client().prepareIndex("nestedindex", "company", "notmatching").setSource(getNotMatchingNestedDoc()).get();
        client().prepareIndex("nestedindex", "company", "matching").setSource(getMatchingNestedDoc()).get();
        refresh();
        PercolateResponse response = client().preparePercolate().setGetRequest(Requests.getRequest("nestedindex").type("company").id("notmatching")).setDocumentType("company").setIndices("nestedindex").get();
        assertEquals(response.getMatches().length, 0);
        response = client().preparePercolate().setGetRequest(Requests.getRequest("nestedindex").type("company").id("matching")).setDocumentType("company").setIndices("nestedindex").get();
        assertEquals(response.getMatches().length, 1);
        assertEquals(response.getMatches()[0].getId().string(), "Q");
    }

    @Test
    public void testPercolationWithDynamicTemplates() throws Exception {
        assertAcked(prepareCreate("idx").addMapping("type", jsonBuilder().startObject().startObject("type")
                .field("dynamic", false)
                .startObject("properties")
                .startObject("custom")
                .field("dynamic", true)
                .field("type", "object")
                .field("include_in_all", false)
                .endObject()
                .endObject()
                .startArray("dynamic_templates")
                .startObject()
                .startObject("custom_fields")
                .field("path_match", "custom.*")
                .startObject("mapping")
                .field("index", "not_analyzed")
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject().endObject()));
        ensureGreen("idx");

        try {
        client().prepareIndex("idx", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", QueryBuilders.queryStringQuery("color:red")).endObject())
                .get();
            fail();
        } catch (PercolatorException e) {

        }

        PercolateResponse percolateResponse = client().preparePercolate().setDocumentType("type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(jsonBuilder().startObject().startObject("custom").field("color", "blue").endObject().endObject()))
                .get();

        assertMatchCount(percolateResponse, 0l);
        assertThat(percolateResponse.getMatches(), arrayWithSize(0));
        waitForConcreteMappingsOnAll("idx", "type", "custom.color");

        // The previous percolate request introduced the custom.color field, so now we register the query again
        // and the field name `color` will be resolved to `custom.color` field in mapping via smart field mapping resolving.
        client().prepareIndex("idx", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", QueryBuilders.queryStringQuery("custom.color:red")).endObject())
                .get();
        client().prepareIndex("idx", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", QueryBuilders.queryStringQuery("custom.color:blue")).field("type", "type").endObject())
                .get();

        // The second request will yield a match, since the query during the proper field during parsing.
        percolateResponse = client().preparePercolate().setDocumentType("type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(jsonBuilder().startObject().startObject("custom").field("color", "blue").endObject().endObject()))
                .get();

        assertMatchCount(percolateResponse, 1l);
        assertThat(percolateResponse.getMatches()[0].getId().string(), equalTo("2"));
    }

    @Test
    public void testUpdateMappingDynamicallyWhilePercolating() throws Exception {
        createIndex("test");
        ensureSearchable();

        // percolation source
        XContentBuilder percolateDocumentSource = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", 1)
                .field("field2", "value")
                .endObject().endObject();

        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(percolateDocumentSource).execute().actionGet();
        assertAllSuccessful(response);
        assertMatchCount(response, 0l);
        assertThat(response.getMatches(), arrayWithSize(0));

        waitForMappingOnMaster("test", "type1");

        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings("test").get();
        assertThat(mappingsResponse.getMappings().get("test"), notNullValue());
        assertThat(mappingsResponse.getMappings().get("test").get("type1"), notNullValue());
        assertThat(mappingsResponse.getMappings().get("test").get("type1").getSourceAsMap().isEmpty(), is(false));
        Map<String, Object> properties = (Map<String, Object>) mappingsResponse.getMappings().get("test").get("type1").getSourceAsMap().get("properties");
        assertThat(((Map<String, String>) properties.get("field1")).get("type"), equalTo("long"));
        assertThat(((Map<String, String>) properties.get("field2")).get("type"), equalTo("string"));
    }

    @Test
    public void testDontReportDeletedPercolatorDocs() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .get();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .get();
        refresh();

        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field", "value").endObject()))
                .setPercolateFilter(FilterBuilders.matchAllFilter())
                .get();
        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1"));
    }

    @Test
    public void testAddQueryWithNoMapping() throws Exception {
        client().admin().indices().prepareCreate("test").get();
        ensureGreen();

        try {
            client().prepareIndex("test", PercolatorService.TYPE_NAME)
                    .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "value")).endObject())
                    .get();
            fail();
        } catch (PercolatorException e) {
            assertThat(e.getRootCause(), instanceOf(QueryParsingException.class));
        }

        try {
            client().prepareIndex("test", PercolatorService.TYPE_NAME)
                    .setSource(jsonBuilder().startObject().field("query", rangeQuery("field1").from(0).to(1)).endObject())
                    .get();
            fail();
        } catch (PercolatorException e) {
            assertThat(e.getRootCause(), instanceOf(QueryParsingException.class));
        }
    }

    @Test
    public void testPercolatorQueryWithNowRange() throws Exception {
        client().admin().indices().prepareCreate("test")
                .addMapping("my-type", "timestamp", "type=date")
                .get();
        ensureGreen();

        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", rangeQuery("timestamp").from("now-1d").to("now")).endObject())
                .get();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", constantScoreQuery(rangeFilter("timestamp").from("now-1d").to("now"))).endObject())
                .get();

        logger.info("--> Percolate doc with field1=b");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("my-type")
                .setPercolateDoc(docBuilder().setDoc("timestamp", System.currentTimeMillis()))
                .get();
        assertMatchCount(response, 2l);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2"));
    }

    void initNestedIndexAndPercolation() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject().startObject("properties").startObject("companyname").field("type", "string").endObject()
                .startObject("employee").field("type", "nested").startObject("properties")
                .startObject("name").field("type", "string").endObject().endObject().endObject().endObject()
                .endObject();

        assertAcked(client().admin().indices().prepareCreate("nestedindex").addMapping("company", mapping));
        ensureGreen("nestedindex");

        client().prepareIndex("nestedindex", PercolatorService.TYPE_NAME, "Q").setSource(jsonBuilder().startObject()
                .field("query", QueryBuilders.nestedQuery("employee", QueryBuilders.matchQuery("employee.name", "virginia potts").operator(MatchQueryBuilder.Operator.AND)).scoreMode("avg")).endObject()).get();

        refresh();

    }

    XContentBuilder getMatchingNestedDoc() throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder();
        doc.startObject().field("companyname", "stark").startArray("employee")
                .startObject().field("name", "virginia potts").endObject()
                .startObject().field("name", "tony stark").endObject()
                .endArray().endObject();
        return doc;
    }

    XContentBuilder getNotMatchingNestedDoc() throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder();
        doc.startObject().field("companyname", "notstark").startArray("employee")
                .startObject().field("name", "virginia stark").endObject()
                .startObject().field("name", "tony potts").endObject()
                .endArray().endObject();
        return doc;
    }

    // issue
    @Test
    public void testNestedDocFilter() throws IOException {
        String mapping = "{\n" +
                "    \"doc\": {\n" +
                "      \"properties\": {\n" +
                "        \"name\": {\"type\":\"string\"},\n" +
                "        \"persons\": {\n" +
                "          \"type\": \"nested\"\n," +
                "          \"properties\" : {\"foo\" : {\"type\" : \"string\"}}" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }";
        String doc = "{\n" +
                "    \"name\": \"obama\",\n" +
                "    \"persons\": [\n" +
                "      {\n" +
                "        \"foo\": \"bar\"\n" +
                "      }\n" +
                "    ]\n" +
                "  }";
        String q1 = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": {\n" +
                "        \"match\": {\n" +
                "          \"name\": \"obama\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "\"text\":\"foo\""+
                "}";
        String q2 = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must_not\": {\n" +
                "        \"match\": {\n" +
                "          \"name\": \"obama\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "\"text\":\"foo\""+
                "}";
        String q3 = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": {\n" +
                "        \"match\": {\n" +
                "          \"persons.foo\": \"bar\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "\"text\":\"foo\""+
                "}";
        String q4 = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must_not\": {\n" +
                "        \"match\": {\n" +
                "          \"persons.foo\": \"bar\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "\"text\":\"foo\""+
                "}";
        String q5 = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": {\n" +
                "        \"nested\": {\n" +
                "          \"path\": \"persons\",\n" +
                "          \"query\": {\n" +
                "            \"match\": {\n" +
                "              \"persons.foo\": \"bar\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "\"text\":\"foo\""+
                "}";
        String q6 = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must_not\": {\n" +
                "        \"nested\": {\n" +
                "          \"path\": \"persons\",\n" +
                "          \"query\": {\n" +
                "            \"match\": {\n" +
                "              \"persons.foo\": \"bar\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "\"text\":\"foo\""+
                "}";
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("doc", mapping));
        ensureGreen("test");
        client().prepareIndex("test", PercolatorService.TYPE_NAME).setSource(q1).setId("q1").get();
        client().prepareIndex("test", PercolatorService.TYPE_NAME).setSource(q2).setId("q2").get();
        client().prepareIndex("test", PercolatorService.TYPE_NAME).setSource(q3).setId("q3").get();
        client().prepareIndex("test", PercolatorService.TYPE_NAME).setSource(q4).setId("q4").get();
        client().prepareIndex("test", PercolatorService.TYPE_NAME).setSource(q5).setId("q5").get();
        client().prepareIndex("test", PercolatorService.TYPE_NAME).setSource(q6).setId("q6").get();
        refresh();
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("doc")
                .setPercolateDoc(docBuilder().setDoc(doc))
                .get();
        assertMatchCount(response, 3l);
        Set<String> expectedIds = new HashSet<>();
        expectedIds.add("q1");
        expectedIds.add("q4");
        expectedIds.add("q5");
        for (PercolateResponse.Match match : response.getMatches()) {
            assertTrue(expectedIds.remove(match.getId().string()));
        }
        assertTrue(expectedIds.isEmpty());
        response = client().preparePercolate().setOnlyCount(true)
                .setIndices("test").setDocumentType("doc")
                .setPercolateDoc(docBuilder().setDoc(doc))
                .get();
        assertMatchCount(response, 3l);
        response = client().preparePercolate().setScore(randomBoolean()).setSortByScore(randomBoolean()).setOnlyCount(randomBoolean()).setSize(10).setPercolateQuery(QueryBuilders.termQuery("text", "foo"))
                .setIndices("test").setDocumentType("doc")
                .setPercolateDoc(docBuilder().setDoc(doc))
                .get();
        assertMatchCount(response, 3l);
    }

    @Test
    public void testMapUnmappedFieldAsString() throws IOException{
        // If index.percolator.map_unmapped_fields_as_string is set to true, unmapped field is mapped as an analyzed string.
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder()
                .put(indexSettings())
                .put("index.percolator.map_unmapped_fields_as_string", true);
        assertAcked(prepareCreate("test")
                .setSettings(settings));
        client().prepareIndex("test", PercolatorService.TYPE_NAME)
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "value")).endObject()).get();
        logger.info("--> Percolate doc with field1=value");
        PercolateResponse response1 = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "value").endObject()))
                .execute().actionGet();
        assertMatchCount(response1, 1l);
        assertThat(response1.getMatches(), arrayWithSize(1));

    }
}

