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

import com.vividsolutions.jts.geom.Coordinate;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.functionscore.WeightBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.percolator.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.yamlBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.elasticsearch.percolator.PercolatorTestUtil.convertFromTextArray;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.percolator.PercolatorTestUtil.assertMatchCount;
import static org.elasticsearch.percolator.PercolatorTestUtil.preparePercolate;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class PercolatorIT extends ESIntegTestCase {

    private final static String INDEX_NAME = "queries";
    private final static String TYPE_NAME = "query";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(PercolatorPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(PercolatorPlugin.class);
    }

    public void testSimple1() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME).addMapping(TYPE_NAME, "query", "type=percolator").get();
        ensureGreen();

        logger.info("--> Add dummy doc");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1").setSource("field1", "value").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        refresh();

        logger.info("--> Percolate doc with field1=b");
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType(TYPE_NAME)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "4"));

        logger.info("--> Percolate doc with field1=c");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType(TYPE_NAME)
                .setPercolateDoc(docBuilder().setDoc(yamlBuilder().startObject().field("field1", "c").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate doc with field1=b c");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType(TYPE_NAME)
                .setPercolateDoc(docBuilder().setDoc(smileBuilder().startObject().field("field1", "b c").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 4L);
        assertThat(response.getMatches(), arrayWithSize(4));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4"));

        logger.info("--> Percolate doc with field1=d");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType(TYPE_NAME)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "d").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 1L);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContaining("4"));

        logger.info("--> Percolate non existing doc");
        try {
            preparePercolate(client())
                    .setIndices(INDEX_NAME).setDocumentType(TYPE_NAME)
                    .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("5"))
                    .execute().actionGet();
            fail("Exception should have been thrown");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("percolate document [queries/type/5] doesn't exist"));
        }
    }

    public void testSimple2() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME)
                .addMapping("type1", "field1", "type=long", "field2", "type=text")
                .addMapping(TYPE_NAME, "query", "type=percolator")
        );
        ensureGreen();

        // introduce the doc
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", 1)
                .field("field2", "value")
                .endObject().endObject();

        PercolateResponse response = preparePercolate(client()).setSource(doc)
                .setIndices(INDEX_NAME).setDocumentType(TYPE_NAME)
                .execute().actionGet();
        assertMatchCount(response, 0L);
        assertThat(response.getMatches(), emptyArray());

        // add first query...
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "test1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field2", "value")).endObject())
                .execute().actionGet();
        refresh();

        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType(TYPE_NAME)
                .setSource(doc).execute().actionGet();
        assertMatchCount(response, 1L);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContaining("test1"));

        // add second query...
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "test2")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field1", 1)).endObject())
                .execute().actionGet();
        refresh();

        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type1")
                .setSource(doc)
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("test1", "test2"));


        client().prepareDelete(INDEX_NAME, TYPE_NAME, "test2").execute().actionGet();
        refresh();
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type1")
                .setSource(doc).execute().actionGet();
        assertMatchCount(response, 1L);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContaining("test1"));
    }

    public void testPercolateQueriesWithRouting() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .setSettings(Settings.builder().put("index.number_of_shards", 2))
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .addMapping("type", "field1", "type=string")
                .execute().actionGet();
        ensureGreen();

        logger.info("--> register a queries");
        for (int i = 1; i <= 100; i++) {
            client().prepareIndex(INDEX_NAME, TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .setRouting(Integer.toString(i % 2))
                    .execute().actionGet();
        }
        refresh();

        logger.info("--> Percolate doc with no routing");
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject()))
                .setSize(100)
                .execute().actionGet();
        assertMatchCount(response, 100L);
        assertThat(response.getMatches(), arrayWithSize(100));

        logger.info("--> Percolate doc with routing=0");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject()))
                        .setSize(100)
                        .setRouting("0")
                        .execute().actionGet();
        assertMatchCount(response, 50L);
        assertThat(response.getMatches(), arrayWithSize(50));

        logger.info("--> Percolate doc with routing=1");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject()))
                .setSize(100)
                .setRouting("1")
                .execute().actionGet();
        assertMatchCount(response, 50L);
        assertThat(response.getMatches(), arrayWithSize(50));
    }

    public void storePercolateQueriesOnRecreatedIndex() throws Exception {
        prepareCreate(INDEX_NAME).addMapping(TYPE_NAME, "query", "type=percolator").get();
        ensureGreen();

        client().prepareIndex(INDEX_NAME, "test", "1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> register a query");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "kuku1")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();

        cluster().wipeIndices("test");
        createIndex("test");
        ensureGreen();

        client().prepareIndex(INDEX_NAME, "test", "1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> register a query");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "kuku2")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();
    }

    // see #2814
    public void testPercolateCustomAnalyzer() throws Exception {
        Builder builder = Settings.builder();
        builder.put("index.analysis.analyzer.lwhitespacecomma.tokenizer", "whitespacecomma");
        builder.putArray("index.analysis.analyzer.lwhitespacecomma.filter", "lowercase");
        builder.put("index.analysis.tokenizer.whitespacecomma.type", "pattern");
        builder.put("index.analysis.tokenizer.whitespacecomma.pattern", "(,|\\s+)");

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .startObject("properties")
                .startObject("filingcategory").field("type", "text").field("analyzer", "lwhitespacecomma").endObject()
                .endObject()
                .endObject().endObject();

        assertAcked(prepareCreate(INDEX_NAME).setSettings(builder)
                .addMapping("doc", mapping)
                .addMapping(TYPE_NAME, "query", "type=percolator")
        );
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject()
                        .field("source", "productizer")
                        .field("query", QueryBuilders.constantScoreQuery(QueryBuilders.queryStringQuery("filingcategory:s")))
                        .endObject())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();
        refresh();

        PercolateResponse percolate = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("doc")
                .setSource(jsonBuilder().startObject()
                        .startObject("doc").field("filingcategory", "s").endObject()
                        .field("query", termQuery("source", "productizer"))
                        .endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1L);
        assertThat(percolate.getMatches(), arrayWithSize(1));

    }

    public void testCreateIndexAndThenRegisterPercolator() throws Exception {
        prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .addMapping("type1", "field1", "type=text")
                .get();
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .execute().actionGet();
        refresh();
        SearchResponse countResponse = client().prepareSearch().setSize(0)
                .setQuery(matchAllQuery()).setTypes(TYPE_NAME)
                .execute().actionGet();
        assertThat(countResponse.getHits().totalHits(), equalTo(1L));


        for (int i = 0; i < 10; i++) {
            PercolateResponse percolate = preparePercolate(client())
                    .setIndices(INDEX_NAME).setDocumentType("type1")
                    .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                    .execute().actionGet();
            assertMatchCount(percolate, 1L);
            assertThat(percolate.getMatches(), arrayWithSize(1));
        }

        for (int i = 0; i < 10; i++) {
            PercolateResponse percolate = preparePercolate(client())
                    .setIndices(INDEX_NAME).setDocumentType("type1")
                    .setPreference("_local")
                    .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                    .execute().actionGet();
            assertMatchCount(percolate, 1L);
            assertThat(percolate.getMatches(), arrayWithSize(1));
        }


        logger.info("--> delete the index");
        client().admin().indices().prepareDelete(INDEX_NAME).execute().actionGet();
        logger.info("--> make sure percolated queries for it have been deleted as well");
        countResponse = client().prepareSearch().setSize(0)
                .setQuery(matchAllQuery()).setTypes(TYPE_NAME)
                .execute().actionGet();
        assertHitCount(countResponse, 0L);
    }

    public void testMultiplePercolators() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .addMapping("type1", "field1", "type=text")
        );
        ensureGreen();

        logger.info("--> register a query 1");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();

        logger.info("--> register a query 2");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "bubu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "green")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();

        PercolateResponse percolate = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1L);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), INDEX_NAME), arrayContaining("kuku"));

        percolate = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value2").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1L);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), INDEX_NAME), arrayContaining("bubu"));

    }

    public void testDynamicAddingRemovingQueries() throws Exception {
        assertAcked(
                prepareCreate(INDEX_NAME)
                        .addMapping("type1", "field1", "type=text")
                        .addMapping(TYPE_NAME, "query", "type=percolator")
        );
        ensureGreen();

        logger.info("--> register a query 1");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();

        PercolateResponse percolate = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1L);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), INDEX_NAME), arrayContaining("kuku"));

        logger.info("--> register a query 2");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "bubu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "green")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();

        percolate = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value2").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 1L);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), INDEX_NAME), arrayContaining("bubu"));

        logger.info("--> register a query 3");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "susu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "red")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();

        PercolateSourceBuilder sourceBuilder = new PercolateSourceBuilder()
                .setDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "value2").endObject()))
                .setQueryBuilder(termQuery("color", "red"));
        percolate = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type1")
                .setSource(sourceBuilder)
                .execute().actionGet();
        assertMatchCount(percolate, 1L);
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), INDEX_NAME), arrayContaining("susu"));

        logger.info("--> deleting query 1");
        client().prepareDelete(INDEX_NAME, TYPE_NAME, "kuku").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        percolate = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").startObject("type1")
                        .field("field1", "value1")
                        .endObject().endObject().endObject())
                .execute().actionGet();
        assertMatchCount(percolate, 0L);
        assertThat(percolate.getMatches(), emptyArray());
    }

    public void testPercolatingExistingDocs() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .get();
        ensureGreen();

        logger.info("--> Adding docs");
        client().prepareIndex(INDEX_NAME, "type", "1").setSource("field1", "b").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "2").setSource("field1", "c").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "3").setSource("field1", "b c").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "4").setSource("field1", "d").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        refresh();

        logger.info("--> Percolate existing doc with id 1");
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("1"))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "4"));

        logger.info("--> Percolate existing doc with id 2");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("2"))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate existing doc with id 3");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("3"))
                .execute().actionGet();
        assertMatchCount(response, 4L);
        assertThat(response.getMatches(), arrayWithSize(4));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4"));

        logger.info("--> Percolate existing doc with id 4");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("4"))
                .execute().actionGet();
        assertMatchCount(response, 1L);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContaining("4"));
    }

    public void testPercolatingExistingDocs_routing() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .execute().actionGet();
        ensureGreen();

        logger.info("--> Adding docs");
        client().prepareIndex(INDEX_NAME, "type", "1").setSource("field1", "b").setRouting("4").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "2").setSource("field1", "c").setRouting("3").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "3").setSource("field1", "b c").setRouting("2").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "4").setSource("field1", "d").setRouting("1").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        refresh();

        logger.info("--> Percolate existing doc with id 1");
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("1").routing("4"))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "4"));

        logger.info("--> Percolate existing doc with id 2");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("2").routing("3"))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate existing doc with id 3");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("3").routing("2"))
                .execute().actionGet();
        assertMatchCount(response, 4L);
        assertThat(response.getMatches(), arrayWithSize(4));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4"));

        logger.info("--> Percolate existing doc with id 4");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("4").routing("1"))
                .execute().actionGet();
        assertMatchCount(response, 1L);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContaining("4"));
    }

    public void testPercolatingExistingDocs_versionCheck() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .get();
        ensureGreen();

        logger.info("--> Adding docs");
        client().prepareIndex(INDEX_NAME, "type", "1").setSource("field1", "b").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "2").setSource("field1", "c").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "3").setSource("field1", "b c").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "4").setSource("field1", "d").execute().actionGet();

        logger.info("--> registering queries");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        refresh();

        logger.info("--> Percolate existing doc with id 2 and version 1");
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("2").version(1L))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate existing doc with id 2 and version 2");
        try {
            preparePercolate(client())
                    .setIndices(INDEX_NAME).setDocumentType("type")
                    .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("2").version(2L))
                    .execute().actionGet();
            fail("Error should have been thrown");
        } catch (VersionConflictEngineException e) {
        }

        logger.info("--> Index doc with id for the second time");
        client().prepareIndex(INDEX_NAME, "type", "2").setSource("field1", "c").execute().actionGet();

        logger.info("--> Percolate existing doc with id 2 and version 2");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("2").version(2L))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("2", "4"));
    }

    public void testPercolateMultipleIndicesAndAliases() throws Exception {
        prepareCreate(INDEX_NAME).addMapping(TYPE_NAME, "query", "type=percolator").get();
        prepareCreate(INDEX_NAME + "2").addMapping(TYPE_NAME, "query", "type=percolator").get();
        ensureGreen();

        logger.info("--> registering queries");
        for (int i = 1; i <= 10; i++) {
            String index = i % 2 == 0 ? INDEX_NAME : INDEX_NAME + "2";
            client().prepareIndex(index, TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .execute().actionGet();
        }
        refresh();

        logger.info("--> Percolate doc to index test1");
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 5L);
        assertThat(response.getMatches(), arrayWithSize(5));

        logger.info("--> Percolate doc to index test2");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME + "2").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 5L);
        assertThat(response.getMatches(), arrayWithSize(5));

        logger.info("--> Percolate doc to index test1 and test2");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME, INDEX_NAME + "2").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 10L);
        assertThat(response.getMatches(), arrayWithSize(10));

        logger.info("--> Percolate doc to index test2 and test3, with ignore missing");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME , INDEX_NAME + "3").setDocumentType("type")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 5L);
        assertThat(response.getMatches(), arrayWithSize(5));

        logger.info("--> Adding aliases");
        IndicesAliasesResponse aliasesResponse = client().admin().indices().prepareAliases()
                .addAlias(INDEX_NAME, "my-alias1")
                .addAlias(INDEX_NAME + "2", "my-alias1")
                .addAlias(INDEX_NAME + "2", "my-alias2")
                .setTimeout(TimeValue.timeValueHours(10))
                .execute().actionGet();
        assertTrue(aliasesResponse.isAcknowledged());

        logger.info("--> Percolate doc to my-alias1");
        response = preparePercolate(client())
                .setIndices("my-alias1").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 10L);
        assertThat(response.getMatches(), arrayWithSize(10));
        for (PercolateResponse.Match match : response) {
            assertThat(match.getIndex().string(), anyOf(equalTo(INDEX_NAME), equalTo(INDEX_NAME + "2")));
        }

        logger.info("--> Percolate doc to my-alias2");
        response = preparePercolate(client())
                .setIndices("my-alias2").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertMatchCount(response, 5L);
        assertThat(response.getMatches(), arrayWithSize(5));
        for (PercolateResponse.Match match : response) {
            assertThat(match.getIndex().string(), equalTo(INDEX_NAME + "2"));
        }
    }

    public void testPercolateWithAliasFilter() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME)
                        .addMapping(TYPE_NAME, "query", "type=percolator")
                        .addMapping("my-type", "a", "type=keyword")
                        .addAlias(new Alias("a").filter(QueryBuilders.termQuery("a", "a")))
                        .addAlias(new Alias("b").filter(QueryBuilders.termQuery("a", "b")))
                        .addAlias(new Alias("c").filter(QueryBuilders.termQuery("a", "c")))
        );
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("a", "a").endObject())
                .get();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("a", "b").endObject())
                .get();
        refresh();

        // Specifying only the document to percolate and no filter, sorting or aggs, the queries are retrieved from
        // memory directly. Otherwise we need to retrieve those queries from lucene to be able to execute filters,
        // aggregations and sorting on top of them. So this test a different code execution path.
        PercolateResponse response = preparePercolate(client())
                .setIndices("a")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(1L));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));

        response = preparePercolate(client())
                .setIndices("b")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(1L));
        assertThat(response.getMatches()[0].getId().string(), equalTo("2"));


        response = preparePercolate(client())
                .setIndices("c")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(0L));

        // Testing that the alias filter and the filter specified while percolating are both taken into account.
        response = preparePercolate(client())
                .setIndices("a")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .setPercolateQuery(QueryBuilders.matchAllQuery())
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(1L));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));

        response = preparePercolate(client())
                .setIndices("b")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .setPercolateQuery(QueryBuilders.matchAllQuery())
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(1L));
        assertThat(response.getMatches()[0].getId().string(), equalTo("2"));


        response = preparePercolate(client())
                .setIndices("c")
                .setDocumentType("my-type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .setPercolateQuery(QueryBuilders.matchAllQuery())
                .get();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(0L));
    }

    public void testCountPercolation() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .get();
        ensureGreen();

        logger.info("--> Add dummy doc");
        client().prepareIndex(INDEX_NAME, "type", "1").setSource("field1", "value").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        refresh();

        logger.info("--> Count percolate doc with field1=b");
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate doc with field1=c");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(yamlBuilder().startObject().field("field1", "c").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate doc with field1=b c");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(smileBuilder().startObject().field("field1", "b c").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 4L);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate doc with field1=d");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "d").endObject()))
                .execute().actionGet();
        assertMatchCount(response, 1L);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate non existing doc");
        try {
            preparePercolate(client())
                    .setIndices(INDEX_NAME).setDocumentType("type").setOnlyCount(true)
                    .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("5"))
                    .execute().actionGet();
            fail("Exception should have been thrown");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("percolate document [" + INDEX_NAME + "/type/5] doesn't exist"));
        }
    }

    public void testCountPercolatingExistingDocs() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .get();
        ensureGreen();

        logger.info("--> Adding docs");
        client().prepareIndex(INDEX_NAME, "type", "1").setSource("field1", "b").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "2").setSource("field1", "c").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "3").setSource("field1", "b c").execute().actionGet();
        client().prepareIndex(INDEX_NAME, "type", "4").setSource("field1", "d").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        refresh();

        logger.info("--> Count percolate existing doc with id 1");
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("1"))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate existing doc with id 2");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("2"))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate existing doc with id 3");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("3"))
                .execute().actionGet();
        assertMatchCount(response, 4L);
        assertThat(response.getMatches(), nullValue());

        logger.info("--> Count percolate existing doc with id 4");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("4"))
                .execute().actionGet();
        assertMatchCount(response, 1L);
        assertThat(response.getMatches(), nullValue());
    }

    public void testPercolateSizingWithQueryAndFilter() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .get();
        ensureGreen();

        int numLevels = randomIntBetween(1, 25);
        long numQueriesPerLevel = randomIntBetween(10, 250);
        long totalQueries = numLevels * numQueriesPerLevel;
        logger.info("--> register {} queries", totalQueries);
        for (int level = 1; level <= numLevels; level++) {
            for (int query = 1; query <= numQueriesPerLevel; query++) {
                client().prepareIndex(INDEX_NAME, TYPE_NAME, level + "-" + query)
                        .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("level", level).endObject())
                        .execute().actionGet();
            }
        }
        refresh();

        boolean onlyCount = randomBoolean();
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("my-type")
                .setOnlyCount(onlyCount)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .setSize((int) totalQueries)
                .execute().actionGet();
        assertMatchCount(response, totalQueries);
        if (!onlyCount) {
            assertThat(response.getMatches().length, equalTo((int) totalQueries));
        }

        int size = randomIntBetween(0, (int) totalQueries - 1);
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("my-type")
                .setOnlyCount(onlyCount)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .setSize(size)
                .execute().actionGet();
        assertMatchCount(response, totalQueries);
        if (!onlyCount) {
            assertThat(response.getMatches().length, equalTo(size));
        }

        // The query / filter capabilities are NOT in realtime
        refresh();

        int runs = randomIntBetween(3, 16);
        for (int i = 0; i < runs; i++) {
            onlyCount = randomBoolean();
            response = preparePercolate(client())
                    .setIndices(INDEX_NAME).setDocumentType("my-type")
                    .setOnlyCount(onlyCount)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateQuery(termQuery("level", 1 + randomInt(numLevels - 1)))
                    .setSize((int) numQueriesPerLevel)
                    .execute().actionGet();
            assertMatchCount(response, numQueriesPerLevel);
            if (!onlyCount) {
                assertThat(response.getMatches().length, equalTo((int) numQueriesPerLevel));
            }
        }

        for (int i = 0; i < runs; i++) {
            onlyCount = randomBoolean();
            response = preparePercolate(client())
                    .setIndices(INDEX_NAME).setDocumentType("my-type")
                    .setOnlyCount(onlyCount)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateQuery(termQuery("level", 1 + randomInt(numLevels - 1)))
                    .setSize((int) numQueriesPerLevel)
                    .execute().actionGet();
            assertMatchCount(response, numQueriesPerLevel);
            if (!onlyCount) {
                assertThat(response.getMatches().length, equalTo((int) numQueriesPerLevel));
            }
        }

        for (int i = 0; i < runs; i++) {
            onlyCount = randomBoolean();
            size = randomIntBetween(0, (int) numQueriesPerLevel - 1);
            response = preparePercolate(client())
                    .setIndices(INDEX_NAME).setDocumentType("my-type")
                    .setOnlyCount(onlyCount)
                    .setSize(size)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateQuery(termQuery("level", 1 + randomInt(numLevels - 1)))
                    .execute().actionGet();
            assertMatchCount(response, numQueriesPerLevel);
            if (!onlyCount) {
                assertThat(response.getMatches().length, equalTo(size));
            }
        }
    }

    public void testPercolateScoreAndSorting() throws Exception {
        prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .get();
        ensureGreen();

        // Add a dummy doc, that shouldn't never interfere with percolate operations.
        client().prepareIndex(INDEX_NAME, "my-type", "1").setSource("field", "value").execute().actionGet();

        Map<Integer, NavigableSet<Integer>> controlMap = new HashMap<>();
        long numQueries = randomIntBetween(100, 250);
        logger.info("--> register {} queries", numQueries);
        for (int i = 0; i < numQueries; i++) {
            int value = randomInt(10);
            client().prepareIndex(INDEX_NAME, TYPE_NAME, Integer.toString(i))
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
            PercolateResponse response = preparePercolate(client()).setIndices(INDEX_NAME).setDocumentType("my-type")
                    .setScore(true)
                    .setSize(size)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("level")))
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
            PercolateResponse response = preparePercolate(client()).setIndices(INDEX_NAME).setDocumentType("my-type")
                    .setSortByScore(true)
                    .setSize(size)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("level")))
                    .execute().actionGet();
            assertMatchCount(response, numQueries);
            assertThat(response.getMatches().length, equalTo(size));

            int expectedId = (int) (numQueries - 1);
            for (PercolateResponse.Match match : response) {
                assertThat(match.getId().string(), equalTo(Integer.toString(expectedId)));
                assertThat(match.getScore(), equalTo((float) expectedId));
                assertThat(match.getIndex().string(), equalTo(INDEX_NAME));
                expectedId--;
            }
        }


        for (int i = 0; i < runs; i++) {
            int value = usedValues.get(randomInt(usedValues.size() - 1));
            NavigableSet<Integer> levels = controlMap.get(value);
            int size = randomIntBetween(1, levels.size());
            PercolateResponse response = preparePercolate(client()).setIndices(INDEX_NAME).setDocumentType("my-type")
                    .setSortByScore(true)
                    .setSize(size)
                    .setPercolateDoc(docBuilder().setDoc("field", "value"))
                    .setPercolateQuery(
                            QueryBuilders.functionScoreQuery(matchQuery("field1", value), fieldValueFactorFunction("level"))
                                    .boostMode(
                                    CombineFunction.REPLACE))
                    .execute().actionGet();

            assertMatchCount(response, levels.size());
            assertThat(response.getMatches().length, equalTo(Math.min(levels.size(), size)));
            Iterator<Integer> levelIterator = levels.descendingIterator();
            for (PercolateResponse.Match match : response) {
                int controlLevel = levelIterator.next();
                assertThat(match.getId().string(), equalTo(Integer.toString(controlLevel)));
                assertThat(match.getScore(), equalTo((float) controlLevel));
                assertThat(match.getIndex().string(), equalTo(INDEX_NAME));
            }
        }
    }

    public void testPercolateSortingWithNoSize() throws Exception {
        prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .get();
        ensureGreen();

        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("level", 1).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("level", 2).endObject())
                .execute().actionGet();
        refresh();

        PercolateResponse response = preparePercolate(client()).setIndices(INDEX_NAME).setDocumentType("my-type")
                .setSortByScore(true)
                .setSize(2)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("level")))
                .execute().actionGet();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches()[0].getId().string(), equalTo("2"));
        assertThat(response.getMatches()[0].getScore(), equalTo(2f));
        assertThat(response.getMatches()[1].getId().string(), equalTo("1"));
        assertThat(response.getMatches()[1].getScore(), equalTo(1f));
    }

    public void testPercolateOnEmptyIndex() throws Exception {
        prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .get();
        ensureGreen();

        PercolateResponse response = preparePercolate(client()).setIndices(INDEX_NAME).setDocumentType("my-type")
                .setSortByScore(true)
                .setSize(2)
                .setPercolateDoc(docBuilder().setDoc("field", "value"))
                .setPercolateQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("level").missing(0.0)))
                .execute().actionGet();
        assertMatchCount(response, 0L);
    }

    public void testPercolatorWithHighlighting() throws Exception {
        StringBuilder fieldMapping = new StringBuilder("type=text")
                .append(",store=").append(randomBoolean());
        if (randomBoolean()) {
            fieldMapping.append(",term_vector=with_positions_offsets");
        } else if (randomBoolean()) {
            fieldMapping.append(",index_options=offsets");
        }
        assertAcked(prepareCreate(INDEX_NAME)
                .addMapping("type", "field1", fieldMapping.toString())
                .addMapping(TYPE_NAME, "query", "type=percolator")
        );

        logger.info("--> register a queries");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "brown fox")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "lazy dog")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "jumps")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "dog")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "5")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "fox")).endObject())
                .execute().actionGet();
        refresh();

        logger.info("--> Percolate doc with field1=The quick brown fox jumps over the lazy dog");
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setSize(5)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()))
                .setHighlightBuilder(new HighlightBuilder().field("field1"))
                .execute().actionGet();
        assertMatchCount(response, 5L);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

        PercolateResponse.Match[] matches = response.getMatches();
        Arrays.sort(matches, (a, b) -> a.getId().compareTo(b.getId()));

        assertThat(matches[0].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog"));
        assertThat(matches[1].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the <em>lazy</em> <em>dog</em>"));
        assertThat(matches[2].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(matches[3].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown fox jumps over the lazy <em>dog</em>"));
        assertThat(matches[4].getHighlightFields().get("field1").fragments()[0].string(), equalTo("The quick brown <em>fox</em> jumps over the lazy dog"));

        logger.info("--> Query percolate doc with field1=The quick brown fox jumps over the lazy dog");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setSize(5)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()))
                .setHighlightBuilder(new HighlightBuilder().field("field1"))
                .setPercolateQuery(matchAllQuery())
                .execute().actionGet();
        assertMatchCount(response, 5L);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

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
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setSize(5)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()))
                .setHighlightBuilder(new HighlightBuilder().field("field1"))
                .setPercolateQuery(functionScoreQuery(new WeightBuilder().setWeight(5.5f)))
                .setScore(true)
                .execute().actionGet();
        assertNoFailures(response);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

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
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setSize(5)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()))
                .setHighlightBuilder(new HighlightBuilder().field("field1"))
                .setPercolateQuery(functionScoreQuery(new WeightBuilder().setWeight(5.5f)))
                .setSortByScore(true)
                .execute().actionGet();
        assertMatchCount(response, 5L);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

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
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setSize(5)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject()))
                .setHighlightBuilder(new HighlightBuilder().field("field1").highlightQuery(QueryBuilders.matchQuery("field1", "jumps")))
                .setPercolateQuery(functionScoreQuery(new WeightBuilder().setWeight(5.5f)))
                .setSortByScore(true)
                .execute().actionGet();
        assertMatchCount(response, 5L);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

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
        client().prepareIndex(INDEX_NAME, "type", "1")
                .setSource(jsonBuilder().startObject().field("field1", "The quick brown fox jumps over the lazy dog").endObject())
                .get();
        refresh();

        logger.info("--> Top percolate for doc with field1=The quick brown fox jumps over the lazy dog");
        response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setSize(5)
                .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("1"))
                .setHighlightBuilder(new HighlightBuilder().field("field1"))
                .setPercolateQuery(functionScoreQuery(new WeightBuilder().setWeight(5.5f)))
                .setSortByScore(true)
                .execute().actionGet();
        assertMatchCount(response, 5L);
        assertThat(response.getMatches(), arrayWithSize(5));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4", "5"));

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

    public void testPercolateNonMatchingConstantScoreQuery() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .addMapping("doc", "message", "type=text"));
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject()
                        .field("query", QueryBuilders.constantScoreQuery(QueryBuilders.boolQuery()
                                .must(QueryBuilders.queryStringQuery("root"))
                                .must(QueryBuilders.termQuery("message", "tree"))))
                        .endObject())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();
        refresh();

        PercolateResponse percolate = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("doc")
                .setSource(jsonBuilder().startObject()
                        .startObject("doc").field("message", "A new bonsai tree ").endObject()
                        .endObject())
                .execute().actionGet();
        assertNoFailures(percolate);
        assertMatchCount(percolate, 0L);
    }

    public void testNestedPercolation() throws IOException {
        initNestedIndexAndPercolation();
        PercolateResponse response = preparePercolate(client()).setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(getNotMatchingNestedDoc())).setIndices(INDEX_NAME).setDocumentType("company").get();
        assertEquals(response.getMatches().length, 0);
        response = preparePercolate(client()).setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(getMatchingNestedDoc())).setIndices(INDEX_NAME).setDocumentType("company").get();
        assertEquals(response.getMatches().length, 1);
        assertEquals(response.getMatches()[0].getId().string(), "Q");
    }

    public void testNonNestedDocumentDoesNotTriggerAssertion() throws IOException {
        initNestedIndexAndPercolation();
        XContentBuilder doc = jsonBuilder();
        doc.startObject();
        doc.field("some_unnested_field", "value");
        doc.endObject();
        PercolateResponse response = preparePercolate(client()).setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(doc)).setIndices(INDEX_NAME).setDocumentType("company").get();
        assertNoFailures(response);
    }

    public void testNestedPercolationOnExistingDoc() throws IOException {
        initNestedIndexAndPercolation();
        client().prepareIndex(INDEX_NAME, "company", "notmatching").setSource(getNotMatchingNestedDoc()).get();
        client().prepareIndex(INDEX_NAME, "company", "matching").setSource(getMatchingNestedDoc()).get();
        refresh();
        PercolateResponse response = preparePercolate(client()).setGetRequest(Requests.getRequest(INDEX_NAME).type("company").id("notmatching")).setDocumentType("company").setIndices(INDEX_NAME).get();
        assertEquals(response.getMatches().length, 0);
        response = preparePercolate(client()).setGetRequest(Requests.getRequest(INDEX_NAME).type("company").id("matching")).setDocumentType("company").setIndices(INDEX_NAME).get();
        assertEquals(response.getMatches().length, 1);
        assertEquals(response.getMatches()[0].getId().string(), "Q");
    }

    public void testDontReportDeletedPercolatorDocs() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .execute().actionGet();
        ensureGreen();

        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .get();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .get();
        refresh();

        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field", "value").endObject()))
                .setPercolateQuery(QueryBuilders.matchAllQuery())
                .get();
        assertMatchCount(response, 1L);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1"));
    }

    public void testAddQueryWithNoMapping() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .get();
        ensureGreen();

        try {
            client().prepareIndex(INDEX_NAME, TYPE_NAME)
                    .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "value")).endObject())
                    .get();
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getRootCause(), instanceOf(QueryShardException.class));
        }

        try {
            client().prepareIndex(INDEX_NAME, TYPE_NAME)
                    .setSource(jsonBuilder().startObject().field("query", rangeQuery("field1").from(0).to(1)).endObject())
                    .get();
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getRootCause(), instanceOf(QueryShardException.class));
        }
    }

    public void testPercolatorQueryWithNowRange() throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping("my-type", "timestamp", "type=date,format=epoch_millis")
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .get();
        ensureGreen();

        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", rangeQuery("timestamp").from("now-1d").to("now")).endObject())
                .get();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", constantScoreQuery(rangeQuery("timestamp").from("now-1d").to("now"))).endObject())
                .get();
        refresh();

        logger.info("--> Percolate doc with field1=b");
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("my-type")
                .setPercolateDoc(docBuilder().setDoc("timestamp", System.currentTimeMillis()))
                .get();
        assertMatchCount(response, 2L);
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2"));
    }

    void initNestedIndexAndPercolation() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject().startObject("properties").startObject("companyname").field("type", "text").endObject()
                .startObject("employee").field("type", "nested").startObject("properties")
                .startObject("name").field("type", "text").endObject().endObject().endObject().endObject()
                .endObject();

        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping("company", mapping)
                .addMapping(TYPE_NAME, "query", "type=percolator")
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME, TYPE_NAME, "Q").setSource(jsonBuilder().startObject()
                .field("query", QueryBuilders.nestedQuery("employee", QueryBuilders.matchQuery("employee.name", "virginia potts").operator(Operator.AND), ScoreMode.Avg)).endObject()).get();

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

    public void testNestedDocFilter() throws IOException {
        String mapping = "{\n" +
                "    \"doc\": {\n" +
                "      \"properties\": {\n" +
                "        \"name\": {\"type\":\"text\"},\n" +
                "        \"persons\": {\n" +
                "          \"type\": \"nested\"\n," +
                "          \"properties\" : {\"foo\" : {\"type\" : \"text\"}}" +
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
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .addMapping("doc", mapping));
        ensureGreen(INDEX_NAME);
        client().prepareIndex(INDEX_NAME, TYPE_NAME).setSource(q1).setId("q1").get();
        client().prepareIndex(INDEX_NAME, TYPE_NAME).setSource(q2).setId("q2").get();
        client().prepareIndex(INDEX_NAME, TYPE_NAME).setSource(q3).setId("q3").get();
        client().prepareIndex(INDEX_NAME, TYPE_NAME).setSource(q4).setId("q4").get();
        client().prepareIndex(INDEX_NAME, TYPE_NAME).setSource(q5).setId("q5").get();
        client().prepareIndex(INDEX_NAME, TYPE_NAME).setSource(q6).setId("q6").get();
        refresh();
        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("doc")
                .setPercolateDoc(docBuilder().setDoc(doc))
                .get();
        assertMatchCount(response, 3L);
        Set<String> expectedIds = new HashSet<>();
        expectedIds.add("q1");
        expectedIds.add("q4");
        expectedIds.add("q5");
        for (PercolateResponse.Match match : response.getMatches()) {
            assertTrue(expectedIds.remove(match.getId().string()));
        }
        assertTrue(expectedIds.isEmpty());
        response = preparePercolate(client()).setOnlyCount(true)
                .setIndices(INDEX_NAME).setDocumentType("doc")
                .setPercolateDoc(docBuilder().setDoc(doc))
                .get();
        assertMatchCount(response, 3L);
        response = preparePercolate(client()).setScore(randomBoolean()).setSortByScore(randomBoolean()).setOnlyCount(randomBoolean()).setSize(10).setPercolateQuery(QueryBuilders.termQuery("text", "foo"))
                .setIndices(INDEX_NAME).setDocumentType("doc")
                .setPercolateDoc(docBuilder().setDoc(doc))
                .get();
        assertMatchCount(response, 3L);
    }

    public void testMapUnmappedFieldAsString() throws IOException{
        // If index.percolator.map_unmapped_fields_as_string is set to true, unmapped field is mapped as an analyzed string.
        Settings.Builder settings = Settings.builder()
                .put(indexSettings())
                .put("index.percolator.map_unmapped_fields_as_string", true);
        assertAcked(prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .setSettings(settings));
        client().prepareIndex(INDEX_NAME, TYPE_NAME)
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "value")).endObject()).get();
        refresh();
        logger.info("--> Percolate doc with field1=value");
        PercolateResponse response1 = preparePercolate(client())
                .setIndices(INDEX_NAME).setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "value").endObject()))
                .execute().actionGet();
        assertMatchCount(response1, 1L);
        assertThat(response1.getMatches(), arrayWithSize(1));
    }

    public void testGeoShapeWithMapUnmappedFieldAsString() throws Exception {
        // If index.percolator.map_unmapped_fields_as_string is set to true, unmapped field is mapped as an analyzed string.
        Settings.Builder settings = Settings.builder()
            .put(indexSettings())
            .put("index.percolator.map_unmapped_fields_as_string", true);
        assertAcked(prepareCreate(INDEX_NAME)
            .setSettings(settings)
            .addMapping(TYPE_NAME, "query", "type=percolator")
            .addMapping("type", "location", "type=geo_shape"));
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
            .setSource(jsonBuilder().startObject().field("query", geoShapeQuery("location", ShapeBuilders.newEnvelope(new Coordinate(0d, 50d), new Coordinate(2d, 40d)))).endObject())
            .get();
        refresh();

        PercolateResponse response1 = preparePercolate(client())
            .setIndices(INDEX_NAME).setDocumentType("type")
            .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject()
                .startObject("location")
                    .field("type", "point")
                    .field("coordinates", Arrays.asList(1.44207d, 43.59959d))
                .endObject()
                .endObject()))
            .execute().actionGet();
        assertMatchCount(response1, 1L);
        assertThat(response1.getMatches().length, equalTo(1));
        assertThat(response1.getMatches()[0].getId().string(), equalTo("1"));
    }

    public void testParentChild() throws Exception {
        // We don't fail p/c queries, but those queries are unusable because only a single document can be provided in
        // the percolate api

        assertAcked(prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .addMapping("child", "_parent", "type=parent").addMapping("parent"));
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", hasChildQuery("child", matchAllQuery(), ScoreMode.None)).endObject())
                .execute().actionGet();
    }

    public void testPercolateDocumentWithParentField() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .addMapping("child", "_parent", "type=parent").addMapping("parent"));
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        refresh();

        // Just percolating a document that has a _parent field in its mapping should just work:
        PercolateResponse response = preparePercolate(client())
                .setDocumentType("parent")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("field", "value"))
                .get();
        assertMatchCount(response, 1);
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));
    }

    public void testFilterByNow() throws Exception {
        prepareCreate(INDEX_NAME).addMapping(TYPE_NAME, "query", "type=percolator").get();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).field("created", "2015-07-10T14:41:54+0000").endObject())
                .get();
        refresh();

        PercolateResponse response = preparePercolate(client())
                .setIndices(INDEX_NAME)
                .setDocumentType("type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .setPercolateQuery(rangeQuery("created").lte("now"))
                .get();
        assertMatchCount(response, 1);
    }
}

