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

package org.elasticsearch.test.integration.percolator;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimplePercolatorTests extends AbstractSharedClusterTest {

    @Override
    protected int numberOfNodes() {
        return 2;
    }

    @Test
    public void testSimple1() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> Add dummy doc");
        client().prepareIndex("test", "type", "1").setSource("field", "value").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Percolate doc with field1=b");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "4"));

        logger.info("--> Percolate doc with field1=c");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(yamlBuilder().startObject().field("field1", "c").endObject()))
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(response.getCount(), equalTo(2l));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate doc with field1=b c");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(smileBuilder().startObject().field("field1", "b c").endObject()))
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(4));
        assertThat(response.getCount(), equalTo(4l));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4"));

        logger.info("--> Percolate doc with field1=d");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "d").endObject()))
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(response.getCount(), equalTo(1l));
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
        client().admin().indices().prepareCreate("index").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .build()
        ).execute().actionGet();
        client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .build()
        ).execute().actionGet();
        ensureGreen();

        // introduce the doc
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", 1)
                .field("field2", "value")
                .endObject().endObject();

        XContentBuilder docWithType = XContentFactory.jsonBuilder().startObject().startObject("doc").startObject("type1")
                .field("field1", 1)
                .field("field2", "value")
                .endObject().endObject().endObject();

        PercolateResponse response = client().preparePercolate().setSource(doc)
                .setIndices("test").setDocumentType("type1")
                .execute().actionGet();
        assertThat(response.getMatches(), emptyArray());

        // add first query...
        client().prepareIndex("test", "_percolator", "test1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field2", "value")).endObject())
                .execute().actionGet();

        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(doc).execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("test1"));

        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(docWithType).execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("test1"));

        // add second query...
        client().prepareIndex("test", "_percolator", "test2")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field1", 1)).endObject())
                .execute().actionGet();

        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(doc)
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("test1", "test2"));


        client().prepareDelete("test", "_percolator", "test2").execute().actionGet();
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(doc).execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("test1"));

        // add a range query (cached)
        // add a query
        client().prepareIndex("test1", "_percolator")
                .setSource(
                        XContentFactory.jsonBuilder().startObject().field("query",
                                constantScoreQuery(FilterBuilders.rangeFilter("field2").from("value").includeLower(true))
                        ).endObject()
                )
                .execute().actionGet();

        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(doc).execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("test1"));
    }

    @Test
    public void testPercolateQueriesWithRouting() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .execute().actionGet();
        ensureGreen();

        logger.info("--> register a queries");
        for (int i = 1; i <= 100; i++) {
            client().prepareIndex("test", "_percolator", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .setRouting(Integer.toString(i % 2))
                    .execute().actionGet();
        }

        logger.info("--> Percolate doc with no routing");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(100));

        logger.info("--> Percolate doc with routing=0");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .setRouting("0")
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(50));

        logger.info("--> Percolate doc with routing=1");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .setRouting("1")
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(50));
    }

    @Test
    public void percolateOnRecreatedIndex() throws Exception {
        prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> register a query");
        client().prepareIndex("my-queries-index", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        wipeIndex("test");
        prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> register a query");
        client().prepareIndex("my-queries-index", "_percolator", "kuku")
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
        Builder builder = ImmutableSettings.builder();
        builder.put("index.analysis.analyzer.lwhitespacecomma.tokenizer", "whitespacecomma");
        builder.putArray("index.analysis.analyzer.lwhitespacecomma.filter", "lowercase");
        builder.put("index.analysis.tokenizer.whitespacecomma.type", "pattern");
        builder.put("index.analysis.tokenizer.whitespacecomma.pattern", "(,|\\s+)");

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .startObject("properties")
                .startObject("filingcategory").field("type", "string").field("analyzer", "lwhitespacecomma").endObject()
                .endObject()
                .endObject().endObject();

        client().admin().indices().prepareCreate("test")
                .addMapping("doc", mapping)
                .setSettings(builder.put("index.number_of_shards", 1))
                .execute().actionGet();
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject()
                        .field("source", "productizer")
                        .field("query", QueryBuilders.constantScoreQuery(QueryBuilders.queryString("filingcategory:s")))
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
        assertThat(percolate.getMatches(), arrayWithSize(1));

    }

    @Test
    public void createIndexAndThenRegisterPercolator() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .execute().actionGet();

        refresh();
        CountResponse countResponse = client().prepareCount()
                .setQuery(matchAllQuery()).setTypes("_percolator")
                .execute().actionGet();
        assertThat(countResponse.getCount(), equalTo(1l));


        for (int i = 0; i < 10; i++) {
            PercolateResponse percolate = client().preparePercolate()
                    .setIndices("test").setDocumentType("type1")
                    .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                    .execute().actionGet();
            assertThat(percolate.getMatches(), arrayWithSize(1));
        }

        for (int i = 0; i < 10; i++) {
            PercolateResponse percolate = client().preparePercolate()
                    .setIndices("test").setDocumentType("type1")
                    .setPreference("_local")
                    .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                    .execute().actionGet();
            assertThat(percolate.getMatches(), arrayWithSize(1));
        }


        logger.info("--> delete the index");
        client().admin().indices().prepareDelete("test").execute().actionGet();
        logger.info("--> make sure percolated queries for it have been deleted as well");
        countResponse = client().prepareCount()
                .setQuery(matchAllQuery()).setTypes("_percolator")
                .execute().actionGet();
        assertThat(countResponse.getCount(), equalTo(0l));
    }

    @Test
    public void multiplePercolators() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        logger.info("--> register a query 1");
        client().prepareIndex("test", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        logger.info("--> register a query 2");
        client().prepareIndex("test", "_percolator", "bubu")
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
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("kuku"));

        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").startObject("type1").field("field1", "value2").endObject().endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("bubu"));

    }

    @Test
    public void dynamicAddingRemovingQueries() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        logger.info("--> register a query 1");
        client().prepareIndex("test", "_percolator", "kuku")
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
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("kuku"));

        logger.info("--> register a query 2");
        client().prepareIndex("test", "_percolator", "bubu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "green")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").startObject("type1").field("field1", "value2").endObject().endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("bubu"));

        logger.info("--> register a query 3");
        client().prepareIndex("test", "_percolator", "susu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "red")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateSourceBuilder sourceBuilder = new PercolateSourceBuilder()
                .setDoc(docBuilder().setDoc(jsonBuilder().startObject().startObject("type1").field("field1", "value2").endObject().endObject()))
                .setQueryBuilder(termQuery("color", "red"));
        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(sourceBuilder)
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("susu"));

        logger.info("--> deleting query 1");
        client().prepareDelete("test", "_percolator", "kuku").setRefresh(true).execute().actionGet();

        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").startObject("type1")
                        .field("field1", "value1")
                        .endObject().endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), emptyArray());
    }

    @Test
    public void percolateWithSizeField() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_size").field("enabled", true).field("stored", "yes").endObject()
                .endObject().endObject().string();

        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", mapping)
                .execute().actionGet();
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        logger.info("--> percolate a document");
        PercolateResponse percolate = client().preparePercolate().setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject()
                        .startObject("doc").startObject("type1")
                        .field("field1", "value1")
                        .endObject().endObject()
                        .endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(percolate.getMatches(), "test"), arrayContaining("kuku"));
    }

    @Test
    public void testPercolateStatistics() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> First percolate request");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field", "val").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("1"));

        IndicesStatsResponse indicesResponse = client().admin().indices().prepareStats("test").execute().actionGet();
        assertThat(indicesResponse.getTotal().getPercolate().getCount(), equalTo(5l)); // We have 5 partitions
        assertThat(indicesResponse.getTotal().getPercolate().getCurrent(), equalTo(0l));

        NodesStatsResponse nodesResponse = client().admin().cluster().prepareNodesStats().execute().actionGet();
        long percolateCount = 0;
        for (NodeStats nodeStats : nodesResponse) {
            percolateCount += nodeStats.getIndices().getPercolate().getCount();
        }
        assertThat(percolateCount, equalTo(5l)); // We have 5 partitions

        logger.info("--> Second percolate request");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field", "val").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContaining("1"));

        indicesResponse = client().admin().indices().prepareStats().setPercolate(true).execute().actionGet();
        assertThat(indicesResponse.getTotal().getPercolate().getCount(), equalTo(10l));
        assertThat(indicesResponse.getTotal().getPercolate().getCurrent(), equalTo(0l));

        percolateCount = 0;
        nodesResponse = client().admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats nodeStats : nodesResponse) {
            percolateCount += nodeStats.getIndices().getPercolate().getCount();
        }
        assertThat(percolateCount, equalTo(10l));

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
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Percolate existing doc with id 1");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("1"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "4"));

        logger.info("--> Percolate existing doc with id 2");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("2"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate existing doc with id 3");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("3"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getMatches(), arrayWithSize(4));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4"));

        logger.info("--> Percolate existing doc with id 4");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("4"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
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
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Percolate existing doc with id 1");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("1").routing("4"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "4"));

        logger.info("--> Percolate existing doc with id 2");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("2").routing("3"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate existing doc with id 3");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("3").routing("2"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getMatches(), arrayWithSize(4));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4"));

        logger.info("--> Percolate existing doc with id 4");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("4").routing("1"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
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
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Percolate existing doc with id 2 and version 1");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type")
                .setGetRequest(Requests.getRequest("test").type("type").id("2").version(1l))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
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
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));
    }

    @Test
    public void testPercolateMultipleIndicesAndAliases() throws Exception {
        client().admin().indices().prepareCreate("test1")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .execute().actionGet();
        client().admin().indices().prepareCreate("test2")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .execute().actionGet();
        ensureGreen();

        logger.info("--> registering queries");
        for (int i = 1; i <= 10; i++) {
            String index = i % 2 == 0 ? "test1" : "test2";
            client().prepareIndex(index, "_percolator", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .execute().actionGet();
        }

        logger.info("--> Percolate doc to index test1");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test1").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(5));

        logger.info("--> Percolate doc to index test2");
        response = client().preparePercolate()
                .setIndices("test2").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(5));

        logger.info("--> Percolate doc to index test1 and test2");
        response = client().preparePercolate()
                .setIndices("test1", "test2").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(10));

        logger.info("--> Percolate doc to index test2 and test3, with ignore missing");
        response = client().preparePercolate()
                .setIndices("test1", "test3").setDocumentType("type")
                .setIgnoreIndices(IgnoreIndices.MISSING)
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
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
        assertThat(response.getMatches(), arrayWithSize(10));
        for (PercolateResponse.Match match : response) {
            assertThat(match.getIndex().string(), anyOf(equalTo("test1"), equalTo("test2")));
        }

        logger.info("--> Percolate doc to my-alias2");
        response = client().preparePercolate()
                .setIndices("my-alias2").setDocumentType("type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(5));
        for (PercolateResponse.Match match : response) {
            assertThat(match.getIndex().string(), equalTo("test2"));
        }

    }

    @Test
    public void testCountPercolation() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> Add dummy doc");
        client().prepareIndex("test", "type", "1").setSource("field", "value").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Count percolate doc with field1=b");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertThat(response.getCount(), equalTo(2l));
        assertThat(response.getMatches(), emptyArray());

        logger.info("--> Count percolate doc with field1=c");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(yamlBuilder().startObject().field("field1", "c").endObject()))
                .execute().actionGet();
        assertThat(response.getCount(), equalTo(2l));
        assertThat(response.getMatches(), emptyArray());

        logger.info("--> Count percolate doc with field1=b c");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(smileBuilder().startObject().field("field1", "b c").endObject()))
                .execute().actionGet();
        assertThat(response.getCount(), equalTo(4l));
        assertThat(response.getMatches(), emptyArray());

        logger.info("--> Count percolate doc with field1=d");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "d").endObject()))
                .execute().actionGet();
        assertThat(response.getCount(), equalTo(1l));
        assertThat(response.getMatches(), emptyArray());

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
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Count percolate existing doc with id 1");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest("test").type("type").id("1"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getCount(), equalTo(2l));
        assertThat(response.getMatches(), emptyArray());

        logger.info("--> Count percolate existing doc with id 2");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest("test").type("type").id("2"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getCount(), equalTo(2l));
        assertThat(response.getMatches(), emptyArray());

        logger.info("--> Count percolate existing doc with id 3");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest("test").type("type").id("3"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getCount(), equalTo(4l));
        assertThat(response.getMatches(), emptyArray());

        logger.info("--> Count percolate existing doc with id 4");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type").setOnlyCount(true)
                .setGetRequest(Requests.getRequest("test").type("type").id("4"))
                .execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(5));
        assertThat(response.getCount(), equalTo(1l));
        assertThat(response.getMatches(), emptyArray());
    }

    @Test
    public void testDeletePercolatorType() throws Exception {
        client().admin().indices().prepareCreate("test1").execute().actionGet();
        client().admin().indices().prepareCreate("test2").execute().actionGet();
        ensureGreen();

        client().prepareIndex("test1", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().prepareIndex("test2", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();

        PercolateResponse response = client().preparePercolate()
                .setIndices("test1", "test2").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertThat(response.getCount(), equalTo(2l));

        client().admin().indices().prepareDeleteMapping("test1").setType("_percolator").execute().actionGet();
        percolatorTypeRemoved("test1");
        response = client().preparePercolate()
                .setIndices("test1", "test2").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(1l));

        client().admin().indices().prepareDeleteMapping("test2").setType("_percolator").execute().actionGet();
        percolatorTypeRemoved("test2");

        // Percolate api should return 0 matches, because all _percolate types have been removed.
        response = client().preparePercolate()
                .setIndices("test1", "test2").setDocumentType("type").setOnlyCount(true)
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject()))
                .execute().actionGet();
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(0l));
    }

    private void percolatorTypeRemoved(String index) {
        my_goto: while (true) {
            for (Client client : clients()) {
                TypesExistsResponse existsResponse =
                        client.admin().indices().prepareTypesExists(index).setTypes("_percolator").execute().actionGet();
                if (existsResponse.isExists()) {
                    continue my_goto;
                }
            }
            break;
        }
    }

    public void testPercolateSizingWithQueryAndFilter() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        int numLevels = randomIntBetween(1, 25);
        long numQueriesPerLevel = randomIntBetween(10, 250);
        long totalQueries = numLevels * numQueriesPerLevel;
        logger.info("--> register " + totalQueries +" queries");
        for (int level = 1; level <= numLevels; level++) {
            for (int query = 1; query <= numQueriesPerLevel; query++) {
                client().prepareIndex("my-index", "_percolator", level + "-" + query)
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
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(totalQueries));
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
        assertNoFailures(response);
        assertThat(response.getCount(), equalTo(totalQueries));
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
            assertNoFailures(response);
            assertThat(response.getCount(), equalTo(numQueriesPerLevel));
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
            assertNoFailures(response);
            assertThat(response.getCount(), equalTo(numQueriesPerLevel));
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
            assertNoFailures(response);
            assertThat(response.getCount(), equalTo(numQueriesPerLevel));
            if (!onlyCount) {
                assertThat(response.getMatches().length, equalTo(size));
            }
        }
    }

    public static String[] convertFromTextArray(PercolateResponse.Match[] matches, String index) {
        if (matches.length == 0) {
            return Strings.EMPTY_ARRAY;
        }
        String[] strings = new String[matches.length];
        for (int i = 0; i < matches.length; i++) {
            assert index.equals(matches[i].getIndex().string());
            strings[i] = matches[i].id().string();
        }
        return strings;
    }

}
