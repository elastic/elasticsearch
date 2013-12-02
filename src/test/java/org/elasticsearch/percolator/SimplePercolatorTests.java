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

package org.elasticsearch.percolator;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 *
 */
public class SimplePercolatorTests extends ElasticsearchIntegrationTest {

    @Test
    public void percolateOnRecreatedIndex() throws Exception {
        prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "test", "1").setSource("field1", "value1").execute().actionGet();

        logger.info("--> register a query");
        client().prepareIndex("_percolator", "test", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        wipeIndices("test");

        prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().prepareIndex("test", "test", "1").setSource("field1", "value1").execute().actionGet();

        logger.info("--> register a query");
        client().prepareIndex("_percolator", "test", "kuku")
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

        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("doc", mapping)
                .setSettings(builder.put("index.number_of_shards", 1))
                .execute().actionGet());
        ensureGreen();
        logger.info("--> register a query");
        client().prepareIndex("_percolator", "test", "1")
                .setSource(jsonBuilder().startObject()
                        .field("source", "productizer")
                        .field("query", QueryBuilders.constantScoreQuery(QueryBuilders.queryString("filingcategory:s")))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
        waitForPercolatorIndexAllocated(1);

        PercolateResponse percolate = client().preparePercolate("test", "doc").setSource(jsonBuilder().startObject()
                .startObject("doc").field("filingcategory", "s").endObject()
                .field("query", termQuery("source", "productizer"))
                .endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
      
    }

    @Test
    public void registerPercolatorAndThenCreateAnIndex() throws Exception {
        logger.info("--> register a query");
        client().prepareIndex("_percolator", "test", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
        waitForPercolatorIndexAllocated(0);

        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        PercolateResponse percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));

        percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject()
                .startObject("doc").field("field1", "value1").endObject()
                .field("query", matchAllQuery())
                .endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
    }

    @Test
    public void createIndexAndThenRegisterPercolator() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        logger.info("--> register a query");
        client().prepareIndex("_percolator", "test", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .execute().actionGet();
        waitForPercolatorIndexAllocated(1);

        for (int i = 0; i < 10; i++) {
            PercolateResponse percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc")
                    .field("field1", "value1")
                    .endObject().endObject())
                    .execute().actionGet();
            assertThat(percolate.getMatches().size(), equalTo(1));
        }

        for (int i = 0; i < 10; i++) {
            PercolateResponse percolate = client().preparePercolate("test", "type1").setPreferLocal(false).setSource(jsonBuilder().startObject().startObject("doc")
                    .field("field1", "value1")
                    .endObject().endObject())
                    .execute().actionGet();
            assertThat(percolate.getMatches().size(), equalTo(1));
        }

        logger.info("--> delete the index");

        wipeIndices("test");

        logger.info("--> make sure percoalted queries for it have been deleted as well");
        assertThat(client().prepareCount("_percolator").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(0l));
    }

    @Test
    public void percolateOnIndexOperation() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 2)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        logger.info("--> register a query");
        client().prepareIndex("_percolator", "test", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
        waitForPercolatorIndexAllocated(1);

        for (int i = 0; i < 10; i++) {
            IndexResponse index = client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "value1")
                    .setPercolate("*").execute().actionGet();
            assertThat(index.getMatches().size(), equalTo(1));
            assertThat(index.getMatches(), hasItem("kuku"));
        }

        for (int i = 0; i < 10; i++) {
            IndexResponse index = client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "value1")
                    .setPercolate("color:blue").execute().actionGet();
            assertThat(index.getMatches().size(), equalTo(1));
            assertThat(index.getMatches(), hasItem("kuku"));
        }

        for (int i = 0; i < 10; i++) {
            IndexResponse index = client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "value1")
                    .setPercolate("color:green").execute().actionGet();
            assertThat(index.getMatches().size(), equalTo(0));
        }

        // test bulk
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < 10; i++) {
            bulkRequestBuilder.add(client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "value1")
                    .setPercolate("*"));
        }
        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            IndexResponse index = bulkItemResponse.getResponse();
            assertThat(index.getMatches().size(), equalTo(1));
            assertThat(index.getMatches(), hasItem("kuku"));
        }
    }

    @Test
    public void multiplePercolators() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();
        logger.info("--> register a query 1");
        client().prepareIndex("_percolator", "test", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
        waitForPercolatorIndexAllocated(1);
        logger.info("--> register a query 2");
        client().prepareIndex("_percolator", "test", "bubu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "green")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();


        PercolateResponse percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
        assertThat(percolate.getMatches(), hasItem("kuku"));

        percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc").startObject("type1")
                .field("field1", "value2")
                .endObject().endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
        assertThat(percolate.getMatches(), hasItem("bubu"));

    }

    @Test
    public void dynamicAddingRemovingQueries() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        logger.info("--> register a query 1");
        client().prepareIndex("_percolator", "test", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
        waitForPercolatorIndexAllocated(1);
        PercolateResponse percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
        assertThat(percolate.getMatches(), hasItem("kuku"));

        logger.info("--> register a query 2");
        client().prepareIndex("_percolator", "test", "bubu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "green")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc").startObject("type1")
                .field("field1", "value2")
                .endObject().endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
        assertThat(percolate.getMatches(), hasItem("bubu"));

        logger.info("--> register a query 3");
        client().prepareIndex("_percolator", "test", "susu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "red")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject()
                .startObject("doc").startObject("type1")
                .field("field1", "value2")
                .endObject().endObject()

                .field("query", termQuery("color", "red"))

                .endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
        assertThat(percolate.getMatches(), hasItem("susu"));

        logger.info("--> deleting query 1");
        client().prepareDelete("_percolator", "test", "kuku").setRefresh(true).execute().actionGet();

        percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc").startObject("type1")
                .field("field1", "value1")
                .endObject().endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(0));
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
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        logger.info("--> register a query");
        client().prepareIndex("_percolator", "test", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
        waitForPercolatorIndexAllocated(2);
        logger.info("--> percolate a document");
        PercolateResponse percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject()
                .startObject("doc").startObject("type1")
                .field("field1", "value1")
                .endObject().endObject()
                .endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
        assertThat(percolate.getMatches(), hasItem("kuku"));
    }

    @Test
    public void testThatPercolatingWithTimeToLiveWorks() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_ttl").field("enabled", true).field("default", "60d").endObject()
                .startObject("_timestamp").field("enabled", true).endObject()
                .endObject().endObject().string();

        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", mapping)
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("_percolator", "test", "kuku").setSource(jsonBuilder()
                .startObject()
                    .startObject("query")
                        .startObject("term")
                            .field("field1", "value1")
                        .endObject()
                    .endObject()
                .endObject()
            ).setRefresh(true).execute().actionGet();
        waitForPercolatorIndexAllocated(2);
        PercolateResponse percolateResponse = client().preparePercolate("test", "type1").setSource(jsonBuilder()
                .startObject()
                    .startObject("doc")
                        .field("field1", "value1")
                    .endObject()
                .endObject()
        ).execute().actionGet();
        assertThat(percolateResponse.getMatches(), hasItem("kuku"));
    }

    private void waitForPercolatorIndexAllocated(int baseShards) {
        ClusterHealthResponse clusterIndexHealths = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(baseShards + cluster().size()).execute().actionGet();
        assertThat(clusterIndexHealths.isTimedOut(), is(false));
        assertThat(clusterIndexHealths.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }
}
