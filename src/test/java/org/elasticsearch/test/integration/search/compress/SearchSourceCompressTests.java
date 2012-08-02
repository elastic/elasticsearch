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

package org.elasticsearch.test.integration.search.compress;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.compress.lzf.LZFCompressor;
import org.elasticsearch.common.compress.snappy.xerial.XerialSnappy;
import org.elasticsearch.common.compress.snappy.xerial.XerialSnappyCompressor;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SearchSourceCompressTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void testSourceCompressionLZF() throws IOException {
        CompressorFactory.setDefaultCompressor(new LZFCompressor());
        verifySource(true);
        verifySource(false);
        verifySource(null);
    }

    @Test
    public void testSourceCompressionXerialSnappy() throws IOException {
        if (XerialSnappy.available) {
            CompressorFactory.setDefaultCompressor(new XerialSnappyCompressor());
            verifySource(true);
            verifySource(false);
            verifySource(null);
        }
    }

    @Test
    public void testAll() throws IOException {
        testSourceCompressionLZF();
        testSourceCompressionXerialSnappy();
        testSourceCompressionLZF();
        testSourceCompressionXerialSnappy();
    }

    private void verifySource(Boolean compress) throws IOException {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_source").field("compress", compress).endObject()
                .endObject().endObject().string();

        client.admin().indices().preparePutMapping().setType("type1").setSource(mapping).execute().actionGet();

        for (int i = 1; i < 100; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource(buildSource(i)).execute().actionGet();
        }
        client.prepareIndex("test", "type1", Integer.toString(10000)).setSource(buildSource(10000)).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 1; i < 100; i++) {
            GetResponse getResponse = client.prepareGet("test", "type1", Integer.toString(i)).execute().actionGet();
            assertThat(getResponse.source(), equalTo(buildSource(i).bytes().toBytes()));
        }
        GetResponse getResponse = client.prepareGet("test", "type1", Integer.toString(10000)).execute().actionGet();
        assertThat(getResponse.source(), equalTo(buildSource(10000).bytes().toBytes()));

        for (int i = 1; i < 100; i++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(QueryBuilders.idsQuery("type1").ids(Integer.toString(i))).execute().actionGet();
            assertThat(searchResponse.hits().getTotalHits(), equalTo(1l));
            assertThat(searchResponse.hits().getAt(0).source(), equalTo(buildSource(i).bytes().toBytes()));
        }
    }

    private XContentBuilder buildSource(int count) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < count; j++) {
            sb.append("value").append(j).append(' ');
        }
        builder.field("field", sb.toString());
        return builder.endObject();
    }
}
