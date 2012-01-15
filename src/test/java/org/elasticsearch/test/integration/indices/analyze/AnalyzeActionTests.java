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

package org.elasticsearch.test.integration.indices.analyze;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class AnalyzeActionTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void simpleAnalyzerTests() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            AnalyzeResponse analyzeResponse = client.admin().indices().prepareAnalyze("test", "this is a test").execute().actionGet();
            assertThat(analyzeResponse.tokens().size(), equalTo(1));
            AnalyzeResponse.AnalyzeToken token = analyzeResponse.tokens().get(0);
            assertThat(token.term(), equalTo("test"));
            assertThat(token.startOffset(), equalTo(10));
            assertThat(token.endOffset(), equalTo(14));
        }
    }

    @Test
    public void analyzeWithNoIndex() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        AnalyzeResponse analyzeResponse = client.admin().indices().prepareAnalyze("THIS IS A TEST").setAnalyzer("simple").execute().actionGet();
        assertThat(analyzeResponse.tokens().size(), equalTo(4));

        analyzeResponse = client.admin().indices().prepareAnalyze("THIS IS A TEST").setTokenizer("keyword").setTokenFilters("lowercase").execute().actionGet();
        assertThat(analyzeResponse.tokens().size(), equalTo(1));
        assertThat(analyzeResponse.tokens().get(0).term(), equalTo("this is a test"));
    }

    @Test
    public void analyzerWithFieldOrTypeTests() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.admin().indices().preparePutMapping("test")
                .setType("document").setSource(
                "{\n" +
                        "    \"document\":{\n" +
                        "        \"properties\":{\n" +
                        "            \"simple\":{\n" +
                        "                \"type\":\"string\",\n" +
                        "                \"analyzer\": \"simple\"\n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}"
        ).execute().actionGet();

        for (int i = 0; i < 10; i++) {
            final AnalyzeRequestBuilder requestBuilder = client.admin().indices().prepareAnalyze("test", "THIS IS A TEST");
            requestBuilder.setField("document.simple");
            AnalyzeResponse analyzeResponse = requestBuilder.execute().actionGet();
            assertThat(analyzeResponse.tokens().size(), equalTo(4));
            AnalyzeResponse.AnalyzeToken token = analyzeResponse.tokens().get(3);
            assertThat(token.term(), equalTo("test"));
            assertThat(token.startOffset(), equalTo(10));
            assertThat(token.endOffset(), equalTo(14));
        }
    }
}
