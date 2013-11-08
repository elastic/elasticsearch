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

package org.elasticsearch.indices.analyze;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class AnalyzeActionTests extends ElasticsearchIntegrationTest {
    
    @Test
    public void simpleAnalyzerTests() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        createIndex("test");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze("test", "this is a test").execute().actionGet();
            assertThat(analyzeResponse.getTokens().size(), equalTo(1));
            AnalyzeResponse.AnalyzeToken token = analyzeResponse.getTokens().get(0);
            assertThat(token.getTerm(), equalTo("test"));
            assertThat(token.getStartOffset(), equalTo(10));
            assertThat(token.getEndOffset(), equalTo(14));
        }
    }
    
    @Test
    public void analyzeNumericField() throws ElasticSearchException, IOException {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        createIndex("test");
        
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().prepareIndex("test", "test", "1")
        .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .field("long", 1l)
                    .field("double", 1.0d)
                .endObject())
        .setRefresh(true).execute().actionGet();

        try {
            client().admin().indices().prepareAnalyze("test", "123").setField("long").execute().actionGet();
        } catch (ElasticSearchIllegalArgumentException ex) {
        }
        try {
            client().admin().indices().prepareAnalyze("test", "123.0").setField("double").execute().actionGet();
        } catch (ElasticSearchIllegalArgumentException ex) {
        }
    }

    @Test
    public void analyzeWithNoIndex() throws Exception {

        AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze("THIS IS A TEST").setAnalyzer("simple").execute().actionGet();
        assertThat(analyzeResponse.getTokens().size(), equalTo(4));

        analyzeResponse = client().admin().indices().prepareAnalyze("THIS IS A TEST").setTokenizer("keyword").setTokenFilters("lowercase").execute().actionGet();
        assertThat(analyzeResponse.getTokens().size(), equalTo(1));
        assertThat(analyzeResponse.getTokens().get(0).getTerm(), equalTo("this is a test"));
    }

    @Test
    public void analyzerWithFieldOrTypeTests() throws Exception {

        createIndex("test");
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().admin().indices().preparePutMapping("test")
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
            final AnalyzeRequestBuilder requestBuilder = client().admin().indices().prepareAnalyze("test", "THIS IS A TEST");
            requestBuilder.setField("document.simple");
            AnalyzeResponse analyzeResponse = requestBuilder.execute().actionGet();
            assertThat(analyzeResponse.getTokens().size(), equalTo(4));
            AnalyzeResponse.AnalyzeToken token = analyzeResponse.getTokens().get(3);
            assertThat(token.getTerm(), equalTo("test"));
            assertThat(token.getStartOffset(), equalTo(10));
            assertThat(token.getEndOffset(), equalTo(14));
        }
    }
}
