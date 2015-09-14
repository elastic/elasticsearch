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

package org.elasticsearch.action.search;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class SearchRequestBuilderTests extends ESTestCase {

    private static Client client;

    @BeforeClass
    public static void initClient() {
        //this client will not be hit by any request, but it needs to be a non null proper client
        //that is why we create it but we don't add any transport address to it
        Settings settings = Settings.builder()
                .put("path.home", createTempDir().toString())
                .build();
        client = TransportClient.builder().settings(settings).build();
    }

    @AfterClass
    public static void closeClient() {
        client.close();
        client = null;
    }

    @Test
    public void testEmptySourceToString() {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().toString()));
    }

    @Test
    public void testQueryBuilderQueryToString() {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()));
    }

    @Test
    public void testXContentBuilderQueryToString() throws IOException {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        xContentBuilder.startObject();
        xContentBuilder.startObject("match_all");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        searchRequestBuilder.setQuery(xContentBuilder);
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().query(xContentBuilder).toString()));
    }

    @Test
    public void testStringQueryToString() {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        String query = "{ \"match_all\" : {} }";
        searchRequestBuilder.setQuery(query);
        assertThat(searchRequestBuilder.toString(), containsString("\"query\":{ \"match_all\" : {} }"));
    }

    @Test
    public void testStringSourceToString() {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        String source = "{ \"query\" : { \"match_all\" : {} } }";
        searchRequestBuilder.setSource(new BytesArray(source));
        assertThat(searchRequestBuilder.toString(), equalTo(source));
    }

    @Test
    public void testXContentBuilderSourceToString() throws IOException {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        xContentBuilder.startObject();
        xContentBuilder.startObject("query");
        xContentBuilder.startObject("match_all");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        searchRequestBuilder.setSource(xContentBuilder.bytes());
        assertThat(searchRequestBuilder.toString(), equalTo(XContentHelper.convertToJson(xContentBuilder.bytes(), false, true)));
    }

    @Test
    public void testThatToStringDoesntWipeRequestSource() {
        String source = "{\n" +
                "            \"query\" : {\n" +
                "            \"match\" : {\n" +
                "                \"field\" : {\n" +
                "                    \"query\" : \"value\"" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "        }";
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch().setSource(new BytesArray(source));
        String preToString = searchRequestBuilder.request().source().toUtf8();
        assertThat(searchRequestBuilder.toString(), equalTo(source));
        String postToString = searchRequestBuilder.request().source().toUtf8();
        assertThat(preToString, equalTo(postToString));
    }
}
