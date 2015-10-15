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

package org.elasticsearch.action.count;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class CountRequestBuilderTests extends ESTestCase {

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
        CountRequestBuilder countRequestBuilder = client.prepareCount();
        assertThat(countRequestBuilder.toString(), equalTo(new CountRequest().toString()));
    }

    @Test
    public void testQueryBuilderQueryToString() {
        CountRequestBuilder countRequestBuilder = client.prepareCount();
        countRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        assertThat(countRequestBuilder.toString(), equalTo(new CountRequest().query(QueryBuilders.matchAllQuery()).toString()));
    }

    @Test
    public void testStringQueryToString() {
        CountRequestBuilder countRequestBuilder = client.prepareCount();
        countRequestBuilder.setQuery(new MatchAllQueryBuilder());
        assertThat(countRequestBuilder.toString(), containsString("match_all"));
    }

    @Test
    public void testThatToStringDoesntWipeSource() {
        CountRequestBuilder countRequestBuilder = client.prepareCount().setQuery(QueryBuilders.termQuery("field", "value"));
        String preToString = countRequestBuilder.request().toString();
        assertThat(countRequestBuilder.toString(), equalTo(new CountRequest().query(QueryBuilders.termQuery("field", "value")).toString()));
        String postToString = countRequestBuilder.request().toString();
        assertThat(preToString, equalTo(postToString));
    }
}
