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

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.count.CountRequest;
import org.elasticsearch.client.count.CountResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Documentation for count API in the high level java client.
 * Code wrapped in {@code tag} and {@code end} tags is included in the docs.
 */
public class CountDocumentationIT extends ESRestHighLevelClientTestCase {

    @SuppressWarnings({"unused", "unchecked"})
    public void testCount() throws Exception {
        indexCountTestData();
        RestHighLevelClient client = highLevelClient();
        {
            // tag::count-request-basic
            CountRequest countRequest = new CountRequest(); // <1>
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); // <2>
            searchSourceBuilder.query(QueryBuilders.matchAllQuery()); // <3>
            countRequest.source(searchSourceBuilder); // <4>
            // end::count-request-basic
        }
        {
            // tag::count-request-indices-types
            CountRequest countRequest = new CountRequest("blog"); // <1>
            countRequest.types("doc"); // <2>
            // end::count-request-indices-types
            // tag::count-request-routing
            countRequest.routing("routing"); // <1>
            // end::count-request-routing
            // tag::count-request-indicesOptions
            countRequest.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::count-request-indicesOptions
            // tag::count-request-preference
            countRequest.preference("_local"); // <1>
            // end::count-request-preference
            assertNotNull(client.count(countRequest, RequestOptions.DEFAULT));
        }
        {
            // tag::count-source-basics
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); // <1>
            sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy")); // <2>
            // end::count-source-basics

            // tag::count-source-setter
            CountRequest countRequest = new CountRequest();
            countRequest.indices("blog", "author");
            countRequest.source(sourceBuilder);
            // end::count-source-setter

            // tag::count-execute
            CountResponse countResponse = client
                .count(countRequest, RequestOptions.DEFAULT);
            // end::count-execute

            // tag::count-execute-listener
            ActionListener<CountResponse> listener =
                new ActionListener<CountResponse>() {

                    @Override
                    public void onResponse(CountResponse countResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::count-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::count-execute-async
            client.countAsync(countRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::count-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));

            // tag::count-response-1
            long count = countResponse.getCount();
            RestStatus status = countResponse.status();
            Boolean terminatedEarly = countResponse.isTerminatedEarly();
            // end::count-response-1

            // tag::count-response-2
            int totalShards = countResponse.getTotalShards();
            int skippedShards = countResponse.getSkippedShards();
            int successfulShards = countResponse.getSuccessfulShards();
            int failedShards = countResponse.getFailedShards();
            for (ShardSearchFailure failure : countResponse.getShardFailures()) {
                // failures should be handled here
            }
            // end::count-response-2
            assertNotNull(countResponse);
            assertEquals(4, countResponse.getCount());
        }
    }

    private static void indexCountTestData() throws IOException {
        CreateIndexRequest authorsRequest = new CreateIndexRequest("author")
            .mapping("doc", "user", "type=keyword,doc_values=false");
        CreateIndexResponse authorsResponse = highLevelClient().indices().create(authorsRequest, RequestOptions.DEFAULT);
        assertTrue(authorsResponse.isAcknowledged());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("blog", "doc", "1")
            .source(XContentType.JSON, "title", "Doubling Down on Open?", "user",
                Collections.singletonList("kimchy"), "innerObject", Collections.singletonMap("key", "value")));
        bulkRequest.add(new IndexRequest("blog", "doc", "2")
            .source(XContentType.JSON, "title", "Swiftype Joins Forces with Elastic", "user",
                Arrays.asList("kimchy", "matt"), "innerObject", Collections.singletonMap("key", "value")));
        bulkRequest.add(new IndexRequest("blog", "doc", "3")
            .source(XContentType.JSON, "title", "On Net Neutrality", "user",
                Arrays.asList("tyler", "kimchy"), "innerObject", Collections.singletonMap("key", "value")));

        bulkRequest.add(new IndexRequest("author", "doc", "1")
            .source(XContentType.JSON, "user", "kimchy"));


        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        BulkResponse bulkResponse = highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        assertSame(RestStatus.OK, bulkResponse.status());
        assertFalse(bulkResponse.hasFailures());
    }
}
