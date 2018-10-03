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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.count.CountRequest;
import org.elasticsearch.client.count.CountResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.greaterThan;

public class CountIT extends ESRestHighLevelClientTestCase {

    @Before
    public void indexDocuments() throws IOException {

        Request doc1 = new Request(HttpPut.METHOD_NAME, "/index/type/1");
        doc1.setJsonEntity("{\"type\":\"type1\", \"num\":10, \"num2\":50}");
        client().performRequest(doc1);

        Request doc2 = new Request(HttpPut.METHOD_NAME, "/index/type/2");
        doc2.setJsonEntity("{\"type\":\"type1\", \"num\":20, \"num2\":40}");
        client().performRequest(doc2);

        Request doc3 = new Request(HttpPut.METHOD_NAME, "/index/type/3");
        doc3.setJsonEntity("{\"type\":\"type1\", \"num\":50, \"num2\":35}");
        client().performRequest(doc3);

        Request doc4 = new Request(HttpPut.METHOD_NAME, "/index/type/4");
        doc4.setJsonEntity("{\"type\":\"type2\", \"num\":100, \"num2\":10}");
        client().performRequest(doc4);

        Request doc5 = new Request(HttpPut.METHOD_NAME, "/index/type/5");
        doc5.setJsonEntity("{\"type\":\"type2\", \"num\":100, \"num2\":10}");
        client().performRequest(doc5);

        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));

        Request doc6 = new Request(HttpPut.METHOD_NAME, "/index1/doc/1");
        doc6.setJsonEntity("{\"field\":\"value1\", \"rating\": 7}");
        client().performRequest(doc6);

        Request doc7 = new Request(HttpPut.METHOD_NAME, "/index1/doc/2");
        doc7.setJsonEntity("{\"field\":\"value2\"}");
        client().performRequest(doc7);

        Request doc2_1 = new Request(HttpPut.METHOD_NAME, "/index2/doc/1");
        doc2_1.setJsonEntity("{\"type\":\"type1\", \"num\":10, \"num2\":50}");
        client().performRequest(doc2_1);

        client().performRequest(new Request(HttpPost.METHOD_NAME, "/index,index1,index2/_refresh"));

    }

    public void testCountOneIndexNoQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index");
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(5, countResponse.getCount());
    }

    public void testCountMultipleIndicesNoQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index", "index1");
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(7, countResponse.getCount());
    }

    public void testCountAllIndicesNoQuery() throws IOException {
        CountRequest countRequest = new CountRequest();
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(8, countResponse.getCount());
    }

    public void testCountOneIndexMatchQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index");
        countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(1, countResponse.getCount());
    }

    public void testCountMultipleIndicesMatchQueryUsingConstructor() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10));
        CountRequest countRequest = new CountRequest(new String[]{"index", "index2"}, sourceBuilder);
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(2, countResponse.getCount());
    }

    public void testCountMultipleIndicesMatchQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index", "index2");
        countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(2, countResponse.getCount());
    }

    public void testCountAllIndicesMatchQuery() throws IOException {
        CountRequest countRequest = new CountRequest();
        countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(2, countResponse.getCount());
    }

    private static void assertCountHeader(CountResponse countResponse) {
        assertEquals(0, countResponse.getSkippedShards());
        assertEquals(0, countResponse.getFailedShards());
        assertThat(countResponse.getTotalShards(), greaterThan(0));
        assertEquals(countResponse.getTotalShards(), countResponse.getSuccessfulShards());
        assertEquals(0, countResponse.getShardFailures().length);
    }

}
