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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public class IndicesClientIT extends ESRestHighLevelClientTestCase {

    public void testDeleteIndex() throws IOException {
        {
            // Delete index if exists
            String indexName = "test_index";
            createIndex(indexName);

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            DeleteIndexResponse deleteIndexResponse =
                execute(deleteIndexRequest, highLevelClient().indices()::deleteIndex, highLevelClient().indices()::deleteIndexAsync);
            assertTrue(deleteIndexResponse.isAcknowledged());

            assertFalse(indexExists(indexName));
        }
        {
            // Return 404 if index doesn't exist
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(nonExistentIndex);

            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(deleteIndexRequest, highLevelClient().indices()::deleteIndex, highLevelClient().indices()::deleteIndexAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    public void testOpenExistingIndex() throws IOException {
        String[] indices = randomIndices(1, 5);
        for (String index : indices) {
            createIndex(index);
            closeIndex(index);
            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest("GET", index + "/_search"));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
            assertThat(exception.getMessage().contains(index), equalTo(true));
        }

        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indices);
        OpenIndexResponse openIndexResponse = execute(openIndexRequest, highLevelClient().indices()::openIndex,
                highLevelClient().indices()::openIndexAsync);
        assertTrue(openIndexResponse.isAcknowledged());

        for (String index : indices) {
            client().performRequest("GET", index + "/_search");
        }
    }

    public void testOpenNonExistentIndex() throws IOException {
        String[] nonExistentIndices = randomIndices();
        for (String nonExistentIndex : nonExistentIndices) {
            assertFalse(indexExists(nonExistentIndex));
        }

        OpenIndexRequest openIndexRequest = new OpenIndexRequest(nonExistentIndices);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(openIndexRequest, highLevelClient().indices()::openIndex, highLevelClient().indices()::openIndexAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    private String[] randomIndices() {
        return randomIndices(0, 5);
    }

    private String[] randomIndices(int minIndicesNum, int maxIndicesNum) {
        int numIndices = randomIntBetween(minIndicesNum, maxIndicesNum);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = "index-" + randomAlphaOfLengthBetween(2, 5).toLowerCase(Locale.ROOT);
        }
        return indices;
    }

    private static void createIndex(String index) throws IOException {
        Response response = client().performRequest("PUT", index);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    private static boolean indexExists(String index) throws IOException {
        Response response = client().performRequest("HEAD", index);
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    private static void closeIndex(String index) throws IOException {
        Response response = client().performRequest("POST", index + "/_close");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }
}
