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
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

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

    private static void createIndex(String index) throws IOException {
        Response response = client().performRequest("PUT", index);

        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private static boolean indexExists(String index) throws IOException {
        Response response = client().performRequest("HEAD", index);

        return response.getStatusLine().getStatusCode() == 200;
    }
}
