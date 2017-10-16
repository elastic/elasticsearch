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
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;

public class IndexAdminIT extends ESRestHighLevelClientTestCase {

    public void testDeleteIndex_ifIndexExists() throws IOException {
        // Testing existing index is deleted
        GetRequest getRequest = new GetRequest("test_index", "type", "id");
        highLevelClient().index(new IndexRequest("test_index", "type", "id").source(Collections.singletonMap("foo", "bar")));

        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("test_index");
        DeleteIndexResponse deleteIndexResponse =
            execute(deleteIndexRequest, highLevelClient()::deleteIndex, highLevelClient()::deleteIndexAsync);
        assertTrue(deleteIndexResponse.isAcknowledged());

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testDeleteIndex_ifIndexNotExists() throws IOException {
        // Testing error on non-existing index
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("non_existent_index");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> execute(deleteIndexRequest, highLevelClient()::deleteIndex, highLevelClient()::deleteIndexAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }
}
