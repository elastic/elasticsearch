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

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;

public class CrudIT extends ESRestHighLevelClientTestCase {

    public void testExists() throws IOException {
        {
            GetRequest getRequest = new GetRequest("index", "type", "id");
            assertFalse(execute(getRequest, highLevelClient()::exists, highLevelClient()::existsAsync));
        }
        String document = "{\"field1\":\"value1\",\"field2\":\"value2\"}";
        StringEntity stringEntity = new StringEntity(document, ContentType.APPLICATION_JSON);
        Response response = client().performRequest("PUT", "/index/type/id", Collections.singletonMap("refresh", "wait_for"), stringEntity);
        assertEquals(201, response.getStatusLine().getStatusCode());
        {
            GetRequest getRequest = new GetRequest("index", "type", "id");
            assertTrue(execute(getRequest, highLevelClient()::exists, highLevelClient()::existsAsync));
        }
        {
            GetRequest getRequest = new GetRequest("index", "type", "does_not_exist");
            assertFalse(execute(getRequest, highLevelClient()::exists, highLevelClient()::existsAsync));
        }
        {
            GetRequest getRequest = new GetRequest("index", "type", "does_not_exist").version(1);
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                    () -> execute(getRequest, highLevelClient()::exists, highLevelClient()::existsAsync));
            assertEquals(RestStatus.BAD_REQUEST, exception.status());
            assertThat(exception.getMessage(), containsString("/index/type/does_not_exist?version=1: HTTP/1.1 400 Bad Request"));
        }
    }

    public void testGet() throws IOException {
        {
            GetRequest getRequest = new GetRequest("index", "type", "id");
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                    () -> execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
            assertEquals("Elasticsearch exception [type=index_not_found_exception, reason=no such index]", exception.getMessage());
            assertEquals("index", exception.getMetadata("es.index").get(0));
        }

        String document = "{\"field1\":\"value1\",\"field2\":\"value2\"}";
        StringEntity stringEntity = new StringEntity(document, ContentType.APPLICATION_JSON);
        Response response = client().performRequest("PUT", "/index/type/id", Collections.singletonMap("refresh", "wait_for"), stringEntity);
        assertEquals(201, response.getStatusLine().getStatusCode());
        {
            GetRequest getRequest = new GetRequest("index", "type", "id").version(2);
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                    () -> execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync));
            assertEquals(RestStatus.CONFLICT, exception.status());
            assertEquals("Elasticsearch exception [type=version_conflict_engine_exception, " + "reason=[type][id]: " +
                    "version conflict, current version [1] is different than the one provided [2]]", exception.getMessage());
            assertEquals("index", exception.getMetadata("es.index").get(0));
        }
        {
            GetRequest getRequest = new GetRequest("index", "type", "id");
            if (randomBoolean()) {
                getRequest.version(1L);
            }
            GetResponse getResponse = execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync);
            assertEquals("index", getResponse.getIndex());
            assertEquals("type", getResponse.getType());
            assertEquals("id", getResponse.getId());
            assertTrue(getResponse.isExists());
            assertFalse(getResponse.isSourceEmpty());
            assertEquals(1L, getResponse.getVersion());
            assertEquals(document, getResponse.getSourceAsString());
        }
        {
            GetRequest getRequest = new GetRequest("index", "type", "does_not_exist");
            GetResponse getResponse = execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync);
            assertEquals("index", getResponse.getIndex());
            assertEquals("type", getResponse.getType());
            assertEquals("does_not_exist", getResponse.getId());
            assertFalse(getResponse.isExists());
            assertEquals(-1, getResponse.getVersion());
            assertTrue(getResponse.isSourceEmpty());
            assertNull(getResponse.getSourceAsString());
        }
        {
            GetRequest getRequest = new GetRequest("index", "type", "id");
            getRequest.fetchSourceContext(new FetchSourceContext(false, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY));
            GetResponse getResponse = execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync);
            assertEquals("index", getResponse.getIndex());
            assertEquals("type", getResponse.getType());
            assertEquals("id", getResponse.getId());
            assertTrue(getResponse.isExists());
            assertTrue(getResponse.isSourceEmpty());
            assertEquals(1L, getResponse.getVersion());
            assertNull(getResponse.getSourceAsString());
        }
        {
            GetRequest getRequest = new GetRequest("index", "type", "id");
            if (randomBoolean()) {
                getRequest.fetchSourceContext(new FetchSourceContext(true, new String[]{"field1"}, Strings.EMPTY_ARRAY));
            } else {
                getRequest.fetchSourceContext(new FetchSourceContext(true, Strings.EMPTY_ARRAY, new String[]{"field2"}));
            }
            GetResponse getResponse = execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync);
            assertEquals("index", getResponse.getIndex());
            assertEquals("type", getResponse.getType());
            assertEquals("id", getResponse.getId());
            assertTrue(getResponse.isExists());
            assertFalse(getResponse.isSourceEmpty());
            assertEquals(1L, getResponse.getVersion());
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            assertEquals(1, sourceAsMap.size());
            assertEquals("value1", sourceAsMap.get("field1"));
        }
    }

    public void testDelete() throws IOException {
        {
            DeleteRequest deleteRequest = new DeleteRequest("index", "type", "does_not_exist");
            DeleteResponse deleteResponse = execute(deleteRequest, highLevelClient()::delete, highLevelClient()::deleteAsync);
            assertEquals("index", deleteResponse.getIndex());
            assertEquals("type", deleteResponse.getType());
            assertEquals("does_not_exist", deleteResponse.getId());
            assertEquals(DocWriteResponse.Result.NOT_FOUND, deleteResponse.getResult());
            assertEquals(1, deleteResponse.getVersion());
        }
        String document = "{\"field1\":\"value1\",\"field2\":\"value2\"}";
        StringEntity stringEntity = new StringEntity(document, ContentType.APPLICATION_JSON);
        Response response = client().performRequest("PUT", "/index/type/id", Collections.singletonMap("refresh", "wait_for"), stringEntity);
        assertEquals(201, response.getStatusLine().getStatusCode());
        {
            DeleteRequest deleteRequest = new DeleteRequest("index", "type", "id").version(2);
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(deleteRequest, highLevelClient()::delete, highLevelClient()::deleteAsync));
            assertEquals(RestStatus.CONFLICT, exception.status());
            assertEquals("Elasticsearch exception [type=version_conflict_engine_exception, " + "reason=[type][id]: " +
                    "version conflict, current version [1] is different than the one provided [2]]", exception.getMessage());
            assertEquals("index", exception.getMetadata("es.index").get(0));
        }
        {
            DeleteRequest deleteRequest = new DeleteRequest("index", "type", "id");
            if (randomBoolean()) {
                deleteRequest.version(1L);
            }
            DeleteResponse deleteResponse = execute(deleteRequest, highLevelClient()::delete, highLevelClient()::deleteAsync);
            assertEquals("index", deleteResponse.getIndex());
            assertEquals("type", deleteResponse.getType());
            assertEquals("id", deleteResponse.getId());
            assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        }
    }
}
