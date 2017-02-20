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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;

public class CrudIT extends ESRestHighLevelClientTestCase {

    public void testDelete() throws IOException {
        {
            // Testing non existing document
            String docId = "does_not_exist";
            DeleteRequest deleteRequest = new DeleteRequest("index", "type", docId);
            DeleteResponse deleteResponse = execute(deleteRequest, highLevelClient()::delete, highLevelClient()::deleteAsync);
            assertEquals("index", deleteResponse.getIndex());
            assertEquals("type", deleteResponse.getType());
            assertEquals(docId, deleteResponse.getId());
            assertEquals(DocWriteResponse.Result.NOT_FOUND, deleteResponse.getResult());
        }
        {
            // Testing deletion
            String docId = "id";
            createSampleDocument(new IndexRequest("index", "type", docId).source(Collections.singletonMap("foo", "bar")));
            DeleteRequest deleteRequest = new DeleteRequest("index", "type", docId);
            if (randomBoolean()) {
                deleteRequest.version(1L);
            }
            DeleteResponse deleteResponse = execute(deleteRequest, highLevelClient()::delete, highLevelClient()::deleteAsync);
            assertEquals("index", deleteResponse.getIndex());
            assertEquals("type", deleteResponse.getType());
            assertEquals(docId, deleteResponse.getId());
            assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        }
        {
            // Testing version conflict
            String docId = "version_conflict";
            createSampleDocument(new IndexRequest("index", "type", docId).source(Collections.singletonMap("foo", "bar")));
            DeleteRequest deleteRequest = new DeleteRequest("index", "type", docId).version(2);
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(deleteRequest, highLevelClient()::delete, highLevelClient()::deleteAsync));
            assertEquals(RestStatus.CONFLICT, exception.status());
            assertEquals("Elasticsearch exception [type=version_conflict_engine_exception, " +
                "reason=[type][" + docId + "]: " +
                "version conflict, current version [1] is different than the one provided [2]]", exception.getMessage());
            assertEquals("index", exception.getMetadata("es.index").get(0));
        }
        {
            // Testing version type
            String docId = "version_type";
            createSampleDocument(new IndexRequest("index", "type", docId).source(Collections.singletonMap("foo", "bar"))
                .versionType(VersionType.EXTERNAL).version(12));
            DeleteRequest deleteRequest = new DeleteRequest("index", "type", docId).versionType(VersionType.EXTERNAL).version(13);
            DeleteResponse deleteResponse = execute(deleteRequest, highLevelClient()::delete, highLevelClient()::deleteAsync);
            assertEquals("index", deleteResponse.getIndex());
            assertEquals("type", deleteResponse.getType());
            assertEquals(docId, deleteResponse.getId());
            assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        }
        {
            // Testing version type with a wrong version
            String docId = "wrong_version";
            createSampleDocument(new IndexRequest("index", "type", docId).source(Collections.singletonMap("foo", "bar"))
                .versionType(VersionType.EXTERNAL).version(12));
            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () -> {
                DeleteRequest deleteRequest = new DeleteRequest("index", "type", docId).versionType(VersionType.EXTERNAL).version(10);
                execute(deleteRequest, highLevelClient()::delete, highLevelClient()::deleteAsync);
            });
            assertEquals(RestStatus.CONFLICT, exception.status());
            assertEquals("Elasticsearch exception [type=version_conflict_engine_exception, reason=[type][" +
                docId + "]: version conflict, current version [12] is higher or equal to the one provided [10]]", exception.getMessage());
            assertEquals("index", exception.getMetadata("es.index").get(0));
        }
        {
            // Testing routing
            String docId = "routing";
            createSampleDocument(new IndexRequest("index", "type", docId).source(Collections.singletonMap("foo", "bar")).routing("foo"));
            DeleteRequest deleteRequest = new DeleteRequest("index", "type", docId).routing("foo");
            DeleteResponse deleteResponse = execute(deleteRequest, highLevelClient()::delete, highLevelClient()::deleteAsync);
            assertEquals("index", deleteResponse.getIndex());
            assertEquals("type", deleteResponse.getType());
            assertEquals(docId, deleteResponse.getId());
            assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        }
    }

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

    public void testIndex() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        {
            IndexRequest indexRequest = new IndexRequest("index", "type");
            indexRequest.source(XContentBuilder.builder(xContentType.xContent()).startObject().field("test", "test").endObject());

            IndexResponse indexResponse = execute(indexRequest, highLevelClient()::index, highLevelClient()::indexAsync);
            assertEquals(RestStatus.CREATED, indexResponse.status());
            assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
            assertEquals("index", indexResponse.getIndex());
            assertEquals("type", indexResponse.getType());
            assertTrue(Strings.hasLength(indexResponse.getId()));
            assertEquals(1L, indexResponse.getVersion());
            assertNotNull(indexResponse.getShardId());
            assertEquals(-1, indexResponse.getShardId().getId());
            assertEquals("index", indexResponse.getShardId().getIndexName());
            assertEquals("index", indexResponse.getShardId().getIndex().getName());
            assertEquals("_na_", indexResponse.getShardId().getIndex().getUUID());
            assertNotNull(indexResponse.getShardInfo());
            assertEquals(0, indexResponse.getShardInfo().getFailed());
            assertTrue(indexResponse.getShardInfo().getSuccessful() > 0);
            assertTrue(indexResponse.getShardInfo().getTotal() > 0);
        }
        {
            IndexRequest indexRequest = new IndexRequest("index", "type", "id");
            indexRequest.source(XContentBuilder.builder(xContentType.xContent()).startObject().field("version", 1).endObject());

            IndexResponse indexResponse = execute(indexRequest, highLevelClient()::index, highLevelClient()::indexAsync);
            assertEquals(RestStatus.CREATED, indexResponse.status());
            assertEquals("index", indexResponse.getIndex());
            assertEquals("type", indexResponse.getType());
            assertEquals("id", indexResponse.getId());
            assertEquals(1L, indexResponse.getVersion());

            indexRequest = new IndexRequest("index", "type", "id");
            indexRequest.source(XContentBuilder.builder(xContentType.xContent()).startObject().field("version", 2).endObject());

            indexResponse = execute(indexRequest, highLevelClient()::index, highLevelClient()::indexAsync);
            assertEquals(RestStatus.OK, indexResponse.status());
            assertEquals("index", indexResponse.getIndex());
            assertEquals("type", indexResponse.getType());
            assertEquals("id", indexResponse.getId());
            assertEquals(2L, indexResponse.getVersion());

            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () -> {
                IndexRequest wrongRequest = new IndexRequest("index", "type", "id");
                wrongRequest.source(XContentBuilder.builder(xContentType.xContent()).startObject().field("field", "test").endObject());
                wrongRequest.version(5L);

                execute(wrongRequest, highLevelClient()::index, highLevelClient()::indexAsync);
            });
            assertEquals(RestStatus.CONFLICT, exception.status());
            assertEquals("Elasticsearch exception [type=version_conflict_engine_exception, reason=[type][id]: " +
                         "version conflict, current version [2] is different than the one provided [5]]", exception.getMessage());
            assertEquals("index", exception.getMetadata("es.index").get(0));
        }
        {
            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () -> {
                IndexRequest indexRequest = new IndexRequest("index", "type", "missing_parent");
                indexRequest.source(XContentBuilder.builder(xContentType.xContent()).startObject().field("field", "test").endObject());
                indexRequest.parent("missing");

                execute(indexRequest, highLevelClient()::index, highLevelClient()::indexAsync);
            });

            assertEquals(RestStatus.BAD_REQUEST, exception.status());
            assertEquals("Elasticsearch exception [type=illegal_argument_exception, " +
                         "reason=Can't specify parent if no parent field has been configured]", exception.getMessage());
        }
        {
            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () -> {
                IndexRequest indexRequest = new IndexRequest("index", "type", "missing_pipeline");
                indexRequest.source(XContentBuilder.builder(xContentType.xContent()).startObject().field("field", "test").endObject());
                indexRequest.setPipeline("missing");

                execute(indexRequest, highLevelClient()::index, highLevelClient()::indexAsync);
            });

            assertEquals(RestStatus.BAD_REQUEST, exception.status());
            assertEquals("Elasticsearch exception [type=illegal_argument_exception, " +
                         "reason=pipeline with id [missing] does not exist]", exception.getMessage());
        }
        {
            IndexRequest indexRequest = new IndexRequest("index", "type", "external_version_type");
            indexRequest.source(XContentBuilder.builder(xContentType.xContent()).startObject().field("field", "test").endObject());
            indexRequest.version(12L);
            indexRequest.versionType(VersionType.EXTERNAL);

            IndexResponse indexResponse = execute(indexRequest, highLevelClient()::index, highLevelClient()::indexAsync);
            assertEquals(RestStatus.CREATED, indexResponse.status());
            assertEquals("index", indexResponse.getIndex());
            assertEquals("type", indexResponse.getType());
            assertEquals("external_version_type", indexResponse.getId());
            assertEquals(12L, indexResponse.getVersion());
        }
        {
            final IndexRequest indexRequest = new IndexRequest("index", "type", "with_create_op_type");
            indexRequest.source(XContentBuilder.builder(xContentType.xContent()).startObject().field("field", "test").endObject());
            indexRequest.opType(DocWriteRequest.OpType.CREATE);

            IndexResponse indexResponse = execute(indexRequest, highLevelClient()::index, highLevelClient()::indexAsync);
            assertEquals(RestStatus.CREATED, indexResponse.status());
            assertEquals("index", indexResponse.getIndex());
            assertEquals("type", indexResponse.getType());
            assertEquals("with_create_op_type", indexResponse.getId());

            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () -> {
                execute(indexRequest, highLevelClient()::index, highLevelClient()::indexAsync);
            });

            assertEquals(RestStatus.CONFLICT, exception.status());
            assertEquals("Elasticsearch exception [type=version_conflict_engine_exception, reason=[type][with_create_op_type]: " +
                         "version conflict, document already exists (current version [1])]", exception.getMessage());
        }
    }

    /**
     * Index a document
     * @param request index request
     * @throws IOException if something goes wrong while executing the index request
     */
    private void createSampleDocument(IndexRequest request) throws IOException {
        IndexResponse indexResponse = execute(request, highLevelClient()::index, highLevelClient()::indexAsync);
        assertEquals(RestStatus.CREATED, indexResponse.status());
    }
}
