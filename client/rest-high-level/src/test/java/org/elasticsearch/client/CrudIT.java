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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonMap;

public class CrudIT extends ESRestHighLevelClientTestCase {

    public void testDelete() throws IOException {
        {
            // Testing deletion
            String docId = "id";
            highLevelClient().index(new IndexRequest("index", "type", docId).source(Collections.singletonMap("foo", "bar")));
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
            // Testing version conflict
            String docId = "version_conflict";
            highLevelClient().index(new IndexRequest("index", "type", docId).source(Collections.singletonMap("foo", "bar")));
            DeleteRequest deleteRequest = new DeleteRequest("index", "type", docId).version(2);
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(deleteRequest, highLevelClient()::delete, highLevelClient()::deleteAsync));
            assertEquals(RestStatus.CONFLICT, exception.status());
            assertEquals("Elasticsearch exception [type=version_conflict_engine_exception, reason=[type][" + docId + "]: " +
                "version conflict, current version [1] is different than the one provided [2]]", exception.getMessage());
            assertEquals("index", exception.getMetadata("es.index").get(0));
        }
        {
            // Testing version type
            String docId = "version_type";
            highLevelClient().index(new IndexRequest("index", "type", docId).source(Collections.singletonMap("foo", "bar"))
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
            highLevelClient().index(new IndexRequest("index", "type", docId).source(Collections.singletonMap("foo", "bar"))
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
            highLevelClient().index(new IndexRequest("index", "type", docId).source(Collections.singletonMap("foo", "bar")).routing("foo"));
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
            assertFalse(execute(getRequest, highLevelClient()::exists, highLevelClient()::existsAsync));
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
                         "reason=can't specify parent if no parent field has been configured]", exception.getMessage());
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

    public void testUpdate() throws IOException {
        {
            UpdateRequest updateRequest = new UpdateRequest("index", "type", "does_not_exist");
            updateRequest.doc(singletonMap("field", "value"), randomFrom(XContentType.values()));

            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () ->
                    execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
            assertEquals("Elasticsearch exception [type=document_missing_exception, reason=[type][does_not_exist]: document missing]",
                    exception.getMessage());
        }
        {
            IndexRequest indexRequest = new IndexRequest("index", "type", "id");
            indexRequest.source(singletonMap("field", "value"));
            IndexResponse indexResponse = highLevelClient().index(indexRequest);
            assertEquals(RestStatus.CREATED, indexResponse.status());

            UpdateRequest updateRequest = new UpdateRequest("index", "type", "id");
            updateRequest.doc(singletonMap("field", "updated"), randomFrom(XContentType.values()));

            UpdateResponse updateResponse = execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync);
            assertEquals(RestStatus.OK, updateResponse.status());
            assertEquals(indexResponse.getVersion() + 1, updateResponse.getVersion());

            UpdateRequest updateRequestConflict = new UpdateRequest("index", "type", "id");
            updateRequestConflict.doc(singletonMap("field", "with_version_conflict"), randomFrom(XContentType.values()));
            updateRequestConflict.version(indexResponse.getVersion());

            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () ->
                    execute(updateRequestConflict, highLevelClient()::update, highLevelClient()::updateAsync));
            assertEquals(RestStatus.CONFLICT, exception.status());
            assertEquals("Elasticsearch exception [type=version_conflict_engine_exception, reason=[type][id]: version conflict, " +
                            "current version [2] is different than the one provided [1]]", exception.getMessage());
        }
        {
            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () -> {
                UpdateRequest updateRequest = new UpdateRequest("index", "type", "id");
                updateRequest.doc(singletonMap("field", "updated"), randomFrom(XContentType.values()));
                if (randomBoolean()) {
                    updateRequest.parent("missing");
                } else {
                    updateRequest.routing("missing");
                }
                execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync);
            });

            assertEquals(RestStatus.NOT_FOUND, exception.status());
            assertEquals("Elasticsearch exception [type=document_missing_exception, reason=[type][id]: document missing]",
                    exception.getMessage());
        }
        {
            IndexRequest indexRequest = new IndexRequest("index", "type", "with_script");
            indexRequest.source(singletonMap("counter", 12));
            IndexResponse indexResponse = highLevelClient().index(indexRequest);
            assertEquals(RestStatus.CREATED, indexResponse.status());

            UpdateRequest updateRequest = new UpdateRequest("index", "type", "with_script");
            Script script = new Script(ScriptType.INLINE, "painless", "ctx._source.counter += params.count", singletonMap("count", 8));
            updateRequest.script(script);
            updateRequest.fetchSource(true);

            UpdateResponse updateResponse = execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync);
            assertEquals(RestStatus.OK, updateResponse.status());
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
            assertEquals(2L, updateResponse.getVersion());
            assertEquals(20, updateResponse.getGetResult().sourceAsMap().get("counter"));

        }
        {
            IndexRequest indexRequest = new IndexRequest("index", "type", "with_doc");
            indexRequest.source("field_1", "one", "field_3", "three");
            indexRequest.version(12L);
            indexRequest.versionType(VersionType.EXTERNAL);
            IndexResponse indexResponse = highLevelClient().index(indexRequest);
            assertEquals(RestStatus.CREATED, indexResponse.status());
            assertEquals(12L, indexResponse.getVersion());

            UpdateRequest updateRequest = new UpdateRequest("index", "type", "with_doc");
            updateRequest.doc(singletonMap("field_2", "two"), randomFrom(XContentType.values()));
            updateRequest.fetchSource("field_*", "field_3");

            UpdateResponse updateResponse = execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync);
            assertEquals(RestStatus.OK, updateResponse.status());
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
            assertEquals(13L, updateResponse.getVersion());
            GetResult getResult = updateResponse.getGetResult();
            assertEquals(13L, updateResponse.getVersion());
            Map<String, Object> sourceAsMap = getResult.sourceAsMap();
            assertEquals("one", sourceAsMap.get("field_1"));
            assertEquals("two", sourceAsMap.get("field_2"));
            assertFalse(sourceAsMap.containsKey("field_3"));
        }
        {
            IndexRequest indexRequest = new IndexRequest("index", "type", "noop");
            indexRequest.source("field", "value");
            IndexResponse indexResponse = highLevelClient().index(indexRequest);
            assertEquals(RestStatus.CREATED, indexResponse.status());
            assertEquals(1L, indexResponse.getVersion());

            UpdateRequest updateRequest = new UpdateRequest("index", "type", "noop");
            updateRequest.doc(singletonMap("field", "value"), randomFrom(XContentType.values()));

            UpdateResponse updateResponse = execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync);
            assertEquals(RestStatus.OK, updateResponse.status());
            assertEquals(DocWriteResponse.Result.NOOP, updateResponse.getResult());
            assertEquals(1L, updateResponse.getVersion());

            updateRequest.detectNoop(false);

            updateResponse = execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync);
            assertEquals(RestStatus.OK, updateResponse.status());
            assertEquals(DocWriteResponse.Result.UPDATED, updateResponse.getResult());
            assertEquals(2L, updateResponse.getVersion());
        }
        {
            UpdateRequest updateRequest = new UpdateRequest("index", "type", "with_upsert");
            updateRequest.upsert(singletonMap("doc_status", "created"));
            updateRequest.doc(singletonMap("doc_status", "updated"));
            updateRequest.fetchSource(true);

            UpdateResponse updateResponse = execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync);
            assertEquals(RestStatus.CREATED, updateResponse.status());
            assertEquals("index", updateResponse.getIndex());
            assertEquals("type", updateResponse.getType());
            assertEquals("with_upsert", updateResponse.getId());
            GetResult getResult = updateResponse.getGetResult();
            assertEquals(1L, updateResponse.getVersion());
            assertEquals("created", getResult.sourceAsMap().get("doc_status"));
        }
        {
            UpdateRequest updateRequest = new UpdateRequest("index", "type", "with_doc_as_upsert");
            updateRequest.doc(singletonMap("field", "initialized"));
            updateRequest.fetchSource(true);
            updateRequest.docAsUpsert(true);

            UpdateResponse updateResponse = execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync);
            assertEquals(RestStatus.CREATED, updateResponse.status());
            assertEquals("index", updateResponse.getIndex());
            assertEquals("type", updateResponse.getType());
            assertEquals("with_doc_as_upsert", updateResponse.getId());
            GetResult getResult = updateResponse.getGetResult();
            assertEquals(1L, updateResponse.getVersion());
            assertEquals("initialized", getResult.sourceAsMap().get("field"));
        }
        {
            UpdateRequest updateRequest = new UpdateRequest("index", "type", "with_scripted_upsert");
            updateRequest.fetchSource(true);
            updateRequest.script(new Script(ScriptType.INLINE, "painless", "ctx._source.level = params.test", singletonMap("test", "C")));
            updateRequest.scriptedUpsert(true);
            updateRequest.upsert(singletonMap("level", "A"));

            UpdateResponse updateResponse = execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync);
            assertEquals(RestStatus.CREATED, updateResponse.status());
            assertEquals("index", updateResponse.getIndex());
            assertEquals("type", updateResponse.getType());
            assertEquals("with_scripted_upsert", updateResponse.getId());

            GetResult getResult = updateResponse.getGetResult();
            assertEquals(1L, updateResponse.getVersion());
            assertEquals("C", getResult.sourceAsMap().get("level"));
        }
        {
            IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
                UpdateRequest updateRequest = new UpdateRequest("index", "type", "id");
                updateRequest.doc(new IndexRequest().source(Collections.singletonMap("field", "doc"), XContentType.JSON));
                updateRequest.upsert(new IndexRequest().source(Collections.singletonMap("field", "upsert"), XContentType.YAML));
                execute(updateRequest, highLevelClient()::update, highLevelClient()::updateAsync);
            });
            assertEquals("Update request cannot have different content types for doc [JSON] and upsert [YAML] documents",
                    exception.getMessage());
        }
    }

    public void testBulk() throws IOException {
        int nbItems = randomIntBetween(10, 100);
        boolean[] errors = new boolean[nbItems];

        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < nbItems; i++) {
            String id = String.valueOf(i);
            boolean erroneous = randomBoolean();
            errors[i] = erroneous;

            DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());
            if (opType == DocWriteRequest.OpType.DELETE) {
                if (erroneous == false) {
                    assertEquals(RestStatus.CREATED,
                            highLevelClient().index(new IndexRequest("index", "test", id).source("field", -1)).status());
                }
                DeleteRequest deleteRequest = new DeleteRequest("index", "test", id);
                bulkRequest.add(deleteRequest);

            } else {
                BytesReference source = XContentBuilder.builder(xContentType.xContent()).startObject().field("id", i).endObject().bytes();
                if (opType == DocWriteRequest.OpType.INDEX) {
                    IndexRequest indexRequest = new IndexRequest("index", "test", id).source(source, xContentType);
                    if (erroneous) {
                        indexRequest.version(12L);
                    }
                    bulkRequest.add(indexRequest);

                } else if (opType == DocWriteRequest.OpType.CREATE) {
                    IndexRequest createRequest = new IndexRequest("index", "test", id).source(source, xContentType).create(true);
                    if (erroneous) {
                        assertEquals(RestStatus.CREATED, highLevelClient().index(createRequest).status());
                    }
                    bulkRequest.add(createRequest);

                } else if (opType == DocWriteRequest.OpType.UPDATE) {
                    UpdateRequest updateRequest = new UpdateRequest("index", "test", id)
                            .doc(new IndexRequest().source(source, xContentType));
                    if (erroneous == false) {
                        assertEquals(RestStatus.CREATED,
                                highLevelClient().index(new IndexRequest("index", "test", id).source("field", -1)).status());
                    }
                    bulkRequest.add(updateRequest);
                }
            }
        }

        BulkResponse bulkResponse = execute(bulkRequest, highLevelClient()::bulk, highLevelClient()::bulkAsync);
        assertEquals(RestStatus.OK, bulkResponse.status());
        assertTrue(bulkResponse.getTook().getMillis() > 0);
        assertEquals(nbItems, bulkResponse.getItems().length);

        validateBulkResponses(nbItems, errors, bulkResponse, bulkRequest);
    }

    public void testBulkProcessorIntegration() throws IOException, InterruptedException {
        int nbItems = randomIntBetween(10, 100);
        boolean[] errors = new boolean[nbItems];

        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);

        AtomicReference<BulkResponse> responseRef = new AtomicReference<>();
        AtomicReference<BulkRequest> requestRef = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                responseRef.set(response);
                requestRef.set(request);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                error.set(failure);
            }
        };

        // Pull the client to a variable to work around https://bugs.eclipse.org/bugs/show_bug.cgi?id=514884
        RestHighLevelClient hlClient = highLevelClient();

        try (BulkProcessor processor = BulkProcessor.builder(hlClient::bulkAsync, listener)
                .setConcurrentRequests(0)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.GB))
                .setBulkActions(nbItems + 1)
                .build()) {
            for (int i = 0; i < nbItems; i++) {
                String id = String.valueOf(i);
                boolean erroneous = randomBoolean();
                errors[i] = erroneous;

                DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());
                if (opType == DocWriteRequest.OpType.DELETE) {
                    if (erroneous == false) {
                        assertEquals(RestStatus.CREATED,
                                highLevelClient().index(new IndexRequest("index", "test", id).source("field", -1)).status());
                    }
                    DeleteRequest deleteRequest = new DeleteRequest("index", "test", id);
                    processor.add(deleteRequest);

                } else {
                    if (opType == DocWriteRequest.OpType.INDEX) {
                        IndexRequest indexRequest = new IndexRequest("index", "test", id).source(xContentType, "id", i);
                        if (erroneous) {
                            indexRequest.version(12L);
                        }
                        processor.add(indexRequest);

                    } else if (opType == DocWriteRequest.OpType.CREATE) {
                        IndexRequest createRequest = new IndexRequest("index", "test", id).source(xContentType, "id", i).create(true);
                        if (erroneous) {
                            assertEquals(RestStatus.CREATED, highLevelClient().index(createRequest).status());
                        }
                        processor.add(createRequest);

                    } else if (opType == DocWriteRequest.OpType.UPDATE) {
                        UpdateRequest updateRequest = new UpdateRequest("index", "test", id)
                                .doc(new IndexRequest().source(xContentType, "id", i));
                        if (erroneous == false) {
                            assertEquals(RestStatus.CREATED,
                                    highLevelClient().index(new IndexRequest("index", "test", id).source("field", -1)).status());
                        }
                        processor.add(updateRequest);
                    }
                }
            }
            assertNull(responseRef.get());
            assertNull(requestRef.get());
        }


        BulkResponse bulkResponse = responseRef.get();
        BulkRequest bulkRequest = requestRef.get();

        assertEquals(RestStatus.OK, bulkResponse.status());
        assertTrue(bulkResponse.getTook().getMillis() > 0);
        assertEquals(nbItems, bulkResponse.getItems().length);
        assertNull(error.get());

        validateBulkResponses(nbItems, errors, bulkResponse, bulkRequest);
    }

    private void validateBulkResponses(int nbItems, boolean[] errors, BulkResponse bulkResponse, BulkRequest bulkRequest) {
        for (int i = 0; i < nbItems; i++) {
            BulkItemResponse bulkItemResponse = bulkResponse.getItems()[i];

            assertEquals(i, bulkItemResponse.getItemId());
            assertEquals("index", bulkItemResponse.getIndex());
            assertEquals("test", bulkItemResponse.getType());
            assertEquals(String.valueOf(i), bulkItemResponse.getId());

            DocWriteRequest.OpType requestOpType = bulkRequest.requests().get(i).opType();
            if (requestOpType == DocWriteRequest.OpType.INDEX || requestOpType == DocWriteRequest.OpType.CREATE) {
                assertEquals(errors[i], bulkItemResponse.isFailed());
                assertEquals(errors[i] ? RestStatus.CONFLICT : RestStatus.CREATED, bulkItemResponse.status());
            } else if (requestOpType == DocWriteRequest.OpType.UPDATE) {
                assertEquals(errors[i], bulkItemResponse.isFailed());
                assertEquals(errors[i] ? RestStatus.NOT_FOUND : RestStatus.OK, bulkItemResponse.status());
            } else if (requestOpType == DocWriteRequest.OpType.DELETE) {
                assertFalse(bulkItemResponse.isFailed());
                assertEquals(errors[i] ? RestStatus.NOT_FOUND : RestStatus.OK, bulkItemResponse.status());
            }
        }
    }
}
