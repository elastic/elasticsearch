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

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.client.Request.enforceSameContentType;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class RequestTests extends ESTestCase {

    public void testPing() {
        Request request = Request.ping();
        assertEquals("/", request.endpoint);
        assertEquals(0, request.params.size());
        assertNull(request.entity);
        assertEquals("HEAD", request.method);
    }

    public void testGet() {
        getAndExistsTest(Request::get, "GET");
    }

    public void testDelete() throws IOException {
        String index = randomAsciiOfLengthBetween(3, 10);
        String type = randomAsciiOfLengthBetween(3, 10);
        String id = randomAsciiOfLengthBetween(3, 10);
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);

        Map<String, String> expectedParams = new HashMap<>();

        setRandomTimeout(deleteRequest, expectedParams);
        setRandomRefreshPolicy(deleteRequest, expectedParams);
        setRandomVersion(deleteRequest, expectedParams);
        setRandomVersionType(deleteRequest, expectedParams);

        if (frequently()) {
            if (randomBoolean()) {
                String routing = randomAsciiOfLengthBetween(3, 10);
                deleteRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
            if (randomBoolean()) {
                String parent = randomAsciiOfLengthBetween(3, 10);
                deleteRequest.parent(parent);
                expectedParams.put("parent", parent);
            }
        }

        Request request = Request.delete(deleteRequest);
        assertEquals("/" + index + "/" + type + "/" + id, request.endpoint);
        assertEquals(expectedParams, request.params);
        assertEquals("DELETE", request.method);
        assertNull(request.entity);
    }

    public void testExists() {
        getAndExistsTest(Request::exists, "HEAD");
    }

    private static void getAndExistsTest(Function<GetRequest, Request> requestConverter, String method) {
        String index = randomAsciiOfLengthBetween(3, 10);
        String type = randomAsciiOfLengthBetween(3, 10);
        String id = randomAsciiOfLengthBetween(3, 10);
        GetRequest getRequest = new GetRequest(index, type, id);

        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            if (randomBoolean()) {
                String preference = randomAsciiOfLengthBetween(3, 10);
                getRequest.preference(preference);
                expectedParams.put("preference", preference);
            }
            if (randomBoolean()) {
                String routing = randomAsciiOfLengthBetween(3, 10);
                getRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
            if (randomBoolean()) {
                boolean realtime = randomBoolean();
                getRequest.realtime(realtime);
                if (realtime == false) {
                    expectedParams.put("realtime", "false");
                }
            }
            if (randomBoolean()) {
                boolean refresh = randomBoolean();
                getRequest.refresh(refresh);
                if (refresh) {
                    expectedParams.put("refresh", "true");
                }
            }
            if (randomBoolean()) {
                long version = randomLong();
                getRequest.version(version);
                if (version != Versions.MATCH_ANY) {
                    expectedParams.put("version", Long.toString(version));
                }
            }
            if (randomBoolean()) {
                VersionType versionType = randomFrom(VersionType.values());
                getRequest.versionType(versionType);
                if (versionType != VersionType.INTERNAL) {
                    expectedParams.put("version_type", versionType.name().toLowerCase(Locale.ROOT));
                }
            }
            if (randomBoolean()) {
                int numStoredFields = randomIntBetween(1, 10);
                String[] storedFields = new String[numStoredFields];
                StringBuilder storedFieldsParam = new StringBuilder();
                for (int i = 0; i < numStoredFields; i++) {
                    String storedField = randomAsciiOfLengthBetween(3, 10);
                    storedFields[i] = storedField;
                    storedFieldsParam.append(storedField);
                    if (i < numStoredFields - 1) {
                        storedFieldsParam.append(",");
                    }
                }
                getRequest.storedFields(storedFields);
                expectedParams.put("stored_fields", storedFieldsParam.toString());
            }
            if (randomBoolean()) {
                randomizeFetchSourceContextParams(getRequest::fetchSourceContext, expectedParams);
            }
        }
        Request request = requestConverter.apply(getRequest);
        assertEquals("/" + index + "/" + type + "/" + id, request.endpoint);
        assertEquals(expectedParams, request.params);
        assertNull(request.entity);
        assertEquals(method, request.method);
    }

    public void testIndex() throws IOException {
        String index = randomAsciiOfLengthBetween(3, 10);
        String type = randomAsciiOfLengthBetween(3, 10);
        IndexRequest indexRequest = new IndexRequest(index, type);

        String id = randomBoolean() ? randomAsciiOfLengthBetween(3, 10) : null;
        indexRequest.id(id);

        Map<String, String> expectedParams = new HashMap<>();

        String method = "POST";
        if (id != null) {
            method = "PUT";
            if (randomBoolean()) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE);
            }
        }

        setRandomTimeout(indexRequest, expectedParams);
        setRandomRefreshPolicy(indexRequest, expectedParams);

        // There is some logic around _create endpoint and version/version type
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            indexRequest.version(randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED));
            expectedParams.put("version", Long.toString(Versions.MATCH_DELETED));
        } else {
            setRandomVersion(indexRequest, expectedParams);
            setRandomVersionType(indexRequest, expectedParams);
        }

        if (frequently()) {
            if (randomBoolean()) {
                String routing = randomAsciiOfLengthBetween(3, 10);
                indexRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
            if (randomBoolean()) {
                String parent = randomAsciiOfLengthBetween(3, 10);
                indexRequest.parent(parent);
                expectedParams.put("parent", parent);
            }
            if (randomBoolean()) {
                String pipeline = randomAsciiOfLengthBetween(3, 10);
                indexRequest.setPipeline(pipeline);
                expectedParams.put("pipeline", pipeline);
            }
        }

        XContentType xContentType = randomFrom(XContentType.values());
        int nbFields = randomIntBetween(0, 10);
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            for (int i = 0; i < nbFields; i++) {
                builder.field("field_" + i, i);
            }
            builder.endObject();
            indexRequest.source(builder);
        }

        Request request = Request.index(indexRequest);
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            assertEquals("/" + index + "/" + type + "/" + id + "/_create", request.endpoint);
        } else if (id != null) {
            assertEquals("/" + index + "/" + type + "/" + id, request.endpoint);
        } else {
            assertEquals("/" + index + "/" + type, request.endpoint);
        }
        assertEquals(expectedParams, request.params);
        assertEquals(method, request.method);

        HttpEntity entity = request.entity;
        assertNotNull(entity);
        assertTrue(entity instanceof ByteArrayEntity);

        try (XContentParser parser = createParser(xContentType.xContent(), entity.getContent())) {
            assertEquals(nbFields, parser.map().size());
        }
    }

    public void testUpdate() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        Map<String, String> expectedParams = new HashMap<>();
        String index = randomAsciiOfLengthBetween(3, 10);
        String type = randomAsciiOfLengthBetween(3, 10);
        String id = randomAsciiOfLengthBetween(3, 10);

        UpdateRequest updateRequest = new UpdateRequest(index, type, id);
        updateRequest.detectNoop(randomBoolean());

        if (randomBoolean()) {
            BytesReference source = RandomObjects.randomSource(random(), xContentType);
            updateRequest.doc(new IndexRequest().source(source, xContentType));

            boolean docAsUpsert = randomBoolean();
            updateRequest.docAsUpsert(docAsUpsert);
            if (docAsUpsert) {
                expectedParams.put("doc_as_upsert", "true");
            }
        } else {
            updateRequest.script(new Script("_value + 1"));
            updateRequest.scriptedUpsert(randomBoolean());
        }
        if (randomBoolean()) {
            BytesReference source = RandomObjects.randomSource(random(), xContentType);
            updateRequest.upsert(new IndexRequest().source(source, xContentType));
        }
        if (randomBoolean()) {
            String routing = randomAsciiOfLengthBetween(3, 10);
            updateRequest.routing(routing);
            expectedParams.put("routing", routing);
        }
        if (randomBoolean()) {
            String parent = randomAsciiOfLengthBetween(3, 10);
            updateRequest.parent(parent);
            expectedParams.put("parent", parent);
        }
        if (randomBoolean()) {
            String timeout = randomTimeValue();
            updateRequest.timeout(timeout);
            expectedParams.put("timeout", timeout);
        } else {
            expectedParams.put("timeout", ReplicationRequest.DEFAULT_TIMEOUT.getStringRep());
        }
        if (randomBoolean()) {
            WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
            updateRequest.setRefreshPolicy(refreshPolicy);
            if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                expectedParams.put("refresh", refreshPolicy.getValue());
            }
        }
        if (randomBoolean()) {
            int waitForActiveShards = randomIntBetween(0, 10);
            updateRequest.waitForActiveShards(waitForActiveShards);
            expectedParams.put("wait_for_active_shards", String.valueOf(waitForActiveShards));
        }
        if (randomBoolean()) {
            long version = randomLong();
            updateRequest.version(version);
            if (version != Versions.MATCH_ANY) {
                expectedParams.put("version", Long.toString(version));
            }
        }
        if (randomBoolean()) {
            VersionType versionType = randomFrom(VersionType.values());
            updateRequest.versionType(versionType);
            if (versionType != VersionType.INTERNAL) {
                expectedParams.put("version_type", versionType.name().toLowerCase(Locale.ROOT));
            }
        }
        if (randomBoolean()) {
            int retryOnConflict = randomIntBetween(0, 5);
            updateRequest.retryOnConflict(retryOnConflict);
            if (retryOnConflict > 0) {
                expectedParams.put("retry_on_conflict", String.valueOf(retryOnConflict));
            }
        }
        if (randomBoolean()) {
            randomizeFetchSourceContextParams(updateRequest::fetchSource, expectedParams);
        }

        Request request = Request.update(updateRequest);
        assertEquals("/" + index + "/" + type + "/" + id + "/_update", request.endpoint);
        assertEquals(expectedParams, request.params);
        assertEquals("POST", request.method);

        HttpEntity entity = request.entity;
        assertNotNull(entity);
        assertTrue(entity instanceof ByteArrayEntity);

        UpdateRequest parsedUpdateRequest = new UpdateRequest();

        XContentType entityContentType = XContentType.fromMediaTypeOrFormat(entity.getContentType().getValue());
        try (XContentParser parser = createParser(entityContentType.xContent(), entity.getContent())) {
            parsedUpdateRequest.fromXContent(parser);
        }

        assertEquals(updateRequest.scriptedUpsert(), parsedUpdateRequest.scriptedUpsert());
        assertEquals(updateRequest.docAsUpsert(), parsedUpdateRequest.docAsUpsert());
        assertEquals(updateRequest.detectNoop(), parsedUpdateRequest.detectNoop());
        assertEquals(updateRequest.fetchSource(), parsedUpdateRequest.fetchSource());
        assertEquals(updateRequest.script(), parsedUpdateRequest.script());
        if (updateRequest.doc() != null) {
            assertToXContentEquivalent(updateRequest.doc().source(), parsedUpdateRequest.doc().source(), xContentType);
        } else {
            assertNull(parsedUpdateRequest.doc());
        }
        if (updateRequest.upsertRequest() != null) {
            assertToXContentEquivalent(updateRequest.upsertRequest().source(), parsedUpdateRequest.upsertRequest().source(), xContentType);
        } else {
            assertNull(parsedUpdateRequest.upsertRequest());
        }
    }

    public void testUpdateWithDifferentContentTypes() throws IOException {
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.doc(new IndexRequest().source(singletonMap("field", "doc"), XContentType.JSON));
            updateRequest.upsert(new IndexRequest().source(singletonMap("field", "upsert"), XContentType.YAML));
            Request.update(updateRequest);
        });
        assertEquals("Update request cannot have different content types for doc [JSON] and upsert [YAML] documents",
                exception.getMessage());
    }

    public void testBulk() throws IOException {
        Map<String, String> expectedParams = new HashMap<>();

        BulkRequest bulkRequest = new BulkRequest();
        if (randomBoolean()) {
            String timeout = randomTimeValue();
            bulkRequest.timeout(timeout);
            expectedParams.put("timeout", timeout);
        } else {
            expectedParams.put("timeout", BulkShardRequest.DEFAULT_TIMEOUT.getStringRep());
        }

        if (randomBoolean()) {
            WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
            bulkRequest.setRefreshPolicy(refreshPolicy);
            if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                expectedParams.put("refresh", refreshPolicy.getValue());
            }
        }

        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);

        int nbItems = randomIntBetween(10, 100);
        for (int i = 0; i < nbItems; i++) {
            String index = randomAsciiOfLength(5);
            String type = randomAsciiOfLength(5);
            String id = randomAsciiOfLength(5);

            BytesReference source = RandomObjects.randomSource(random(), xContentType);
            DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());

            DocWriteRequest<?> docWriteRequest = null;
            if (opType == DocWriteRequest.OpType.INDEX) {
                IndexRequest indexRequest = new IndexRequest(index, type, id).source(source, xContentType);
                docWriteRequest = indexRequest;
                if (randomBoolean()) {
                    indexRequest.setPipeline(randomAsciiOfLength(5));
                }
                if (randomBoolean()) {
                    indexRequest.parent(randomAsciiOfLength(5));
                }
            } else if (opType == DocWriteRequest.OpType.CREATE) {
                IndexRequest createRequest = new IndexRequest(index, type, id).source(source, xContentType).create(true);
                docWriteRequest = createRequest;
                if (randomBoolean()) {
                    createRequest.parent(randomAsciiOfLength(5));
                }
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                final UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(new IndexRequest().source(source, xContentType));
                docWriteRequest = updateRequest;
                if (randomBoolean()) {
                    updateRequest.retryOnConflict(randomIntBetween(1, 5));
                }
                if (randomBoolean()) {
                    randomizeFetchSourceContextParams(updateRequest::fetchSource, new HashMap<>());
                }
                if (randomBoolean()) {
                    updateRequest.parent(randomAsciiOfLength(5));
                }
            } else if (opType == DocWriteRequest.OpType.DELETE) {
                docWriteRequest = new DeleteRequest(index, type, id);
            }

            if (randomBoolean()) {
                docWriteRequest.routing(randomAsciiOfLength(10));
            }
            if (randomBoolean()) {
                docWriteRequest.version(randomNonNegativeLong());
            }
            if (randomBoolean()) {
                docWriteRequest.versionType(randomFrom(VersionType.values()));
            }
            bulkRequest.add(docWriteRequest);
        }

        Request request = Request.bulk(bulkRequest);
        assertEquals("/_bulk", request.endpoint);
        assertEquals(expectedParams, request.params);
        assertEquals("POST", request.method);

        byte[] content = new byte[(int) request.entity.getContentLength()];
        try (InputStream inputStream = request.entity.getContent()) {
            Streams.readFully(inputStream, content);
        }

        BulkRequest parsedBulkRequest = new BulkRequest();
        parsedBulkRequest.add(content, 0, content.length, xContentType);
        assertEquals(bulkRequest.numberOfActions(), parsedBulkRequest.numberOfActions());

        for (int i = 0; i < bulkRequest.numberOfActions(); i++) {
            DocWriteRequest<?> originalRequest = bulkRequest.requests().get(i);
            DocWriteRequest<?> parsedRequest = parsedBulkRequest.requests().get(i);

            assertEquals(originalRequest.opType(), parsedRequest.opType());
            assertEquals(originalRequest.index(), parsedRequest.index());
            assertEquals(originalRequest.type(), parsedRequest.type());
            assertEquals(originalRequest.id(), parsedRequest.id());
            assertEquals(originalRequest.routing(), parsedRequest.routing());
            assertEquals(originalRequest.parent(), parsedRequest.parent());
            assertEquals(originalRequest.version(), parsedRequest.version());
            assertEquals(originalRequest.versionType(), parsedRequest.versionType());

            DocWriteRequest.OpType opType = originalRequest.opType();
            if (opType == DocWriteRequest.OpType.INDEX) {
                IndexRequest indexRequest = (IndexRequest) originalRequest;
                IndexRequest parsedIndexRequest = (IndexRequest) parsedRequest;

                assertEquals(indexRequest.getPipeline(), parsedIndexRequest.getPipeline());
                assertToXContentEquivalent(indexRequest.source(), parsedIndexRequest.source(), xContentType);
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                UpdateRequest updateRequest = (UpdateRequest) originalRequest;
                UpdateRequest parsedUpdateRequest = (UpdateRequest) parsedRequest;

                assertEquals(updateRequest.retryOnConflict(), parsedUpdateRequest.retryOnConflict());
                assertEquals(updateRequest.fetchSource(), parsedUpdateRequest.fetchSource());
                if (updateRequest.doc() != null) {
                    assertToXContentEquivalent(updateRequest.doc().source(), parsedUpdateRequest.doc().source(), xContentType);
                } else {
                    assertNull(parsedUpdateRequest.doc());
                }
            }
        }
    }

    public void testBulkWithDifferentContentTypes() throws IOException {
        {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new DeleteRequest("index", "type", "0"));
            bulkRequest.add(new UpdateRequest("index", "type", "1").script(new Script("test")));
            bulkRequest.add(new DeleteRequest("index", "type", "2"));

            Request request = Request.bulk(bulkRequest);
            assertEquals(XContentType.JSON.mediaType(), request.entity.getContentType().getValue());
        }
        {
            XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new DeleteRequest("index", "type", "0"));
            bulkRequest.add(new IndexRequest("index", "type", "0").source(singletonMap("field", "value"), xContentType));
            bulkRequest.add(new DeleteRequest("index", "type", "2"));

            Request request = Request.bulk(bulkRequest);
            assertEquals(xContentType.mediaType(), request.entity.getContentType().getValue());
        }
        {
            XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
            UpdateRequest updateRequest = new UpdateRequest("index", "type", "0");
            if (randomBoolean()) {
                updateRequest.doc(new IndexRequest().source(singletonMap("field", "value"), xContentType));
            } else {
                updateRequest.upsert(new IndexRequest().source(singletonMap("field", "value"), xContentType));
            }

            Request request = Request.bulk(new BulkRequest().add(updateRequest));
            assertEquals(xContentType.mediaType(), request.entity.getContentType().getValue());
        }
        {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new IndexRequest("index", "type", "0").source(singletonMap("field", "value"), XContentType.SMILE));
            bulkRequest.add(new IndexRequest("index", "type", "1").source(singletonMap("field", "value"), XContentType.JSON));
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> Request.bulk(bulkRequest));
            assertEquals("Mismatching content-type found for request with content-type [JSON], " +
                            "previous requests have content-type [SMILE]", exception.getMessage());
        }
        {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new IndexRequest("index", "type", "0")
                    .source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new IndexRequest("index", "type", "1")
                    .source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new UpdateRequest("index", "type", "2")
                    .doc(new IndexRequest().source(singletonMap("field", "value"), XContentType.JSON))
                    .upsert(new IndexRequest().source(singletonMap("field", "value"), XContentType.SMILE))
            );
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> Request.bulk(bulkRequest));
            assertEquals("Mismatching content-type found for request with content-type [SMILE], " +
                            "previous requests have content-type [JSON]", exception.getMessage());
        }
        {
            XContentType xContentType = randomFrom(XContentType.CBOR, XContentType.YAML);
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new DeleteRequest("index", "type", "0"));
            bulkRequest.add(new IndexRequest("index", "type", "1").source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new DeleteRequest("index", "type", "2"));
            bulkRequest.add(new DeleteRequest("index", "type", "3"));
            bulkRequest.add(new IndexRequest("index", "type", "4").source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new IndexRequest("index", "type", "1").source(singletonMap("field", "value"), xContentType));
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> Request.bulk(bulkRequest));
            assertEquals("Unsupported content-type found for request with content-type [" + xContentType
                    + "], only JSON and SMILE are supported", exception.getMessage());
        }
    }

    public void testParams() {
        final int nbParams = randomIntBetween(0, 10);
        Request.Params params = Request.Params.builder();
        Map<String, String> expectedParams = new HashMap<>();
        for (int i = 0; i < nbParams; i++) {
            String paramName = "p_" + i;
            String paramValue = randomAsciiOfLength(5);
            params.putParam(paramName, paramValue);
            expectedParams.put(paramName, paramValue);
        }

        Map<String, String> requestParams = params.getParams();
        assertEquals(nbParams, requestParams.size());
        assertEquals(expectedParams, requestParams);
    }

    public void testParamsNoDuplicates() {
        Request.Params params = Request.Params.builder();
        params.putParam("test", "1");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> params.putParam("test", "2"));
        assertEquals("Request parameter [test] is already registered", e.getMessage());

        Map<String, String> requestParams = params.getParams();
        assertEquals(1L, requestParams.size());
        assertEquals("1", requestParams.values().iterator().next());
    }

    public void testEndpoint() {
        assertEquals("/", Request.endpoint());
        assertEquals("/", Request.endpoint(Strings.EMPTY_ARRAY));
        assertEquals("/", Request.endpoint(""));
        assertEquals("/a/b", Request.endpoint("a", "b"));
        assertEquals("/a/b/_create", Request.endpoint("a", "b", "_create"));
        assertEquals("/a/b/c/_create", Request.endpoint("a", "b", "c", "_create"));
        assertEquals("/a/_create", Request.endpoint("a", null, null, "_create"));
    }

    public void testEnforceSameContentType() {
        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
        IndexRequest indexRequest = new IndexRequest().source(singletonMap("field", "value"), xContentType);
        assertEquals(xContentType, enforceSameContentType(indexRequest, null));
        assertEquals(xContentType, enforceSameContentType(indexRequest, xContentType));

        XContentType bulkContentType = randomBoolean() ? xContentType : null;

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
                enforceSameContentType(new IndexRequest().source(singletonMap("field", "value"), XContentType.CBOR), bulkContentType));
        assertEquals("Unsupported content-type found for request with content-type [CBOR], only JSON and SMILE are supported",
                exception.getMessage());

        exception = expectThrows(IllegalArgumentException.class, () ->
                enforceSameContentType(new IndexRequest().source(singletonMap("field", "value"), XContentType.YAML), bulkContentType));
        assertEquals("Unsupported content-type found for request with content-type [YAML], only JSON and SMILE are supported",
                exception.getMessage());

        XContentType requestContentType = xContentType == XContentType.JSON ? XContentType.SMILE : XContentType.JSON;

        exception = expectThrows(IllegalArgumentException.class, () ->
                enforceSameContentType(new IndexRequest().source(singletonMap("field", "value"), requestContentType), xContentType));
        assertEquals("Mismatching content-type found for request with content-type [" + requestContentType + "], "
                + "previous requests have content-type [" + xContentType + "]", exception.getMessage());
    }

    /**
     * Randomize the {@link FetchSourceContext} request parameters.
     */
    private static void randomizeFetchSourceContextParams(Consumer<FetchSourceContext> consumer, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                boolean fetchSource = randomBoolean();
                consumer.accept(new FetchSourceContext(fetchSource));
                if (fetchSource == false) {
                    expectedParams.put("_source", "false");
                }
            } else {
                int numIncludes = randomIntBetween(0, 5);
                String[] includes = new String[numIncludes];
                StringBuilder includesParam = new StringBuilder();
                for (int i = 0; i < numIncludes; i++) {
                    String include = randomAsciiOfLengthBetween(3, 10);
                    includes[i] = include;
                    includesParam.append(include);
                    if (i < numIncludes - 1) {
                        includesParam.append(",");
                    }
                }
                if (numIncludes > 0) {
                    expectedParams.put("_source_include", includesParam.toString());
                }
                int numExcludes = randomIntBetween(0, 5);
                String[] excludes = new String[numExcludes];
                StringBuilder excludesParam = new StringBuilder();
                for (int i = 0; i < numExcludes; i++) {
                    String exclude = randomAsciiOfLengthBetween(3, 10);
                    excludes[i] = exclude;
                    excludesParam.append(exclude);
                    if (i < numExcludes - 1) {
                        excludesParam.append(",");
                    }
                }
                if (numExcludes > 0) {
                    expectedParams.put("_source_exclude", excludesParam.toString());
                }
                consumer.accept(new FetchSourceContext(true, includes, excludes));
            }
        }
    }

    private static void setRandomTimeout(ReplicationRequest<?> request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            String timeout = randomTimeValue();
            request.timeout(timeout);
            expectedParams.put("timeout", timeout);
        } else {
            expectedParams.put("timeout", ReplicationRequest.DEFAULT_TIMEOUT.getStringRep());
        }
    }

    private static void setRandomRefreshPolicy(ReplicatedWriteRequest<?> request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
            request.setRefreshPolicy(refreshPolicy);
            if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                expectedParams.put("refresh", refreshPolicy.getValue());
            }
        }
    }

    private static void setRandomVersion(DocWriteRequest<?> request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            long version = randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED, Versions.NOT_FOUND, randomNonNegativeLong());
            request.version(version);
            if (version != Versions.MATCH_ANY) {
                expectedParams.put("version", Long.toString(version));
            }
        }
    }

    private static void setRandomVersionType(DocWriteRequest<?> request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            VersionType versionType = randomFrom(VersionType.values());
            request.versionType(versionType);
            if (versionType != VersionType.INTERNAL) {
                expectedParams.put("version_type", versionType.name().toLowerCase(Locale.ROOT));
            }
        }
    }
}
