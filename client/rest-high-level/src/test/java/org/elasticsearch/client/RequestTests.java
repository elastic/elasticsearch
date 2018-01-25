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
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.client.Request.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.Request.enforceSameContentType;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RequestTests extends ESTestCase {

    public void testConstructor() {
        final String method = randomFrom("GET", "PUT", "POST", "HEAD", "DELETE");
        final String endpoint = randomAlphaOfLengthBetween(1, 10);
        final Map<String, String> parameters = singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
        final HttpEntity entity = randomBoolean() ? new StringEntity(randomAlphaOfLengthBetween(1, 100), ContentType.TEXT_PLAIN) : null;

        NullPointerException e = expectThrows(NullPointerException.class, () -> new Request(null, endpoint, parameters, entity));
        assertEquals("method cannot be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> new Request(method, null, parameters, entity));
        assertEquals("endpoint cannot be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> new Request(method, endpoint, null, entity));
        assertEquals("parameters cannot be null", e.getMessage());

        final Request request = new Request(method, endpoint, parameters, entity);
        assertEquals(method, request.getMethod());
        assertEquals(endpoint, request.getEndpoint());
        assertEquals(parameters, request.getParameters());
        assertEquals(entity, request.getEntity());

        final Constructor<?>[] constructors = Request.class.getConstructors();
        assertEquals("Expected only 1 constructor", 1, constructors.length);
        assertTrue("Request constructor is not public", Modifier.isPublic(constructors[0].getModifiers()));
    }

    public void testClassVisibility() {
        assertTrue("Request class is not public", Modifier.isPublic(Request.class.getModifiers()));
    }

    public void testPing() {
        Request request = Request.ping();
        assertEquals("/", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertNull(request.getEntity());
        assertEquals(HttpHead.METHOD_NAME, request.getMethod());
    }

    public void testInfo() {
        Request request = Request.info();
        assertEquals("/", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertNull(request.getEntity());
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
    }

    public void testGet() {
        getAndExistsTest(Request::get, HttpGet.METHOD_NAME);
    }

    public void testMultiGet() throws IOException {
        Map<String, String> expectedParams = new HashMap<>();
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        if (randomBoolean()) {
            String preference = randomAlphaOfLength(4);
            multiGetRequest.preference(preference);
            expectedParams.put("preference", preference);
        }
        if (randomBoolean()) {
            multiGetRequest.realtime(randomBoolean());
            if (multiGetRequest.realtime() == false) {
                expectedParams.put("realtime", "false");
            }
        }
        if (randomBoolean()) {
            multiGetRequest.refresh(randomBoolean());
            if (multiGetRequest.refresh()) {
                expectedParams.put("refresh", "true");
            }
        }

        int numberOfRequests = randomIntBetween(0, 32);
        for (int i = 0; i < numberOfRequests; i++) {
            MultiGetRequest.Item item =
                    new MultiGetRequest.Item(randomAlphaOfLength(4), randomAlphaOfLength(4), randomAlphaOfLength(4));
            if (randomBoolean()) {
                item.routing(randomAlphaOfLength(4));
            }
            if (randomBoolean()) {
                item.parent(randomAlphaOfLength(4));
            }
            if (randomBoolean()) {
                item.storedFields(generateRandomStringArray(16, 8, false));
            }
            if (randomBoolean()) {
                item.version(randomNonNegativeLong());
            }
            if (randomBoolean()) {
                item.versionType(randomFrom(VersionType.values()));
            }
            if (randomBoolean()) {
                randomizeFetchSourceContextParams(item::fetchSourceContext, new HashMap<>());
            }
            multiGetRequest.add(item);
        }

        Request request = Request.multiGet(multiGetRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_mget", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(multiGetRequest, request.getEntity());
    }

    public void testDelete() {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);

        Map<String, String> expectedParams = new HashMap<>();

        setRandomTimeout(deleteRequest::timeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomRefreshPolicy(deleteRequest::setRefreshPolicy, expectedParams);
        setRandomVersion(deleteRequest, expectedParams);
        setRandomVersionType(deleteRequest::versionType, expectedParams);

        if (frequently()) {
            if (randomBoolean()) {
                String routing = randomAlphaOfLengthBetween(3, 10);
                deleteRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
            if (randomBoolean()) {
                String parent = randomAlphaOfLengthBetween(3, 10);
                deleteRequest.parent(parent);
                expectedParams.put("parent", parent);
            }
        }

        Request request = Request.delete(deleteRequest);
        assertEquals("/" + index + "/" + type + "/" + id, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals("DELETE", request.getMethod());
        assertNull(request.getEntity());
    }

    public void testExists() {
        getAndExistsTest(Request::exists, HttpHead.METHOD_NAME);
    }

    private static void getAndExistsTest(Function<GetRequest, Request> requestConverter, String method) {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        GetRequest getRequest = new GetRequest(index, type, id);

        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            if (randomBoolean()) {
                String preference = randomAlphaOfLengthBetween(3, 10);
                getRequest.preference(preference);
                expectedParams.put("preference", preference);
            }
            if (randomBoolean()) {
                String routing = randomAlphaOfLengthBetween(3, 10);
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
            setRandomVersionType(getRequest::versionType, expectedParams);
            if (randomBoolean()) {
                int numStoredFields = randomIntBetween(1, 10);
                String[] storedFields = new String[numStoredFields];
                String storedFieldsParam = randomFields(storedFields);
                getRequest.storedFields(storedFields);
                expectedParams.put("stored_fields", storedFieldsParam);
            }
            if (randomBoolean()) {
                randomizeFetchSourceContextParams(getRequest::fetchSourceContext, expectedParams);
            }
        }
        Request request = requestConverter.apply(getRequest);
        assertEquals("/" + index + "/" + type + "/" + id, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
        assertEquals(method, request.getMethod());
    }

    public void testCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest();

        String indexName = "index-" + randomAlphaOfLengthBetween(2, 5);

        createIndexRequest.index(indexName);

        Map<String, String> expectedParams = new HashMap<>();

        setRandomTimeout(createIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(createIndexRequest, expectedParams);
        setRandomWaitForActiveShards(createIndexRequest::waitForActiveShards, ActiveShardCount.DEFAULT, expectedParams);

        if (randomBoolean()) {
            boolean updateAllTypes = randomBoolean();
            createIndexRequest.updateAllTypes(updateAllTypes);
            if (updateAllTypes) {
                expectedParams.put("update_all_types", Boolean.TRUE.toString());
            }
        }

        Request request = Request.createIndex(createIndexRequest);
        assertEquals("/" + indexName, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertToXContentBody(createIndexRequest, request.getEntity());
    }

    public void testDeleteIndex() {
        String[] indices = randomIndicesNames(0, 5);
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(deleteIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(deleteIndexRequest, expectedParams);

        setRandomIndicesOptions(deleteIndexRequest::indicesOptions, deleteIndexRequest::indicesOptions, expectedParams);

        Request request = Request.deleteIndex(deleteIndexRequest);
        assertEquals("/" + String.join(",", indices), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertNull(request.getEntity());
    }

    public void testOpenIndex() {
        String[] indices = randomIndicesNames(1, 5);
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indices);
        openIndexRequest.indices(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(openIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(openIndexRequest, expectedParams);
        setRandomIndicesOptions(openIndexRequest::indicesOptions, openIndexRequest::indicesOptions, expectedParams);
        setRandomWaitForActiveShards(openIndexRequest::waitForActiveShards, ActiveShardCount.NONE, expectedParams);

        Request request = Request.openIndex(openIndexRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "").add(String.join(",", indices)).add("_open");
        assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        assertThat(expectedParams, equalTo(request.getParameters()));
        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(request.getEntity(), nullValue());
    }

    public void testCloseIndex() {
        String[] indices = randomIndicesNames(1, 5);
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(closeIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(closeIndexRequest, expectedParams);
        setRandomIndicesOptions(closeIndexRequest::indicesOptions, closeIndexRequest::indicesOptions, expectedParams);

        Request request = Request.closeIndex(closeIndexRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "").add(String.join(",", indices)).add("_close");
        assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        assertThat(expectedParams, equalTo(request.getParameters()));
        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(request.getEntity(), nullValue());
    }

    public void testIndex() throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        IndexRequest indexRequest = new IndexRequest(index, type);

        String id = randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null;
        indexRequest.id(id);

        Map<String, String> expectedParams = new HashMap<>();

        String method = HttpPost.METHOD_NAME;
        if (id != null) {
            method = HttpPut.METHOD_NAME;
            if (randomBoolean()) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE);
            }
        }

        setRandomTimeout(indexRequest::timeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomRefreshPolicy(indexRequest::setRefreshPolicy, expectedParams);

        // There is some logic around _create endpoint and version/version type
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            indexRequest.version(randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED));
            expectedParams.put("version", Long.toString(Versions.MATCH_DELETED));
        } else {
            setRandomVersion(indexRequest, expectedParams);
            setRandomVersionType(indexRequest::versionType, expectedParams);
        }

        if (frequently()) {
            if (randomBoolean()) {
                String routing = randomAlphaOfLengthBetween(3, 10);
                indexRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
            if (randomBoolean()) {
                String parent = randomAlphaOfLengthBetween(3, 10);
                indexRequest.parent(parent);
                expectedParams.put("parent", parent);
            }
            if (randomBoolean()) {
                String pipeline = randomAlphaOfLengthBetween(3, 10);
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
            assertEquals("/" + index + "/" + type + "/" + id + "/_create", request.getEndpoint());
        } else if (id != null) {
            assertEquals("/" + index + "/" + type + "/" + id, request.getEndpoint());
        } else {
            assertEquals("/" + index + "/" + type, request.getEndpoint());
        }
        assertEquals(expectedParams, request.getParameters());
        assertEquals(method, request.getMethod());

        HttpEntity entity = request.getEntity();
        assertTrue(entity instanceof ByteArrayEntity);
        assertEquals(indexRequest.getContentType().mediaTypeWithoutParameters(), entity.getContentType().getValue());
        try (XContentParser parser = createParser(xContentType.xContent(), entity.getContent())) {
            assertEquals(nbFields, parser.map().size());
        }
    }

    public void testUpdate() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        Map<String, String> expectedParams = new HashMap<>();
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);

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
            updateRequest.script(mockScript("_value + 1"));
            updateRequest.scriptedUpsert(randomBoolean());
        }
        if (randomBoolean()) {
            BytesReference source = RandomObjects.randomSource(random(), xContentType);
            updateRequest.upsert(new IndexRequest().source(source, xContentType));
        }
        if (randomBoolean()) {
            String routing = randomAlphaOfLengthBetween(3, 10);
            updateRequest.routing(routing);
            expectedParams.put("routing", routing);
        }
        if (randomBoolean()) {
            String parent = randomAlphaOfLengthBetween(3, 10);
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
        setRandomWaitForActiveShards(updateRequest::waitForActiveShards, ActiveShardCount.DEFAULT, expectedParams);
        setRandomVersion(updateRequest, expectedParams);
        setRandomVersionType(updateRequest::versionType, expectedParams);
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
        assertEquals("/" + index + "/" + type + "/" + id + "/_update", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());

        HttpEntity entity = request.getEntity();
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

    public void testUpdateWithDifferentContentTypes() {
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

        setRandomRefreshPolicy(bulkRequest::setRefreshPolicy, expectedParams);

        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);

        int nbItems = randomIntBetween(10, 100);
        for (int i = 0; i < nbItems; i++) {
            String index = randomAlphaOfLength(5);
            String type = randomAlphaOfLength(5);
            String id = randomAlphaOfLength(5);

            BytesReference source = RandomObjects.randomSource(random(), xContentType);
            DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());

            DocWriteRequest<?> docWriteRequest;
            if (opType == DocWriteRequest.OpType.INDEX) {
                IndexRequest indexRequest = new IndexRequest(index, type, id).source(source, xContentType);
                docWriteRequest = indexRequest;
                if (randomBoolean()) {
                    indexRequest.setPipeline(randomAlphaOfLength(5));
                }
                if (randomBoolean()) {
                    indexRequest.parent(randomAlphaOfLength(5));
                }
            } else if (opType == DocWriteRequest.OpType.CREATE) {
                IndexRequest createRequest = new IndexRequest(index, type, id).source(source, xContentType).create(true);
                docWriteRequest = createRequest;
                if (randomBoolean()) {
                    createRequest.parent(randomAlphaOfLength(5));
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
                    updateRequest.parent(randomAlphaOfLength(5));
                }
            } else if (opType == DocWriteRequest.OpType.DELETE) {
                docWriteRequest = new DeleteRequest(index, type, id);
            } else {
                throw new UnsupportedOperationException("optype [" + opType + "] not supported");
            }

            if (randomBoolean()) {
                docWriteRequest.routing(randomAlphaOfLength(10));
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
        assertEquals("/_bulk", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(xContentType.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        byte[] content = new byte[(int) request.getEntity().getContentLength()];
        try (InputStream inputStream = request.getEntity().getContent()) {
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
            bulkRequest.add(new UpdateRequest("index", "type", "1").script(mockScript("test")));
            bulkRequest.add(new DeleteRequest("index", "type", "2"));

            Request request = Request.bulk(bulkRequest);
            assertEquals(XContentType.JSON.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        }
        {
            XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new DeleteRequest("index", "type", "0"));
            bulkRequest.add(new IndexRequest("index", "type", "0").source(singletonMap("field", "value"), xContentType));
            bulkRequest.add(new DeleteRequest("index", "type", "2"));

            Request request = Request.bulk(bulkRequest);
            assertEquals(xContentType.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
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
            assertEquals(xContentType.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
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

    public void testSearch() throws Exception {
        String[] indices = randomIndicesNames(0, 5);
        SearchRequest searchRequest = new SearchRequest(indices);

        int numTypes = randomIntBetween(0, 5);
        String[] types = new String[numTypes];
        for (int i = 0; i < numTypes; i++) {
            types[i] = "type-" + randomAlphaOfLengthBetween(2, 5);
        }
        searchRequest.types(types);

        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put(RestSearchAction.TYPED_KEYS_PARAM, "true");
        if (randomBoolean()) {
            searchRequest.routing(randomAlphaOfLengthBetween(3, 10));
            expectedParams.put("routing", searchRequest.routing());
        }
        if (randomBoolean()) {
            searchRequest.preference(randomAlphaOfLengthBetween(3, 10));
            expectedParams.put("preference", searchRequest.preference());
        }
        if (randomBoolean()) {
            searchRequest.searchType(randomFrom(SearchType.values()));
        }
        expectedParams.put("search_type", searchRequest.searchType().name().toLowerCase(Locale.ROOT));
        if (randomBoolean()) {
            searchRequest.requestCache(randomBoolean());
            expectedParams.put("request_cache", Boolean.toString(searchRequest.requestCache()));
        }
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(2, Integer.MAX_VALUE));
        }
        expectedParams.put("batched_reduce_size", Integer.toString(searchRequest.getBatchedReduceSize()));
        if (randomBoolean()) {
            searchRequest.scroll(randomTimeValue());
            expectedParams.put("scroll", searchRequest.scroll().keepAlive().getStringRep());
        }

        setRandomIndicesOptions(searchRequest::indicesOptions, searchRequest::indicesOptions, expectedParams);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //rarely skip setting the search source completely
        if (frequently()) {
            //frequently set the search source to have some content, otherwise leave it empty but still set it
            if (frequently()) {
                if (randomBoolean()) {
                    searchSourceBuilder.size(randomIntBetween(0, Integer.MAX_VALUE));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.from(randomIntBetween(0, Integer.MAX_VALUE));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.minScore(randomFloat());
                }
                if (randomBoolean()) {
                    searchSourceBuilder.explain(randomBoolean());
                }
                if (randomBoolean()) {
                    searchSourceBuilder.profile(randomBoolean());
                }
                if (randomBoolean()) {
                    searchSourceBuilder.highlighter(new HighlightBuilder().field(randomAlphaOfLengthBetween(3, 10)));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.query(new TermQueryBuilder(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10)));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.aggregation(new TermsAggregationBuilder(randomAlphaOfLengthBetween(3, 10), ValueType.STRING)
                            .field(randomAlphaOfLengthBetween(3, 10)));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.suggest(new SuggestBuilder().addSuggestion(randomAlphaOfLengthBetween(3, 10),
                            new CompletionSuggestionBuilder(randomAlphaOfLengthBetween(3, 10))));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.addRescorer(new QueryRescorerBuilder(
                            new TermQueryBuilder(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10))));
                }
                if (randomBoolean()) {
                    searchSourceBuilder.collapse(new CollapseBuilder(randomAlphaOfLengthBetween(3, 10)));
                }
            }
            searchRequest.source(searchSourceBuilder);
        }

        Request request = Request.search(searchRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        String type = String.join(",", types);
        if (Strings.hasLength(type)) {
            endpoint.add(type);
        }
        endpoint.add("_search");
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(searchSourceBuilder, request.getEntity());
    }

    public void testMultiSearch() throws IOException {
        int numberOfSearchRequests = randomIntBetween(0, 32);
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (int i = 0; i < numberOfSearchRequests; i++) {
            SearchRequest searchRequest = randomSearchRequest(() -> {
                // No need to return a very complex SearchSourceBuilder here, that is tested elsewhere
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.from(randomInt(10));
                searchSourceBuilder.size(randomIntBetween(20, 100));
                return searchSourceBuilder;
            });
            // scroll is not supported in the current msearch api, so unset it:
            searchRequest.scroll((Scroll) null);
            // only expand_wildcards, ignore_unavailable and allow_no_indices can be specified from msearch api, so unset other options:
            IndicesOptions randomlyGenerated = searchRequest.indicesOptions();
            IndicesOptions msearchDefault = new MultiSearchRequest().indicesOptions();
            searchRequest.indicesOptions(IndicesOptions.fromOptions(
                    randomlyGenerated.ignoreUnavailable(), randomlyGenerated.allowNoIndices(), randomlyGenerated.expandWildcardsOpen(),
                    randomlyGenerated.expandWildcardsClosed(), msearchDefault.allowAliasesToMultipleIndices(),
                    msearchDefault.forbidClosedIndices(), msearchDefault.ignoreAliases()
            ));
            multiSearchRequest.add(searchRequest);
        }

        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put(RestSearchAction.TYPED_KEYS_PARAM, "true");
        if (randomBoolean()) {
            multiSearchRequest.maxConcurrentSearchRequests(randomIntBetween(1, 8));
            expectedParams.put("max_concurrent_searches", Integer.toString(multiSearchRequest.maxConcurrentSearchRequests()));
        }

        Request request = Request.multiSearch(multiSearchRequest);
        assertEquals("/_msearch", request.getEndpoint());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(expectedParams, request.getParameters());

        List<SearchRequest> requests = new ArrayList<>();
        CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer = (searchRequest, p) -> {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(p);
            if (searchSourceBuilder.equals(new SearchSourceBuilder()) == false) {
                searchRequest.source(searchSourceBuilder);
            }
            requests.add(searchRequest);
        };
        MultiSearchRequest.readMultiLineFormat(new BytesArray(EntityUtils.toByteArray(request.getEntity())),
                REQUEST_BODY_CONTENT_TYPE.xContent(), consumer, null, multiSearchRequest.indicesOptions(), null, null,
                null, xContentRegistry(), true);
        assertEquals(requests, multiSearchRequest.requests());
    }

    public void testSearchScroll() throws IOException {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        searchScrollRequest.scrollId(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            searchScrollRequest.scroll(randomPositiveTimeValue());
        }
        Request request = Request.searchScroll(searchScrollRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_search/scroll", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertToXContentBody(searchScrollRequest, request.getEntity());
        assertEquals(REQUEST_BODY_CONTENT_TYPE.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
    }

    public void testClearScroll() throws IOException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        int numScrolls = randomIntBetween(1, 10);
        for (int i = 0; i < numScrolls; i++) {
            clearScrollRequest.addScrollId(randomAlphaOfLengthBetween(5, 10));
        }
        Request request = Request.clearScroll(clearScrollRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_search/scroll", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertToXContentBody(clearScrollRequest, request.getEntity());
        assertEquals(REQUEST_BODY_CONTENT_TYPE.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
    }

    private static void assertToXContentBody(ToXContent expectedBody, HttpEntity actualEntity) throws IOException {
        BytesReference expectedBytes = XContentHelper.toXContent(expectedBody, REQUEST_BODY_CONTENT_TYPE, false);
        assertEquals(XContentType.JSON.mediaTypeWithoutParameters(), actualEntity.getContentType().getValue());
        assertEquals(expectedBytes, new BytesArray(EntityUtils.toByteArray(actualEntity)));
    }

    public void testParams() {
        final int nbParams = randomIntBetween(0, 10);
        Request.Params params = Request.Params.builder();
        Map<String, String> expectedParams = new HashMap<>();
        for (int i = 0; i < nbParams; i++) {
            String paramName = "p_" + i;
            String paramValue = randomAlphaOfLength(5);
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

    public void testCreateContentType() {
        final XContentType xContentType = randomFrom(XContentType.values());
        assertEquals(xContentType.mediaTypeWithoutParameters(), Request.createContentType(xContentType).getMimeType());
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
                String includesParam = randomFields(includes);
                if (numIncludes > 0) {
                    expectedParams.put("_source_include", includesParam);
                }
                int numExcludes = randomIntBetween(0, 5);
                String[] excludes = new String[numExcludes];
                String excludesParam = randomFields(excludes);
                if (numExcludes > 0) {
                    expectedParams.put("_source_exclude", excludesParam);
                }
                consumer.accept(new FetchSourceContext(true, includes, excludes));
            }
        }
    }

    private static void setRandomIndicesOptions(Consumer<IndicesOptions> setter, Supplier<IndicesOptions> getter,
                                                Map<String, String> expectedParams) {

        if (randomBoolean()) {
            setter.accept(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean()));
        }
        expectedParams.put("ignore_unavailable", Boolean.toString(getter.get().ignoreUnavailable()));
        expectedParams.put("allow_no_indices", Boolean.toString(getter.get().allowNoIndices()));
        if (getter.get().expandWildcardsOpen() && getter.get().expandWildcardsClosed()) {
            expectedParams.put("expand_wildcards", "open,closed");
        } else if (getter.get().expandWildcardsOpen()) {
            expectedParams.put("expand_wildcards", "open");
        } else if (getter.get().expandWildcardsClosed()) {
            expectedParams.put("expand_wildcards", "closed");
        } else {
            expectedParams.put("expand_wildcards", "none");
        }
    }

    private static void setRandomTimeout(Consumer<String> setter, TimeValue defaultTimeout, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            String timeout = randomTimeValue();
            setter.accept(timeout);
            expectedParams.put("timeout", timeout);
        } else {
            expectedParams.put("timeout", defaultTimeout.getStringRep());
        }
    }

    private static void setRandomMasterTimeout(MasterNodeRequest<?> request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            String masterTimeout = randomTimeValue();
            request.masterNodeTimeout(masterTimeout);
            expectedParams.put("master_timeout", masterTimeout);
        } else {
            expectedParams.put("master_timeout", MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT.getStringRep());
        }
    }

    private static void setRandomWaitForActiveShards(Consumer<ActiveShardCount> setter, ActiveShardCount defaultActiveShardCount,
                                                     Map<String, String> expectedParams) {
        if (randomBoolean()) {
            int waitForActiveShardsInt = randomIntBetween(-1, 5);
            String waitForActiveShardsString;
            if (waitForActiveShardsInt == -1) {
                waitForActiveShardsString = "all";
            } else {
                waitForActiveShardsString = String.valueOf(waitForActiveShardsInt);
            }
            ActiveShardCount activeShardCount = ActiveShardCount.parseString(waitForActiveShardsString);
            setter.accept(activeShardCount);
            if (defaultActiveShardCount.equals(activeShardCount) == false) {
                expectedParams.put("wait_for_active_shards", waitForActiveShardsString);
            }
        }
    }

    private static void setRandomRefreshPolicy(Consumer<WriteRequest.RefreshPolicy> setter, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
            setter.accept(refreshPolicy);
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

    private static void setRandomVersionType(Consumer<VersionType> setter, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            VersionType versionType = randomFrom(VersionType.values());
            setter.accept(versionType);
            if (versionType != VersionType.INTERNAL) {
                expectedParams.put("version_type", versionType.name().toLowerCase(Locale.ROOT));
            }
        }
    }

    private static String randomFields(String[] fields) {
        StringBuilder excludesParam = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            String exclude = randomAlphaOfLengthBetween(3, 10);
            fields[i] = exclude;
            excludesParam.append(exclude);
            if (i < fields.length - 1) {
                excludesParam.append(",");
            }
        }
        return excludesParam.toString();
    }

    private static String[] randomIndicesNames(int minIndicesNum, int maxIndicesNum) {
        int numIndices = randomIntBetween(minIndicesNum, maxIndicesNum);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = "index-" + randomAlphaOfLengthBetween(2, 5).toLowerCase(Locale.ROOT);
        }
        return indices;
    }
}
