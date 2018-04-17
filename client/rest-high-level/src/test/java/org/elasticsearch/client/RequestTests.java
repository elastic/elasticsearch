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
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
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
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedFunction;
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
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.rankeval.PrecisionAtK;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalSpec;
import org.elasticsearch.index.rankeval.RatedRequest;
import org.elasticsearch.index.rankeval.RestRankEvalAction;
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
import java.util.Collections;
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
import static org.elasticsearch.index.RandomCreateIndexGenerator.randomAliases;
import static org.elasticsearch.index.RandomCreateIndexGenerator.randomCreateIndexRequest;
import static org.elasticsearch.index.RandomCreateIndexGenerator.randomIndexSettings;
import static org.elasticsearch.index.alias.RandomAliasActionsGenerator.randomAliasAction;
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
        }

        Request request = Request.delete(deleteRequest);
        assertEquals("/" + index + "/" + type + "/" + id, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertNull(request.getEntity());
    }

    public void testExists() {
        getAndExistsTest(Request::exists, HttpHead.METHOD_NAME);
    }

    public void testIndicesExist() {
        String[] indices = randomIndicesNames(1, 10);

        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(getIndexRequest::indicesOptions, getIndexRequest::indicesOptions, expectedParams);
        setRandomLocal(getIndexRequest, expectedParams);
        setRandomHumanReadable(getIndexRequest, expectedParams);
        setRandomIncludeDefaults(getIndexRequest, expectedParams);

        final Request request = Request.indicesExist(getIndexRequest);

        assertEquals(HttpHead.METHOD_NAME, request.getMethod());
        assertEquals("/" + String.join(",", indices), request.getEndpoint());
        assertThat(expectedParams, equalTo(request.getParameters()));
        assertNull(request.getEntity());
    }

    public void testIndicesExistEmptyIndices() {
        expectThrows(IllegalArgumentException.class, () -> Request.indicesExist(new GetIndexRequest()));
        expectThrows(IllegalArgumentException.class, () -> Request.indicesExist(new GetIndexRequest().indices((String[])null)));
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
        CreateIndexRequest createIndexRequest = randomCreateIndexRequest();

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(createIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(createIndexRequest, expectedParams);
        setRandomWaitForActiveShards(createIndexRequest::waitForActiveShards, expectedParams);

        Request request = Request.createIndex(createIndexRequest);
        assertEquals("/" + createIndexRequest.index(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertToXContentBody(createIndexRequest, request.getEntity());
    }

    public void testCreateIndexNullIndex() {
        ActionRequestValidationException validationException = new CreateIndexRequest(null).validate();
        assertNotNull(validationException);
    }

    public void testUpdateAliases() throws IOException {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        AliasActions aliasAction = randomAliasAction();
        indicesAliasesRequest.addAliasAction(aliasAction);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(indicesAliasesRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(indicesAliasesRequest, expectedParams);

        Request request = Request.updateAliases(indicesAliasesRequest);
        assertEquals("/_aliases", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(indicesAliasesRequest, request.getEntity());
    }

    public void testPutMapping() throws IOException {
        PutMappingRequest putMappingRequest = new PutMappingRequest();

        String[] indices = randomIndicesNames(0, 5);
        putMappingRequest.indices(indices);

        String type = randomAlphaOfLengthBetween(3, 10);
        putMappingRequest.type(type);

        Map<String, String> expectedParams = new HashMap<>();

        setRandomTimeout(putMappingRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(putMappingRequest, expectedParams);

        Request request = Request.putMapping(putMappingRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_mapping");
        endpoint.add(type);
        assertEquals(endpoint.toString(), request.getEndpoint());

        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertToXContentBody(putMappingRequest, request.getEntity());
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

    public void testDeleteIndexEmptyIndices() {
        String[] indices = randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new DeleteIndexRequest(indices).validate();
        assertNotNull(validationException);
    }

    public void testOpenIndex() {
        String[] indices = randomIndicesNames(1, 5);
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indices);
        openIndexRequest.indices(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(openIndexRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(openIndexRequest, expectedParams);
        setRandomIndicesOptions(openIndexRequest::indicesOptions, openIndexRequest::indicesOptions, expectedParams);
        setRandomWaitForActiveShards(openIndexRequest::waitForActiveShards, expectedParams);

        Request request = Request.openIndex(openIndexRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "").add(String.join(",", indices)).add("_open");
        assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        assertThat(expectedParams, equalTo(request.getParameters()));
        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(request.getEntity(), nullValue());
    }

    public void testOpenIndexEmptyIndices() {
        String[] indices = randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new OpenIndexRequest(indices).validate();
        assertNotNull(validationException);
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

    public void testCloseIndexEmptyIndices() {
        String[] indices = randomBoolean() ? null : Strings.EMPTY_ARRAY;
        ActionRequestValidationException validationException = new CloseIndexRequest(indices).validate();
        assertNotNull(validationException);
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

    public void testRefresh() {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        RefreshRequest refreshRequest;
        if (randomBoolean()) {
            refreshRequest = new RefreshRequest(indices);
        } else {
            refreshRequest = new RefreshRequest();
            refreshRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(refreshRequest::indicesOptions, refreshRequest::indicesOptions, expectedParams);
        Request request = Request.refresh(refreshRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_refresh");
        assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testFlush() {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        FlushRequest flushRequest;
        if (randomBoolean()) {
            flushRequest = new FlushRequest(indices);
        } else {
            flushRequest = new FlushRequest();
            flushRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(flushRequest::indicesOptions, flushRequest::indicesOptions, expectedParams);
        if (randomBoolean()) {
            flushRequest.force(randomBoolean());
        }
        expectedParams.put("force", Boolean.toString(flushRequest.force()));
        if (randomBoolean()) {
            flushRequest.waitIfOngoing(randomBoolean());
        }
        expectedParams.put("wait_if_ongoing", Boolean.toString(flushRequest.waitIfOngoing()));

        Request request = Request.flush(flushRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_flush");
        assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testForceMerge() {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        ForceMergeRequest forceMergeRequest;
        if (randomBoolean()) {
            forceMergeRequest = new ForceMergeRequest(indices);
        } else {
            forceMergeRequest = new ForceMergeRequest();
            forceMergeRequest.indices(indices);
        }

        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(forceMergeRequest::indicesOptions, forceMergeRequest::indicesOptions, expectedParams);
        if (randomBoolean()) {
            forceMergeRequest.maxNumSegments(randomInt());
        }
        expectedParams.put("max_num_segments", Integer.toString(forceMergeRequest.maxNumSegments()));
        if (randomBoolean()) {
            forceMergeRequest.onlyExpungeDeletes(randomBoolean());
        }
        expectedParams.put("only_expunge_deletes", Boolean.toString(forceMergeRequest.onlyExpungeDeletes()));
        if (randomBoolean()) {
            forceMergeRequest.flush(randomBoolean());
        }
        expectedParams.put("flush", Boolean.toString(forceMergeRequest.flush()));

        Request request = Request.forceMerge(forceMergeRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_forcemerge");
        assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
    }

    public void testClearCache() {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        ClearIndicesCacheRequest clearIndicesCacheRequest;
        if (randomBoolean()) {
            clearIndicesCacheRequest = new ClearIndicesCacheRequest(indices);
        } else {
            clearIndicesCacheRequest = new ClearIndicesCacheRequest();
            clearIndicesCacheRequest.indices(indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(clearIndicesCacheRequest::indicesOptions, clearIndicesCacheRequest::indicesOptions, expectedParams);
        if (randomBoolean()) {
            clearIndicesCacheRequest.queryCache(randomBoolean());
        }
        expectedParams.put("query", Boolean.toString(clearIndicesCacheRequest.queryCache()));
        if (randomBoolean()) {
            clearIndicesCacheRequest.fieldDataCache(randomBoolean());
        }
        expectedParams.put("fielddata", Boolean.toString(clearIndicesCacheRequest.fieldDataCache()));
        if (randomBoolean()) {
            clearIndicesCacheRequest.requestCache(randomBoolean());
        }
        expectedParams.put("request", Boolean.toString(clearIndicesCacheRequest.requestCache()));
        if (randomBoolean()) {
            clearIndicesCacheRequest.fields(randomIndicesNames(1, 5));
            expectedParams.put("fields", String.join(",", clearIndicesCacheRequest.fields()));
        }

        Request request = Request.clearCache(clearIndicesCacheRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_cache/clear");
        assertThat(request.getEndpoint(), equalTo(endpoint.toString()));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
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
        setRandomWaitForActiveShards(updateRequest::waitForActiveShards, expectedParams);
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
            } else if (opType == DocWriteRequest.OpType.CREATE) {
                IndexRequest createRequest = new IndexRequest(index, type, id).source(source, xContentType).create(true);
                docWriteRequest = createRequest;
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                final UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(new IndexRequest().source(source, xContentType));
                docWriteRequest = updateRequest;
                if (randomBoolean()) {
                    updateRequest.retryOnConflict(randomIntBetween(1, 5));
                }
                if (randomBoolean()) {
                    randomizeFetchSourceContextParams(updateRequest::fetchSource, new HashMap<>());
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

    public void testSearchNullSource() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        Request request = Request.search(searchRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_search", request.getEndpoint());
        assertNull(request.getEntity());
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
            searchRequest.allowPartialSearchResults(randomBoolean());
            expectedParams.put("allow_partial_search_results", Boolean.toString(searchRequest.allowPartialSearchResults()));
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

    public void testSearchNullIndicesAndTypes() {
        expectThrows(NullPointerException.class, () -> new SearchRequest((String[]) null));
        expectThrows(NullPointerException.class, () -> new SearchRequest().indices((String[]) null));
        expectThrows(NullPointerException.class, () -> new SearchRequest().types((String[]) null));
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
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(p, false);
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

    public void testExistsAlias() {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 5);
        getAliasesRequest.indices(indices);
        //the HEAD endpoint requires at least an alias or an index
        boolean hasIndices = indices != null && indices.length > 0;
        String[] aliases;
        if (hasIndices) {
            aliases = randomBoolean() ? null : randomIndicesNames(0, 5);
        } else {
            aliases = randomIndicesNames(1, 5);
        }
        getAliasesRequest.aliases(aliases);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomLocal(getAliasesRequest, expectedParams);
        setRandomIndicesOptions(getAliasesRequest::indicesOptions, getAliasesRequest::indicesOptions, expectedParams);

        Request request = Request.existsAlias(getAliasesRequest);
        StringJoiner expectedEndpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            expectedEndpoint.add(String.join(",", indices));
        }
        expectedEndpoint.add("_alias");
        if (aliases != null && aliases.length > 0) {
            expectedEndpoint.add(String.join(",", aliases));
        }
        assertEquals(HttpHead.METHOD_NAME, request.getMethod());
        assertEquals(expectedEndpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    public void testExistsAliasNoAliasNoIndex() {
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Request.existsAlias(getAliasesRequest));
            assertEquals("existsAlias requires at least an alias or an index", iae.getMessage());
        }
        {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest((String[])null);
            getAliasesRequest.indices((String[])null);
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Request.existsAlias(getAliasesRequest));
            assertEquals("existsAlias requires at least an alias or an index", iae.getMessage());
        }
    }

    public void testRankEval() throws Exception {
        RankEvalSpec spec = new RankEvalSpec(
                Collections.singletonList(new RatedRequest("queryId", Collections.emptyList(), new SearchSourceBuilder())),
                new PrecisionAtK());
        String[] indices = randomIndicesNames(0, 5);
        RankEvalRequest rankEvalRequest = new RankEvalRequest(spec, indices);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(rankEvalRequest::indicesOptions, rankEvalRequest::indicesOptions, expectedParams);

        Request request = Request.rankEval(rankEvalRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add(RestRankEvalAction.ENDPOINT);
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(3, request.getParameters().size());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(spec, request.getEntity());

    }

    public void testSplit() throws IOException {
        resizeTest(ResizeType.SPLIT, Request::split);
    }

    public void testSplitWrongResizeType() {
        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SHRINK);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Request.split(resizeRequest));
        assertEquals("Wrong resize type [SHRINK] for indices split request", iae.getMessage());
    }

    public void testShrinkWrongResizeType() {
        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> Request.shrink(resizeRequest));
        assertEquals("Wrong resize type [SPLIT] for indices shrink request", iae.getMessage());
    }

    public void testShrink() throws IOException {
        resizeTest(ResizeType.SHRINK, Request::shrink);
    }

    private static void resizeTest(ResizeType resizeType, CheckedFunction<ResizeRequest, Request, IOException> function)
            throws IOException {
        String[] indices = randomIndicesNames(2, 2);
        ResizeRequest resizeRequest = new ResizeRequest(indices[0], indices[1]);
        resizeRequest.setResizeType(resizeType);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(resizeRequest, expectedParams);
        setRandomTimeout(resizeRequest::timeout, resizeRequest.timeout(), expectedParams);

        if (randomBoolean()) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(randomAlphaOfLengthBetween(3, 10));
            if (randomBoolean()) {
                createIndexRequest.settings(randomIndexSettings());
            }
            if (randomBoolean()) {
                randomAliases(createIndexRequest);
            }
            resizeRequest.setTargetIndex(createIndexRequest);
        }
        setRandomWaitForActiveShards(resizeRequest::setWaitForActiveShards, expectedParams);

        Request request = function.apply(resizeRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        String expectedEndpoint = "/" + resizeRequest.getSourceIndex() + "/_" + resizeType.name().toLowerCase(Locale.ROOT) + "/"
                + resizeRequest.getTargetIndexRequest().index();
        assertEquals(expectedEndpoint, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(resizeRequest, request.getEntity());
    }

    public void testClusterPutSettings() throws IOException {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(request, expectedParams);
        setRandomTimeout(request::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);

        Request expectedRequest = Request.clusterPutSettings(request);
        assertEquals("/_cluster/settings", expectedRequest.getEndpoint());
        assertEquals(HttpPut.METHOD_NAME, expectedRequest.getMethod());
        assertEquals(expectedParams, expectedRequest.getParameters());
    }

    public void testRollover() throws IOException {
        RolloverRequest rolloverRequest = new RolloverRequest(randomAlphaOfLengthBetween(3, 10),
                randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10));
        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(rolloverRequest::timeout, rolloverRequest.timeout(), expectedParams);
        setRandomMasterTimeout(rolloverRequest, expectedParams);
        if (randomBoolean()) {
            rolloverRequest.dryRun(randomBoolean());
            if (rolloverRequest.isDryRun()) {
                expectedParams.put("dry_run", "true");
            }
        }
        if (randomBoolean()) {
            rolloverRequest.addMaxIndexAgeCondition(new TimeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            String type = randomAlphaOfLengthBetween(3, 10);
            rolloverRequest.getCreateIndexRequest().mapping(type, RandomCreateIndexGenerator.randomMapping(type));
        }
        if (randomBoolean()) {
            RandomCreateIndexGenerator.randomAliases(rolloverRequest.getCreateIndexRequest());
        }
        if (randomBoolean()) {
            rolloverRequest.getCreateIndexRequest().settings(RandomCreateIndexGenerator.randomIndexSettings());
        }
        setRandomWaitForActiveShards(rolloverRequest.getCreateIndexRequest()::waitForActiveShards, expectedParams);

        Request request = Request.rollover(rolloverRequest);
        if (rolloverRequest.getNewIndexName() == null) {
            assertEquals("/" + rolloverRequest.getAlias() + "/_rollover", request.getEndpoint());
        } else {
            assertEquals("/" + rolloverRequest.getAlias() + "/_rollover/" + rolloverRequest.getNewIndexName(), request.getEndpoint());
        }
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertToXContentBody(rolloverRequest, request.getEntity());
        assertEquals(expectedParams, request.getParameters());
    }

    public void testIndexPutSettings() throws IOException {
        String[] indices = randomBoolean() ? null : randomIndicesNames(0, 2);
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indices);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(updateSettingsRequest, expectedParams);
        setRandomTimeout(updateSettingsRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomIndicesOptions(updateSettingsRequest::indicesOptions, updateSettingsRequest::indicesOptions, expectedParams);
        if (randomBoolean()) {
            updateSettingsRequest.setPreserveExisting(randomBoolean());
            if (updateSettingsRequest.isPreserveExisting()) {
                expectedParams.put("preserve_existing", "true");
            }
        }

        Request request = Request.indexPutSettings(updateSettingsRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        if (indices != null && indices.length > 0) {
            endpoint.add(String.join(",", indices));
        }
        endpoint.add("_settings");
        assertThat(endpoint.toString(), equalTo(request.getEndpoint()));
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertToXContentBody(updateSettingsRequest, request.getEntity());
        assertEquals(expectedParams, request.getParameters());
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

    public void testEndpointBuilder() {
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder();
            assertEquals("/", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart(Strings.EMPTY_ARRAY);
            assertEquals("/", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("");
            assertEquals("/", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("a", "b");
            assertEquals("/a/b", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("a").addPathPart("b")
                    .addPathPartAsIs("_create");
            assertEquals("/a/b/_create", endpointBuilder.build());
        }

        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("a", "b", "c")
                    .addPathPartAsIs("_create");
            assertEquals("/a/b/c/_create", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("a").addPathPartAsIs("_create");
            assertEquals("/a/_create", endpointBuilder.build());
        }
    }

    public void testEndpointBuilderEncodeParts() {
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("-#index1,index#2", "type", "id");
            assertEquals("/-%23index1,index%232/type/id", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("index", "type#2", "id");
            assertEquals("/index/type%232/id", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("index", "type", "this/is/the/id");
            assertEquals("/index/type/this%2Fis%2Fthe%2Fid", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("index", "type", "this|is|the|id");
            assertEquals("/index/type/this%7Cis%7Cthe%7Cid", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("index", "type", "id#1");
            assertEquals("/index/type/id%231", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("<logstash-{now/M}>", "_search");
            assertEquals("/%3Clogstash-%7Bnow%2FM%7D%3E/_search", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("中文");
            assertEquals("/中文", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("foo bar");
            assertEquals("/foo%20bar", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("foo+bar");
            assertEquals("/foo+bar", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("foo+bar");
            assertEquals("/foo+bar", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("foo/bar");
            assertEquals("/foo%2Fbar", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("foo^bar");
            assertEquals("/foo%5Ebar", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder().addPathPart("cluster1:index1,index2")
                    .addPathPartAsIs("_search");
            assertEquals("/cluster1:index1,index2/_search", endpointBuilder.build());
        }
        {
            Request.EndpointBuilder endpointBuilder = new Request.EndpointBuilder()
                    .addCommaSeparatedPathParts(new String[]{"index1", "index2"}).addPathPartAsIs("cache/clear");
            assertEquals("/index1,index2/cache/clear", endpointBuilder.build());
        }
    }

    public void testEndpoint() {
        assertEquals("/index/type/id", Request.endpoint("index", "type", "id"));
        assertEquals("/index/type/id/_endpoint", Request.endpoint("index", "type", "id", "_endpoint"));
        assertEquals("/index1,index2", Request.endpoint(new String[]{"index1", "index2"}));
        assertEquals("/index1,index2/_endpoint", Request.endpoint(new String[]{"index1", "index2"}, "_endpoint"));
        assertEquals("/index1,index2/type1,type2/_endpoint", Request.endpoint(new String[]{"index1", "index2"},
                new String[]{"type1", "type2"}, "_endpoint"));
        assertEquals("/index1,index2/_endpoint/suffix1,suffix2", Request.endpoint(new String[]{"index1", "index2"},
                "_endpoint", new String[]{"suffix1", "suffix2"}));
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

    private static void setRandomIncludeDefaults(GetIndexRequest request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            boolean includeDefaults = randomBoolean();
            request.includeDefaults(includeDefaults);
            if (includeDefaults) {
                expectedParams.put("include_defaults", String.valueOf(includeDefaults));
            }
        }
    }

    private static void setRandomHumanReadable(GetIndexRequest request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            boolean humanReadable = randomBoolean();
            request.humanReadable(humanReadable);
            if (humanReadable) {
                expectedParams.put("human", String.valueOf(humanReadable));
            }
        }
    }

    private static void setRandomLocal(MasterNodeReadRequest<?> request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            boolean local = randomBoolean();
            request.local(local);
            if (local) {
                expectedParams.put("local", String.valueOf(local));
            }
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

    private static void setRandomWaitForActiveShards(Consumer<ActiveShardCount> setter, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            String waitForActiveShardsString;
            int waitForActiveShards = randomIntBetween(-1, 5);
            if (waitForActiveShards == -1) {
                waitForActiveShardsString = "all";
            } else {
                waitForActiveShardsString = String.valueOf(waitForActiveShards);
            }
            setter.accept(ActiveShardCount.parseString(waitForActiveShardsString));
            expectedParams.put("wait_for_active_shards", waitForActiveShardsString);
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
