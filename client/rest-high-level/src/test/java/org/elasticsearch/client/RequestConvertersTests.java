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
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
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
import org.elasticsearch.client.RequestConverters.EndpointBuilder;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.rankeval.PrecisionAtK;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalSpec;
import org.elasticsearch.index.rankeval.RatedRequest;
import org.elasticsearch.index.rankeval.RestRankEvalAction;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
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
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.enforceSameContentType;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.nullValue;

public class RequestConvertersTests extends ESTestCase {
    public void testPing() {
        Request request = RequestConverters.ping();
        assertEquals("/", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertNull(request.getEntity());
        assertEquals(HttpHead.METHOD_NAME, request.getMethod());
    }

    public void testInfo() {
        Request request = RequestConverters.info();
        assertEquals("/", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertNull(request.getEntity());
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
    }

    public void testGet() {
        getAndExistsTest(RequestConverters::get, HttpGet.METHOD_NAME);
    }

    public void testSourceExists() throws IOException {
        doTestSourceExists((index, id) -> new GetSourceRequest(index, id));
    }

    public void testGetSource() throws IOException {
        doTestGetSource((index, id) -> new GetSourceRequest(index, id));
    }

    private static void doTestSourceExists(BiFunction<String, String, GetSourceRequest> requestFunction) throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        final GetSourceRequest getRequest = requestFunction.apply(index, id);

        Map<String, String> expectedParams = new HashMap<>();
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
        Request request = RequestConverters.sourceExists(getRequest);
        assertEquals(HttpHead.METHOD_NAME, request.getMethod());
        assertEquals("/" + index + "/_source/" + id, request.getEndpoint());

        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    private static void doTestGetSource(BiFunction<String, String, GetSourceRequest> requestFunction) throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        final GetSourceRequest getRequest = requestFunction.apply(index, id);

        Map<String, String> expectedParams = new HashMap<>();
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
        Request request = RequestConverters.getSource(getRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/" + index + "/_source/" + id, request.getEndpoint());

        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
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
            MultiGetRequest.Item item = new MultiGetRequest.Item(randomAlphaOfLength(4), randomAlphaOfLength(4));
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

        Request request = RequestConverters.multiGet(multiGetRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_mget", request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(multiGetRequest, request.getEntity());
    }

    public void testMultiGetWithType() throws IOException {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        MultiGetRequest.Item item = new MultiGetRequest.Item(randomAlphaOfLength(4),
            randomAlphaOfLength(4));
        multiGetRequest.add(item);

        Request request = RequestConverters.multiGet(multiGetRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_mget", request.getEndpoint());
        assertToXContentBody(multiGetRequest, request.getEntity());
    }

    public void testDelete() {
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        DeleteRequest deleteRequest = new DeleteRequest(index, id);

        Map<String, String> expectedParams = new HashMap<>();

        setRandomTimeout(deleteRequest::timeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomRefreshPolicy(deleteRequest::setRefreshPolicy, expectedParams);
        setRandomVersion(deleteRequest, expectedParams);
        setRandomVersionType(deleteRequest::versionType, expectedParams);
        setRandomIfSeqNoAndTerm(deleteRequest, expectedParams);

        if (frequently()) {
            if (randomBoolean()) {
                String routing = randomAlphaOfLengthBetween(3, 10);
                deleteRequest.routing(routing);
                expectedParams.put("routing", routing);
            }
        }

        Request request = RequestConverters.delete(deleteRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/" + index + "/_doc/" + id, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
    }

    public void testExists() {
        getAndExistsTest(RequestConverters::exists, HttpHead.METHOD_NAME);
    }

    private static void getAndExistsTest(Function<GetRequest, Request> requestConverter, String method) {
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        GetRequest getRequest = new GetRequest(index, id);

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
        assertEquals("/" + index + "/_doc/" + id, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertNull(request.getEntity());
        assertEquals(method, request.getMethod());
    }

    public void testReindex() throws IOException {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("source_idx");
        reindexRequest.setDestIndex("dest_idx");
        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
            RemoteInfo remoteInfo = new RemoteInfo("http", "remote-host", 9200, null,
                BytesReference.bytes(matchAllQuery().toXContent(builder, ToXContent.EMPTY_PARAMS)),
                "user",
                "pass",
                emptyMap(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                RemoteInfo.DEFAULT_CONNECT_TIMEOUT
            );
            reindexRequest.setRemoteInfo(remoteInfo);
        }
        if (randomBoolean()) {
            reindexRequest.setSourceBatchSize(randomInt(100));
        }
        if (randomBoolean()) {
            reindexRequest.setDestOpType("create");
        }
        if (randomBoolean()) {
            reindexRequest.setDestPipeline("my_pipeline");
        }
        if (randomBoolean()) {
            float requestsPerSecond = (float) randomDoubleBetween(0.0, 10.0, false);
            expectedParams.put(RethrottleRequest.REQUEST_PER_SECOND_PARAMETER, Float.toString(requestsPerSecond));
            reindexRequest.setRequestsPerSecond(requestsPerSecond);
        } else {
            expectedParams.put(RethrottleRequest.REQUEST_PER_SECOND_PARAMETER, "-1");
        }
        if (randomBoolean()) {
            reindexRequest.setDestRouting("=cat");
        }
        if (randomBoolean()) {
            reindexRequest.setMaxDocs(randomIntBetween(100, 1000));
        }
        if (randomBoolean()) {
            reindexRequest.setAbortOnVersionConflict(false);
        }
        if (randomBoolean()) {
            String ts = randomTimeValue();
            reindexRequest.setScroll(TimeValue.parseTimeValue(ts, "scroll"));
        }
        if (reindexRequest.getRemoteInfo() == null && randomBoolean()) {
            reindexRequest.setSourceQuery(new TermQueryBuilder("foo", "fooval"));
        }
        if (randomBoolean()) {
            int slices = randomIntBetween(0,4);
            reindexRequest.setSlices(slices);
            if (slices == 0) {
                expectedParams.put("slices", AbstractBulkByScrollRequest.AUTO_SLICES_VALUE);
            } else {
                expectedParams.put("slices", Integer.toString(slices));
            }
        } else {
            expectedParams.put("slices", "1");
        }
        setRandomTimeout(reindexRequest::setTimeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        setRandomWaitForActiveShards(reindexRequest::setWaitForActiveShards, ActiveShardCount.DEFAULT, expectedParams);
        expectedParams.put("scroll", reindexRequest.getScrollTime().getStringRep());
        expectedParams.put("wait_for_completion", Boolean.TRUE.toString());
        Request request = RequestConverters.reindex(reindexRequest);
        assertEquals("/_reindex", request.getEndpoint());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(reindexRequest, request.getEntity());
    }

    public void testUpdateByQuery() throws IOException {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(randomIndicesNames(1, 5));
        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            int batchSize = randomInt(100);
            updateByQueryRequest.setBatchSize(batchSize);
            expectedParams.put("scroll_size", Integer.toString(batchSize));
        }
        if (randomBoolean()) {
            updateByQueryRequest.setPipeline("my_pipeline");
            expectedParams.put("pipeline", "my_pipeline");
        }
        if (randomBoolean()) {
            float requestsPerSecond = (float) randomDoubleBetween(0.0, 10.0, false);
            expectedParams.put("requests_per_second", Float.toString(requestsPerSecond));
            updateByQueryRequest.setRequestsPerSecond(requestsPerSecond);
        } else {
            expectedParams.put("requests_per_second", "-1");
        }
        if (randomBoolean()) {
            updateByQueryRequest.setRouting("=cat");
            expectedParams.put("routing", "=cat");
        }
        if (randomBoolean()) {
            int maxDocs = randomIntBetween(100, 1000);
            updateByQueryRequest.setMaxDocs(maxDocs);
            expectedParams.put("max_docs", Integer.toString(maxDocs));
        }
        if (randomBoolean()) {
            updateByQueryRequest.setAbortOnVersionConflict(false);
            expectedParams.put("conflicts", "proceed");
        }
        if (randomBoolean()) {
            String ts = randomTimeValue();
            updateByQueryRequest.setScroll(TimeValue.parseTimeValue(ts, "scroll"));
            expectedParams.put("scroll", ts);
        }
        if (randomBoolean()) {
            updateByQueryRequest.setQuery(new TermQueryBuilder("foo", "fooval"));
        }
        if (randomBoolean()) {
            updateByQueryRequest.setScript(new Script("ctx._source.last = \"lastname\""));
        }
        if (randomBoolean()) {
            int slices = randomIntBetween(0, 4);
            if (slices == 0) {
                expectedParams.put("slices", AbstractBulkByScrollRequest.AUTO_SLICES_VALUE);
            } else {
                expectedParams.put("slices", Integer.toString(slices));
            }
            updateByQueryRequest.setSlices(slices);
        } else {
            expectedParams.put("slices", "1");
        }
        setRandomIndicesOptions(updateByQueryRequest::setIndicesOptions, updateByQueryRequest::indicesOptions, expectedParams);
        setRandomTimeout(updateByQueryRequest::setTimeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        Request request = RequestConverters.updateByQuery(updateByQueryRequest);
        StringJoiner joiner = new StringJoiner("/", "/", "");
        joiner.add(String.join(",", updateByQueryRequest.indices()));
        joiner.add("_update_by_query");
        assertEquals(joiner.toString(), request.getEndpoint());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(updateByQueryRequest, request.getEntity());
    }

    public void testDeleteByQuery() throws IOException {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
        deleteByQueryRequest.indices(randomIndicesNames(1, 5));
        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            int batchSize = randomInt(100);
            deleteByQueryRequest.setBatchSize(batchSize);
            expectedParams.put("scroll_size", Integer.toString(batchSize));
        }
        if (randomBoolean()) {
            deleteByQueryRequest.setRouting("=cat");
            expectedParams.put("routing", "=cat");
        }
        if (randomBoolean()) {
            int maxDocs = randomIntBetween(100, 1000);
            deleteByQueryRequest.setMaxDocs(maxDocs);
            expectedParams.put("max_docs", Integer.toString(maxDocs));
        }
        if (randomBoolean()) {
            deleteByQueryRequest.setAbortOnVersionConflict(false);
            expectedParams.put("conflicts", "proceed");
        }
        if (randomBoolean()) {
            String ts = randomTimeValue();
            deleteByQueryRequest.setScroll(TimeValue.parseTimeValue(ts, "scroll"));
            expectedParams.put("scroll", ts);
        }
        if (randomBoolean()) {
            deleteByQueryRequest.setQuery(new TermQueryBuilder("foo", "fooval"));
        }
        if (randomBoolean()) {
            float requestsPerSecond = (float) randomDoubleBetween(0.0, 10.0, false);
            expectedParams.put("requests_per_second", Float.toString(requestsPerSecond));
            deleteByQueryRequest.setRequestsPerSecond(requestsPerSecond);
        } else {
            expectedParams.put("requests_per_second", "-1");
        }
        if (randomBoolean()) {
            int slices = randomIntBetween(0, 4);
            if (slices == 0) {
                expectedParams.put("slices", AbstractBulkByScrollRequest.AUTO_SLICES_VALUE);
            } else {
                expectedParams.put("slices", Integer.toString(slices));
            }
            deleteByQueryRequest.setSlices(slices);
        } else {
            expectedParams.put("slices", "1");
        }
        setRandomIndicesOptions(deleteByQueryRequest::setIndicesOptions, deleteByQueryRequest::indicesOptions, expectedParams);
        setRandomTimeout(deleteByQueryRequest::setTimeout, ReplicationRequest.DEFAULT_TIMEOUT, expectedParams);
        expectedParams.put("wait_for_completion", Boolean.TRUE.toString());
        Request request = RequestConverters.deleteByQuery(deleteByQueryRequest);
        StringJoiner joiner = new StringJoiner("/", "/", "");
        joiner.add(String.join(",", deleteByQueryRequest.indices()));
        joiner.add("_delete_by_query");
        assertEquals(joiner.toString(), request.getEndpoint());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(deleteByQueryRequest, request.getEntity());
    }

    public void testRethrottle() {
        TaskId taskId = new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 100));
        RethrottleRequest rethrottleRequest;
        Float requestsPerSecond;
        Map<String, String> expectedParams = new HashMap<>();
        if (frequently()) {
            requestsPerSecond =  (float) randomDoubleBetween(0.0, 100.0, true);
            rethrottleRequest = new RethrottleRequest(taskId, requestsPerSecond);
            expectedParams.put(RethrottleRequest.REQUEST_PER_SECOND_PARAMETER, Float.toString(requestsPerSecond));
        } else {
            rethrottleRequest = new RethrottleRequest(taskId);
            expectedParams.put(RethrottleRequest.REQUEST_PER_SECOND_PARAMETER, "-1");
        }
        expectedParams.put("group_by", "none");
        List<Tuple<String, Supplier<Request>>> variants = new ArrayList<>();
        variants.add(new Tuple<String, Supplier<Request>>("_reindex", () -> RequestConverters.rethrottleReindex(rethrottleRequest)));
        variants.add(new Tuple<String, Supplier<Request>>("_update_by_query",
                () -> RequestConverters.rethrottleUpdateByQuery(rethrottleRequest)));
        variants.add(new Tuple<String, Supplier<Request>>("_delete_by_query",
                () -> RequestConverters.rethrottleDeleteByQuery(rethrottleRequest)));

        for (Tuple<String, Supplier<Request>> variant : variants) {
            Request request = variant.v2().get();
            assertEquals("/" + variant.v1() + "/" + taskId + "/_rethrottle", request.getEndpoint());
            assertEquals(HttpPost.METHOD_NAME, request.getMethod());
            assertEquals(expectedParams, request.getParameters());
            assertNull(request.getEntity());
        }

        // test illegal RethrottleRequest values
        Exception e = expectThrows(NullPointerException.class, () -> new RethrottleRequest(null, 1.0f));
        assertEquals("taskId cannot be null", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RethrottleRequest(new TaskId("taskId", 1), -5.0f));
        assertEquals("requestsPerSecond needs to be positive value but was [-5.0]", e.getMessage());
    }

    public void testIndex() throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        IndexRequest indexRequest = new IndexRequest(index);

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
            setRandomIfSeqNoAndTerm(indexRequest, expectedParams);
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

        Request request = RequestConverters.index(indexRequest);
        if (indexRequest.opType() == DocWriteRequest.OpType.CREATE) {
            assertEquals("/" + index + "/_create/" + id, request.getEndpoint());
        } else if (id != null) {
            assertEquals("/" + index + "/_doc/" + id, request.getEndpoint());
        } else {
            assertEquals("/" + index + "/_doc", request.getEndpoint());
        }
        assertEquals(expectedParams, request.getParameters());
        assertEquals(method, request.getMethod());

        HttpEntity entity = request.getEntity();
        assertTrue(entity instanceof NByteArrayEntity);
        assertEquals(indexRequest.getContentType().mediaTypeWithoutParameters(), entity.getContentType().getValue());
        try (XContentParser parser = createParser(xContentType.xContent(), entity.getContent())) {
            assertEquals(nbFields, parser.map().size());
        }
    }

    public void testUpdate() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());

        Map<String, String> expectedParams = new HashMap<>();
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);

        UpdateRequest updateRequest = new UpdateRequest(index, id);
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
        setRandomIfSeqNoAndTerm(updateRequest, new HashMap<>()); // if* params are passed in the body
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

        Request request = RequestConverters.update(updateRequest);
        assertEquals("/" + index + "/_update/" + id, request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());

        HttpEntity entity = request.getEntity();
        assertTrue(entity instanceof NByteArrayEntity);

        UpdateRequest parsedUpdateRequest = new UpdateRequest();

        XContentType entityContentType = XContentType.fromMediaTypeOrFormat(entity.getContentType().getValue());
        try (XContentParser parser = createParser(entityContentType.xContent(), entity.getContent())) {
            parsedUpdateRequest.fromXContent(parser);
        }

        assertEquals(updateRequest.scriptedUpsert(), parsedUpdateRequest.scriptedUpsert());
        assertEquals(updateRequest.docAsUpsert(), parsedUpdateRequest.docAsUpsert());
        assertEquals(updateRequest.detectNoop(), parsedUpdateRequest.detectNoop());
        assertEquals(updateRequest.fetchSource(), parsedUpdateRequest.fetchSource());
        assertIfSeqNoAndTerm(updateRequest, parsedUpdateRequest);
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

    private static void assertIfSeqNoAndTerm(DocWriteRequest<?>request, DocWriteRequest<?> parsedRequest) {
        assertEquals(request.ifSeqNo(), parsedRequest.ifSeqNo());
        assertEquals(request.ifPrimaryTerm(), parsedRequest.ifPrimaryTerm());
    }

    private static void setRandomIfSeqNoAndTerm(DocWriteRequest<?> request, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            final long seqNo = randomNonNegativeLong();
            request.setIfSeqNo(seqNo);
            expectedParams.put("if_seq_no", Long.toString(seqNo));
            final long primaryTerm = randomLongBetween(1, 200);
            request.setIfPrimaryTerm(primaryTerm);
            expectedParams.put("if_primary_term", Long.toString(primaryTerm));
        }
    }

    public void testUpdateWithDifferentContentTypes() {
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.doc(new IndexRequest().source(singletonMap("field", "doc"), XContentType.JSON));
            updateRequest.upsert(new IndexRequest().source(singletonMap("field", "upsert"), XContentType.YAML));
            RequestConverters.update(updateRequest);
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
        DocWriteRequest<?>[] requests = new DocWriteRequest<?>[nbItems];
        for (int i = 0; i < nbItems; i++) {
            String index = randomAlphaOfLength(5);
            String id = randomAlphaOfLength(5);

            BytesReference source = RandomObjects.randomSource(random(), xContentType);
            DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());

            DocWriteRequest<?> docWriteRequest;
            if (opType == DocWriteRequest.OpType.INDEX) {
                IndexRequest indexRequest = new IndexRequest(index).id(id).source(source, xContentType);
                docWriteRequest = indexRequest;
                if (randomBoolean()) {
                    indexRequest.setPipeline(randomAlphaOfLength(5));
                }
            } else if (opType == DocWriteRequest.OpType.CREATE) {
                IndexRequest createRequest = new IndexRequest(index).id(id).source(source, xContentType).create(true);
                docWriteRequest = createRequest;
            } else if (opType == DocWriteRequest.OpType.UPDATE) {
                final UpdateRequest updateRequest = new UpdateRequest(index, id).doc(new IndexRequest().source(source, xContentType));
                docWriteRequest = updateRequest;
                if (randomBoolean()) {
                    updateRequest.retryOnConflict(randomIntBetween(1, 5));
                }
                if (randomBoolean()) {
                    randomizeFetchSourceContextParams(updateRequest::fetchSource, new HashMap<>());
                }
            } else if (opType == DocWriteRequest.OpType.DELETE) {
                docWriteRequest = new DeleteRequest(index, id);
            } else {
                throw new UnsupportedOperationException("optype [" + opType + "] not supported");
            }

            if (randomBoolean()) {
                docWriteRequest.routing(randomAlphaOfLength(10));
            }
            if (opType != DocWriteRequest.OpType.UPDATE && randomBoolean()) {
                docWriteRequest.setIfSeqNo(randomNonNegativeLong());
                docWriteRequest.setIfPrimaryTerm(randomLongBetween(1, 200));
            }
            requests[i] = docWriteRequest;
        }
        bulkRequest.add(requests);

        Request request = RequestConverters.bulk(bulkRequest);
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
            assertEquals(originalRequest.id(), parsedRequest.id());
            assertEquals(originalRequest.routing(), parsedRequest.routing());
            assertEquals(originalRequest.version(), parsedRequest.version());
            assertEquals(originalRequest.versionType(), parsedRequest.versionType());
            assertEquals(originalRequest.ifSeqNo(), parsedRequest.ifSeqNo());
            assertEquals(originalRequest.ifPrimaryTerm(), parsedRequest.ifPrimaryTerm());

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
            bulkRequest.add(new DeleteRequest("index", "0"));
            bulkRequest.add(new UpdateRequest("index", "1").script(mockScript("test")));
            bulkRequest.add(new DeleteRequest("index", "2"));

            Request request = RequestConverters.bulk(bulkRequest);
            assertEquals(XContentType.JSON.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        }
        {
            XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new DeleteRequest("index", "0"));
            bulkRequest.add(new IndexRequest("index").id("0").source(singletonMap("field", "value"), xContentType));
            bulkRequest.add(new DeleteRequest("index", "2"));

            Request request = RequestConverters.bulk(bulkRequest);
            assertEquals(xContentType.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        }
        {
            XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
            UpdateRequest updateRequest = new UpdateRequest("index", "0");
            if (randomBoolean()) {
                updateRequest.doc(new IndexRequest().source(singletonMap("field", "value"), xContentType));
            } else {
                updateRequest.upsert(new IndexRequest().source(singletonMap("field", "value"), xContentType));
            }

            Request request = RequestConverters.bulk(new BulkRequest().add(updateRequest));
            assertEquals(xContentType.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        }
        {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new IndexRequest("index").id("0").source(singletonMap("field", "value"), XContentType.SMILE));
            bulkRequest.add(new IndexRequest("index").id("1").source(singletonMap("field", "value"), XContentType.JSON));
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RequestConverters.bulk(bulkRequest));
            assertEquals(
                    "Mismatching content-type found for request with content-type [JSON], " + "previous requests have content-type [SMILE]",
                    exception.getMessage());
        }
        {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new IndexRequest("index").id("0").source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new IndexRequest("index").id("1").source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new UpdateRequest("index", "2")
                    .doc(new IndexRequest().source(singletonMap("field", "value"), XContentType.JSON))
                    .upsert(new IndexRequest().source(singletonMap("field", "value"), XContentType.SMILE)));
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RequestConverters.bulk(bulkRequest));
            assertEquals(
                    "Mismatching content-type found for request with content-type [SMILE], " + "previous requests have content-type [JSON]",
                    exception.getMessage());
        }
        {
            XContentType xContentType = randomFrom(XContentType.CBOR, XContentType.YAML);
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new DeleteRequest("index", "0"));
            bulkRequest.add(new IndexRequest("index").id("1").source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new DeleteRequest("index", "2"));
            bulkRequest.add(new DeleteRequest("index", "3"));
            bulkRequest.add(new IndexRequest("index").id("4").source(singletonMap("field", "value"), XContentType.JSON));
            bulkRequest.add(new IndexRequest("index").id("1").source(singletonMap("field", "value"), xContentType));
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> RequestConverters.bulk(bulkRequest));
            assertEquals("Unsupported content-type found for request with content-type [" + xContentType
                    + "], only JSON and SMILE are supported", exception.getMessage());
        }
    }

    public void testGlobalPipelineOnBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.pipeline("xyz");
        bulkRequest.add(new IndexRequest("test").id("11")
            .source(XContentType.JSON, "field", "bulk1"));
        bulkRequest.add(new IndexRequest("test").id("12")
            .source(XContentType.JSON, "field", "bulk2"));
        bulkRequest.add(new IndexRequest("test").id("13")
            .source(XContentType.JSON, "field", "bulk3"));

        Request request = RequestConverters.bulk(bulkRequest);

        assertThat(request.getParameters(), Matchers.hasEntry("pipeline","xyz"));
    }

    public void testSearchNullSource() throws IOException {
        String searchEndpoint = randomFrom("_" + randomAlphaOfLength(5));
        SearchRequest searchRequest = new SearchRequest();
        Request request = RequestConverters.search(searchRequest, searchEndpoint);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/" + searchEndpoint, request.getEndpoint());
        assertNull(request.getEntity());
    }

    public void testSearch() throws Exception {
        String searchEndpoint = randomFrom("_" + randomAlphaOfLength(5));
        String[] indices = randomIndicesNames(0, 5);
        Map<String, String> expectedParams = new HashMap<>();
        SearchRequest searchRequest = createTestSearchRequest(indices, expectedParams);

        Request request = RequestConverters.search(searchRequest, searchEndpoint);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add(searchEndpoint);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(searchRequest.source(), request.getEntity());
    }

    public static SearchRequest createTestSearchRequest(String[] indices, Map<String, String> expectedParams) {
        SearchRequest searchRequest = new SearchRequest(indices);

        setRandomSearchParams(searchRequest, expectedParams);
        setRandomIndicesOptions(searchRequest::indicesOptions, searchRequest::indicesOptions, expectedParams);

        SearchSourceBuilder searchSourceBuilder = createTestSearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    public static SearchSourceBuilder createTestSearchSourceBuilder() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // rarely skip setting the search source completely
        if (frequently()) {
            // frequently set the search source to have some content, otherwise leave it
            // empty but still set it
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
                    searchSourceBuilder.aggregation(new TermsAggregationBuilder(randomAlphaOfLengthBetween(3, 10))
                            .userValueTypeHint(ValueType.STRING)
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
        }
        return searchSourceBuilder;
    }


    public void testSearchNullIndicesAndTypes() {
        expectThrows(NullPointerException.class, () -> new SearchRequest((String[]) null));
        expectThrows(NullPointerException.class, () -> new SearchRequest().indices((String[]) null));
    }

     public void testCountNotNullSource() throws IOException {
        //as we create SearchSourceBuilder in CountRequest constructor
        CountRequest countRequest = new CountRequest();
        Request request = RequestConverters.count(countRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_count", request.getEndpoint());
        assertNotNull(request.getEntity());
    }

    public void testCount() throws Exception {
        String[] indices = randomIndicesNames(0, 5);
        CountRequest countRequest = new CountRequest(indices);

        int numTypes = randomIntBetween(0, 5);
        String[] types = new String[numTypes];
        for (int i = 0; i < numTypes; i++) {
            types[i] = "type-" + randomAlphaOfLengthBetween(2, 5);
        }
        countRequest.types(types);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomCountParams(countRequest, expectedParams);
        setRandomIndicesOptions(countRequest::indicesOptions, countRequest::indicesOptions, expectedParams);

        if (randomBoolean()) {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            countRequest.source(searchSourceBuilder);
        } else {
            countRequest.query(new MatchAllQueryBuilder());
        }
        Request request = RequestConverters.count(countRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        String type = String.join(",", types);
        if (Strings.hasLength(type)) {
            endpoint.add(type);
        }
        endpoint.add("_count");
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(countRequest, request.getEntity());
    }

    public void testCountNullIndicesAndTypes() {
        expectThrows(NullPointerException.class, () -> new CountRequest((String[]) null));
        expectThrows(NullPointerException.class, () -> new CountRequest().indices((String[]) null));
        expectThrows(NullPointerException.class, () -> new CountRequest().types((String[]) null));
    }

    private static void setRandomCountParams(CountRequest countRequest,
                                             Map<String, String> expectedParams) {
        if (randomBoolean()) {
            countRequest.routing(randomAlphaOfLengthBetween(3, 10));
            expectedParams.put("routing", countRequest.routing());
        }
        if (randomBoolean()) {
            countRequest.preference(randomAlphaOfLengthBetween(3, 10));
            expectedParams.put("preference", countRequest.preference());
        }
        if (randomBoolean()) {
            countRequest.terminateAfter(randomIntBetween(0, Integer.MAX_VALUE));
            expectedParams.put("terminate_after", String.valueOf(countRequest.terminateAfter()));
        }
        if (randomBoolean()) {
            countRequest.minScore((float) randomIntBetween(1, 10));
            expectedParams.put("min_score", String.valueOf(countRequest.minScore()));
        }
    }

    public void testMultiSearch() throws IOException {
        int numberOfSearchRequests = randomIntBetween(0, 32);
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (int i = 0; i < numberOfSearchRequests; i++) {
            SearchRequest searchRequest = randomSearchRequest(() -> {
                // No need to return a very complex SearchSourceBuilder here, that is tested
                // elsewhere
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.from(randomInt(10));
                searchSourceBuilder.size(randomIntBetween(20, 100));
                return searchSourceBuilder;
            });
            // scroll is not supported in the current msearch api, so unset it:
            searchRequest.scroll((Scroll) null);
            // only expand_wildcards, ignore_unavailable and allow_no_indices can be
            // specified from msearch api, so unset other options:
            IndicesOptions randomlyGenerated = searchRequest.indicesOptions();
            IndicesOptions msearchDefault = new MultiSearchRequest().indicesOptions();
            searchRequest.indicesOptions(IndicesOptions.fromOptions(randomlyGenerated.ignoreUnavailable(),
                    randomlyGenerated.allowNoIndices(), randomlyGenerated.expandWildcardsOpen(), randomlyGenerated.expandWildcardsClosed(),
                    msearchDefault.expandWildcardsHidden(), msearchDefault.allowAliasesToMultipleIndices(),
                    msearchDefault.forbidClosedIndices(), msearchDefault.ignoreAliases(), msearchDefault.ignoreThrottled()));
            multiSearchRequest.add(searchRequest);
        }

        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put(RestSearchAction.TYPED_KEYS_PARAM, "true");
        if (randomBoolean()) {
            multiSearchRequest.maxConcurrentSearchRequests(randomIntBetween(1, 8));
            expectedParams.put("max_concurrent_searches", Integer.toString(multiSearchRequest.maxConcurrentSearchRequests()));
        }

        Request request = RequestConverters.multiSearch(multiSearchRequest);
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
                REQUEST_BODY_CONTENT_TYPE.xContent(), consumer, null, multiSearchRequest.indicesOptions(), null, null, null,
                xContentRegistry(), true);
        assertEquals(requests, multiSearchRequest.requests());
    }

    public void testSearchScroll() throws IOException {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        searchScrollRequest.scrollId(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            searchScrollRequest.scroll(randomPositiveTimeValue());
        }
        Request request = RequestConverters.searchScroll(searchScrollRequest);
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
        Request request = RequestConverters.clearScroll(clearScrollRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_search/scroll", request.getEndpoint());
        assertEquals(0, request.getParameters().size());
        assertToXContentBody(clearScrollRequest, request.getEntity());
        assertEquals(REQUEST_BODY_CONTENT_TYPE.mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
    }

    public void testSearchTemplate() throws Exception {
        // Create a random request.
        String[] indices = randomIndicesNames(0, 5);
        SearchRequest searchRequest = new SearchRequest(indices);

        Map<String, String> expectedParams = new HashMap<>();
        setRandomSearchParams(searchRequest, expectedParams);
        setRandomIndicesOptions(searchRequest::indicesOptions, searchRequest::indicesOptions, expectedParams);

        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest(searchRequest);

        searchTemplateRequest.setScript("{\"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" }}}");
        searchTemplateRequest.setScriptType(ScriptType.INLINE);
        searchTemplateRequest.setProfile(randomBoolean());

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("field", "name");
        scriptParams.put("value", "soren");
        searchTemplateRequest.setScriptParams(scriptParams);

        // Verify that the resulting REST request looks as expected.
        Request request = RequestConverters.searchTemplate(searchTemplateRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_search/template");

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(searchTemplateRequest, request.getEntity());
    }

    public void testRenderSearchTemplate() throws Exception {
        // Create a simple request.
        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();
        searchTemplateRequest.setSimulate(true); // Setting simulate true means the template should only be rendered.

        searchTemplateRequest.setScript("template1");
        searchTemplateRequest.setScriptType(ScriptType.STORED);
        searchTemplateRequest.setProfile(randomBoolean());

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("field", "name");
        scriptParams.put("value", "soren");
        searchTemplateRequest.setScriptParams(scriptParams);

        // Verify that the resulting REST request looks as expected.
        Request request = RequestConverters.searchTemplate(searchTemplateRequest);
        String endpoint = "_render/template";

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(endpoint, request.getEndpoint());
        assertEquals(Collections.emptyMap(), request.getParameters());
        assertToXContentBody(searchTemplateRequest, request.getEntity());
    }

    public void testMultiSearchTemplate() throws Exception {
        final int numSearchRequests = randomIntBetween(1, 10);
        MultiSearchTemplateRequest multiSearchTemplateRequest = new MultiSearchTemplateRequest();

        for (int i = 0; i < numSearchRequests; i++) {
            // Create a random request.
            String[] indices = randomIndicesNames(0, 5);
            SearchRequest searchRequest = new SearchRequest(indices);

            Map<String, String> expectedParams = new HashMap<>();
            setRandomSearchParams(searchRequest, expectedParams);

            // scroll is not supported in the current msearch or msearchtemplate api, so unset it:
            searchRequest.scroll((Scroll) null);
            // batched reduce size is currently not set-able on a per-request basis as it is a query string parameter only
            searchRequest.setBatchedReduceSize(SearchRequest.DEFAULT_BATCHED_REDUCE_SIZE);

            setRandomIndicesOptions(searchRequest::indicesOptions, searchRequest::indicesOptions, expectedParams);

            SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest(searchRequest);

            searchTemplateRequest.setScript("{\"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" }}}");
            searchTemplateRequest.setScriptType(ScriptType.INLINE);
            searchTemplateRequest.setProfile(randomBoolean());

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", "name");
            scriptParams.put("value", randomAlphaOfLengthBetween(2, 5));
            searchTemplateRequest.setScriptParams(scriptParams);

            multiSearchTemplateRequest.add(searchTemplateRequest);
        }

        Map<String, String> expectedParams = new HashMap<>();
        if (randomBoolean()) {
            multiSearchTemplateRequest.maxConcurrentSearchRequests(randomIntBetween(1,10));
            expectedParams.put("max_concurrent_searches", Integer.toString(multiSearchTemplateRequest.maxConcurrentSearchRequests()));
        }
        expectedParams.put(RestSearchAction.TYPED_KEYS_PARAM, "true");

        Request multiRequest = RequestConverters.multiSearchTemplate(multiSearchTemplateRequest);

        assertEquals(HttpPost.METHOD_NAME, multiRequest.getMethod());
        assertEquals("/_msearch/template", multiRequest.getEndpoint());
        List<SearchTemplateRequest> searchRequests = multiSearchTemplateRequest.requests();
        assertEquals(numSearchRequests, searchRequests.size());
        assertEquals(expectedParams, multiRequest.getParameters());

        HttpEntity actualEntity = multiRequest.getEntity();
        byte[] expectedBytes = MultiSearchTemplateRequest.writeMultiLineFormat(multiSearchTemplateRequest, XContentType.JSON.xContent());
        assertEquals(XContentType.JSON.mediaTypeWithoutParameters(), actualEntity.getContentType().getValue());
        assertEquals(new BytesArray(expectedBytes), new BytesArray(EntityUtils.toByteArray(actualEntity)));
    }

    public void testExplain() throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);

        ExplainRequest explainRequest = new ExplainRequest(index, id);
        explainRequest.query(QueryBuilders.termQuery(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10)));

        Map<String, String> expectedParams = new HashMap<>();

        if (randomBoolean()) {
            String routing = randomAlphaOfLengthBetween(3, 10);
            explainRequest.routing(routing);
            expectedParams.put("routing", routing);
        }
        if (randomBoolean()) {
            String preference = randomAlphaOfLengthBetween(3, 10);
            explainRequest.preference(preference);
            expectedParams.put("preference", preference);
        }
        if (randomBoolean()) {
            String[] storedFields = generateRandomStringArray(10, 5, false, false);
            String storedFieldsParams = randomFields(storedFields);
            explainRequest.storedFields(storedFields);
            expectedParams.put("stored_fields", storedFieldsParams);
        }
        if (randomBoolean()) {
            randomizeFetchSourceContextParams(explainRequest::fetchSourceContext, expectedParams);
        }

        Request request = RequestConverters.explain(explainRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/" + index + "/_explain/" + id, request.getEndpoint());

        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(explainRequest, request.getEntity());
    }

    public void testTermVectors() throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);

        TermVectorsRequest tvRequest = new TermVectorsRequest(index, id);
        Map<String, String> expectedParams = new HashMap<>();
        String[] fields;
        if (randomBoolean()) {
            String routing = randomAlphaOfLengthBetween(3, 10);
            tvRequest.setRouting(routing);
            expectedParams.put("routing", routing);
        }
        if (randomBoolean()) {
            tvRequest.setRealtime(false);
            expectedParams.put("realtime", "false");
        }

        boolean hasFields = randomBoolean();
        if (hasFields) {
            fields = generateRandomStringArray(10, 5, false, false);
            tvRequest.setFields(fields);
        }

        Request request = RequestConverters.termVectors(tvRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        endpoint.add(index).add("_termvectors").add(id);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
        for (Map.Entry<String, String> param : expectedParams.entrySet()) {
            assertThat(request.getParameters(), hasEntry(param.getKey(), param.getValue()));
        }
        assertToXContentBody(tvRequest, request.getEntity());
    }

    public void testTermVectorsWithType() throws IOException {
        String index = randomAlphaOfLengthBetween(3, 10);
        String type = randomAlphaOfLengthBetween(3, 10);
        String id = randomAlphaOfLengthBetween(3, 10);
        TermVectorsRequest tvRequest = new TermVectorsRequest(index, type, id);

        Request request = RequestConverters.termVectors(tvRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        endpoint.add(index).add(type).add(id).add("_termvectors");

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
    }

    public void testMultiTermVectors() throws IOException {
        MultiTermVectorsRequest mtvRequest = new MultiTermVectorsRequest();

        int numberOfRequests = randomIntBetween(0, 5);
        for (int i = 0; i < numberOfRequests; i++) {
            String index = randomAlphaOfLengthBetween(3, 10);
            String id = randomAlphaOfLengthBetween(3, 10);
            TermVectorsRequest tvRequest = new TermVectorsRequest(index, id);
            String[] fields = generateRandomStringArray(10, 5, false, false);
            tvRequest.setFields(fields);
            mtvRequest.add(tvRequest);
        }

        Request request = RequestConverters.mtermVectors(mtvRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("_mtermvectors", request.getEndpoint());
        assertToXContentBody(mtvRequest, request.getEntity());
    }

    public void testMultiTermVectorsWithType() throws IOException {
        MultiTermVectorsRequest mtvRequest = new MultiTermVectorsRequest();

        int numberOfRequests = randomIntBetween(0, 5);
        for (int i = 0; i < numberOfRequests; i++) {
            String index = randomAlphaOfLengthBetween(3, 10);
            String type = randomAlphaOfLengthBetween(3, 10);
            String id = randomAlphaOfLengthBetween(3, 10);
            TermVectorsRequest tvRequest = new TermVectorsRequest(index, type, id);
            String[] fields = generateRandomStringArray(10, 5, false, false);
            tvRequest.setFields(fields);
            mtvRequest.add(tvRequest);
        }

        Request request = RequestConverters.mtermVectors(mtvRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("_mtermvectors", request.getEndpoint());
        assertToXContentBody(mtvRequest, request.getEntity());
    }

    public void testFieldCaps() {
        // Create a random request.
        String[] indices = randomIndicesNames(0, 5);
        String[] fields = generateRandomStringArray(5, 10, false, false);

        FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().indices(indices).fields(fields);

        Map<String, String> indicesOptionsParams = new HashMap<>();
        setRandomIndicesOptions(fieldCapabilitiesRequest::indicesOptions, fieldCapabilitiesRequest::indicesOptions, indicesOptionsParams);

        Request request = RequestConverters.fieldCaps(fieldCapabilitiesRequest);

        // Verify that the resulting REST request looks as expected.
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String joinedIndices = String.join(",", indices);
        if (!joinedIndices.isEmpty()) {
            endpoint.add(joinedIndices);
        }
        endpoint.add("_field_caps");

        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(5, request.getParameters().size());

        // Note that we don't check the field param value explicitly, as field names are
        // passed through
        // a hash set before being added to the request, and can appear in a
        // non-deterministic order.
        assertThat(request.getParameters(), hasKey("fields"));
        String[] requestFields = Strings.splitStringByCommaToArray(request.getParameters().get("fields"));
        assertEquals(new HashSet<>(Arrays.asList(fields)), new HashSet<>(Arrays.asList(requestFields)));

        for (Map.Entry<String, String> param : indicesOptionsParams.entrySet()) {
            assertThat(request.getParameters(), hasEntry(param.getKey(), param.getValue()));
        }

        assertNull(request.getEntity());
    }

    public void testRankEval() throws Exception {
        RankEvalSpec spec = new RankEvalSpec(
                Collections.singletonList(new RatedRequest("queryId", Collections.emptyList(), new SearchSourceBuilder())),
                new PrecisionAtK());
        String[] indices = randomIndicesNames(0, 5);
        RankEvalRequest rankEvalRequest = new RankEvalRequest(spec, indices);
        Map<String, String> expectedParams = new HashMap<>();
        setRandomIndicesOptions(rankEvalRequest::indicesOptions, rankEvalRequest::indicesOptions, expectedParams);
        if (randomBoolean()) {
            rankEvalRequest.searchType(randomFrom(SearchType.CURRENTLY_SUPPORTED));
        }
        expectedParams.put("search_type", rankEvalRequest.searchType().name().toLowerCase(Locale.ROOT));

        Request request = RequestConverters.rankEval(rankEvalRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add(RestRankEvalAction.ENDPOINT);
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(5, request.getParameters().size());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(spec, request.getEntity());
    }

    public void testPutScript() throws Exception {
        PutStoredScriptRequest putStoredScriptRequest = new PutStoredScriptRequest();

        String id = randomAlphaOfLengthBetween(5, 10);
        putStoredScriptRequest.id(id);

        XContentType xContentType = randomFrom(XContentType.values());
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            builder.startObject("script")
                .field("lang", "painless")
                .field("source", "Math.log(_score * 2) + params.multiplier")
                .endObject();
            builder.endObject();

            putStoredScriptRequest.content(BytesReference.bytes(builder), xContentType);
        }

        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(putStoredScriptRequest, expectedParams);
        setRandomTimeout(putStoredScriptRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);

        if (randomBoolean()) {
            String context = randomAlphaOfLengthBetween(5, 10);
            putStoredScriptRequest.context(context);
            expectedParams.put("context", context);
        }

        Request request = RequestConverters.putScript(putStoredScriptRequest);

        assertThat(request.getEndpoint(), equalTo("/_scripts/" + id));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertNotNull(request.getEntity());
        assertToXContentBody(putStoredScriptRequest, request.getEntity());
    }

    public void testAnalyzeRequest() throws Exception {
        AnalyzeRequest indexAnalyzeRequest
            = AnalyzeRequest.withIndexAnalyzer("test_index", "test_analyzer", "Here is some text");

        Request request = RequestConverters.analyze(indexAnalyzeRequest);
        assertThat(request.getEndpoint(), equalTo("/test_index/_analyze"));
        assertToXContentBody(indexAnalyzeRequest, request.getEntity());

        AnalyzeRequest analyzeRequest = AnalyzeRequest.withGlobalAnalyzer("test_analyzer", "more text");
        assertThat(RequestConverters.analyze(analyzeRequest).getEndpoint(), equalTo("/_analyze"));
    }

    public void testGetScriptRequest() {
        GetStoredScriptRequest getStoredScriptRequest = new GetStoredScriptRequest("x-script");
        Map<String, String> expectedParams = new HashMap<>();
        setRandomMasterTimeout(getStoredScriptRequest, expectedParams);

        Request request = RequestConverters.getScript(getStoredScriptRequest);
        assertThat(request.getEndpoint(), equalTo("/_scripts/" + getStoredScriptRequest.id()));
        assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
    }

    public void testDeleteScriptRequest() {
        DeleteStoredScriptRequest deleteStoredScriptRequest = new DeleteStoredScriptRequest("x-script");

        Map<String, String> expectedParams = new HashMap<>();
        setRandomTimeout(deleteStoredScriptRequest::timeout, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, expectedParams);
        setRandomMasterTimeout(deleteStoredScriptRequest, expectedParams);

        Request request = RequestConverters.deleteScript(deleteStoredScriptRequest);
        assertThat(request.getEndpoint(), equalTo("/_scripts/" + deleteStoredScriptRequest.id()));
        assertThat(request.getMethod(), equalTo(HttpDelete.METHOD_NAME));
        assertThat(request.getParameters(), equalTo(expectedParams));
        assertThat(request.getEntity(), nullValue());
    }

    static void assertToXContentBody(ToXContent expectedBody, HttpEntity actualEntity) throws IOException {
        BytesReference expectedBytes = XContentHelper.toXContent(expectedBody, REQUEST_BODY_CONTENT_TYPE, false);
        assertEquals(XContentType.JSON.mediaTypeWithoutParameters(), actualEntity.getContentType().getValue());
        assertEquals(expectedBytes, new BytesArray(EntityUtils.toByteArray(actualEntity)));
    }

    public void testEndpointBuilder() {
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder();
            assertEquals("/", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart(Strings.EMPTY_ARRAY);
            assertEquals("/", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("");
            assertEquals("/", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("a", "b");
            assertEquals("/a/b", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("a").addPathPart("b").addPathPartAsIs("_endpoint");
            assertEquals("/a/b/_endpoint", endpointBuilder.build());
        }

        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("a", "b", "c").addPathPartAsIs("_endpoint");
            assertEquals("/a/b/c/_endpoint", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("a").addPathPartAsIs("_endpoint");
            assertEquals("/a/_endpoint", endpointBuilder.build());
        }
    }

    public void testEndpointBuilderEncodeParts() {
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("-#index1,index#2", "type", "id");
            assertEquals("/-%23index1,index%232/type/id", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("index", "type#2", "id");
            assertEquals("/index/type%232/id", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("index", "type", "this/is/the/id");
            assertEquals("/index/type/this%2Fis%2Fthe%2Fid", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("index", "type", "this|is|the|id");
            assertEquals("/index/type/this%7Cis%7Cthe%7Cid", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("index", "type", "id#1");
            assertEquals("/index/type/id%231", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("<logstash-{now/M}>", "_search");
            assertEquals("/%3Clogstash-%7Bnow%2FM%7D%3E/_search", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("中文");
            assertEquals("/中文", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("foo bar");
            assertEquals("/foo%20bar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("foo+bar");
            assertEquals("/foo+bar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("foo+bar");
            assertEquals("/foo+bar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("foo/bar");
            assertEquals("/foo%2Fbar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("foo^bar");
            assertEquals("/foo%5Ebar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("cluster1:index1,index2").addPathPartAsIs("_search");
            assertEquals("/cluster1:index1,index2/_search", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addCommaSeparatedPathParts(new String[] { "index1", "index2" })
                    .addPathPartAsIs("cache/clear");
            assertEquals("/index1,index2/cache/clear", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("/foo");
            assertEquals("/%2Ffoo", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("//foo");
            assertEquals("/%2F%2Ffoo", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("///foo");
            assertEquals("/%2F%2F%2Ffoo", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("/foo/bar");
            assertEquals("/%2Ffoo%2Fbar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("//foo/bar");
            assertEquals("/%2F%2Ffoo%2Fbar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("/foo@bar");
            assertEquals("/%2Ffoo@bar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("//foo@bar");
            assertEquals("/%2F%2Ffoo@bar", endpointBuilder.build());
        }
        {
            EndpointBuilder endpointBuilder = new EndpointBuilder().addPathPart("/part1").addPathPart("//part2").addPathPart("///part3");
            assertEquals("/%2Fpart1/%2F%2Fpart2/%2F%2F%2Fpart3", endpointBuilder.build());
        }
    }

    public void testEndpoint() {
        assertEquals("/index/type/id", RequestConverters.endpoint("index", "type", "id"));
        assertEquals("/index/type/id/_endpoint", RequestConverters.endpoint("index", "type", "id", "_endpoint"));
        assertEquals("/index1,index2", RequestConverters.endpoint(new String[] { "index1", "index2" }));
        assertEquals("/index1,index2/_endpoint", RequestConverters.endpoint(new String[] { "index1", "index2" }, "_endpoint"));
        assertEquals("/index1,index2/type1,type2/_endpoint",
                RequestConverters.endpoint(new String[] { "index1", "index2" }, new String[] { "type1", "type2" }, "_endpoint"));
        assertEquals("/index1,index2/_endpoint/suffix1,suffix2",
                RequestConverters.endpoint(new String[] { "index1", "index2" }, "_endpoint", new String[] { "suffix1", "suffix2" }));
    }

    public void testCreateContentType() {
        final XContentType xContentType = randomFrom(XContentType.values());
        assertEquals(xContentType.mediaTypeWithoutParameters(), RequestConverters.createContentType(xContentType).getMimeType());
    }

    public void testEnforceSameContentType() {
        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
        IndexRequest indexRequest = new IndexRequest().source(singletonMap("field", "value"), xContentType);
        assertEquals(xContentType, enforceSameContentType(indexRequest, null));
        assertEquals(xContentType, enforceSameContentType(indexRequest, xContentType));

        XContentType bulkContentType = randomBoolean() ? xContentType : null;

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> enforceSameContentType(new IndexRequest().source(singletonMap("field", "value"), XContentType.CBOR),
                        bulkContentType));
        assertEquals("Unsupported content-type found for request with content-type [CBOR], only JSON and SMILE are supported",
                exception.getMessage());

        exception = expectThrows(IllegalArgumentException.class,
                () -> enforceSameContentType(new IndexRequest().source(singletonMap("field", "value"), XContentType.YAML),
                        bulkContentType));
        assertEquals("Unsupported content-type found for request with content-type [YAML], only JSON and SMILE are supported",
                exception.getMessage());

        XContentType requestContentType = xContentType == XContentType.JSON ? XContentType.SMILE : XContentType.JSON;

        exception = expectThrows(IllegalArgumentException.class,
                () -> enforceSameContentType(new IndexRequest().source(singletonMap("field", "value"), requestContentType), xContentType));
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
                    expectedParams.put("_source_includes", includesParam);
                }
                int numExcludes = randomIntBetween(0, 5);
                String[] excludes = new String[numExcludes];
                String excludesParam = randomFields(excludes);
                if (numExcludes > 0) {
                    expectedParams.put("_source_excludes", excludesParam);
                }
                consumer.accept(new FetchSourceContext(true, includes, excludes));
            }
        }
    }

    private static void setRandomSearchParams(SearchRequest searchRequest,
                                              Map<String, String> expectedParams) {
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
            searchRequest.searchType(randomFrom(SearchType.CURRENTLY_SUPPORTED));
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
        if (randomBoolean()) {
            searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        }
        expectedParams.put("ccs_minimize_roundtrips", Boolean.toString(searchRequest.isCcsMinimizeRoundtrips()));
        if (randomBoolean()) {
            searchRequest.setMaxConcurrentShardRequests(randomIntBetween(1, Integer.MAX_VALUE));
        }
        expectedParams.put("max_concurrent_shard_requests", Integer.toString(searchRequest.getMaxConcurrentShardRequests()));
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(randomIntBetween(2, Integer.MAX_VALUE));
        }
        if (searchRequest.getPreFilterShardSize() != null) {
            expectedParams.put("pre_filter_shard_size", Integer.toString(searchRequest.getPreFilterShardSize()));
        }
    }

    public static void setRandomIndicesOptions(Consumer<IndicesOptions> setter, Supplier<IndicesOptions> getter,
                                        Map<String, String> expectedParams) {

        if (randomBoolean()) {
            setter.accept(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                true, false, false, randomBoolean()));
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
        expectedParams.put("ignore_throttled", Boolean.toString(getter.get().ignoreThrottled()));
    }

    static IndicesOptions setRandomIndicesOptions(IndicesOptions indicesOptions, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                true, false, false, randomBoolean());
        }
        expectedParams.put("ignore_unavailable", Boolean.toString(indicesOptions.ignoreUnavailable()));
        expectedParams.put("allow_no_indices", Boolean.toString(indicesOptions.allowNoIndices()));
        if (indicesOptions.expandWildcardsOpen() && indicesOptions.expandWildcardsClosed()) {
            expectedParams.put("expand_wildcards", "open,closed");
        } else if (indicesOptions.expandWildcardsOpen()) {
            expectedParams.put("expand_wildcards", "open");
        } else if (indicesOptions.expandWildcardsClosed()) {
            expectedParams.put("expand_wildcards", "closed");
        } else {
            expectedParams.put("expand_wildcards", "none");
        }
        expectedParams.put("ignore_throttled", Boolean.toString(indicesOptions.ignoreThrottled()));
        return indicesOptions;
    }

    static void setRandomIncludeDefaults(Consumer<Boolean> setter, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            boolean includeDefaults = randomBoolean();
            setter.accept(includeDefaults);
            if (includeDefaults) {
                expectedParams.put("include_defaults", String.valueOf(includeDefaults));
            }
        }
    }

    static void setRandomHumanReadable(Consumer<Boolean> setter, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            boolean humanReadable = randomBoolean();
            setter.accept(humanReadable);
            if (humanReadable) {
                expectedParams.put("human", String.valueOf(humanReadable));
            }
        }
    }

    static void setRandomLocal(Consumer<Boolean> setter, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            boolean local = randomBoolean();
            setter.accept(local);
            if (local) {
                expectedParams.put("local", String.valueOf(local));
            }
        }
    }

    static void setRandomTimeout(TimedRequest request, TimeValue defaultTimeout, Map<String, String> expectedParams) {
        setRandomTimeout(s ->
                request.setTimeout(TimeValue.parseTimeValue(s, request.getClass().getName() + ".timeout")),
            defaultTimeout, expectedParams);
    }

    static void setRandomTimeout(Consumer<String> setter, TimeValue defaultTimeout, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            String timeout = randomTimeValue();
            setter.accept(timeout);
            expectedParams.put("timeout", timeout);
        } else {
            expectedParams.put("timeout", defaultTimeout.getStringRep());
        }
    }

    static void setRandomTimeoutTimeValue(Consumer<TimeValue> setter, TimeValue defaultTimeout,
                                                  Map<String, String> expectedParams) {
        if (randomBoolean()) {
            TimeValue timeout = TimeValue.parseTimeValue(randomTimeValue(), "random_timeout");
            setter.accept(timeout);
            expectedParams.put("timeout", timeout.getStringRep());
        } else {
            expectedParams.put("timeout", defaultTimeout.getStringRep());
        }
    }

    static void setRandomMasterTimeout(MasterNodeRequest<?> request, Map<String, String> expectedParams) {
        setRandomMasterTimeout(request::masterNodeTimeout, expectedParams);
    }

    static void setRandomMasterTimeout(TimedRequest request, Map<String, String> expectedParams) {
        setRandomMasterTimeout(s ->
                request.setMasterTimeout(TimeValue.parseTimeValue(s, request.getClass().getName() + ".masterNodeTimeout")),
            expectedParams);
    }

    static void setRandomMasterTimeout(Consumer<String> setter, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            String masterTimeout = randomTimeValue();
            setter.accept(masterTimeout);
            expectedParams.put("master_timeout", masterTimeout);
        } else {
            expectedParams.put("master_timeout", MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT.getStringRep());
        }
    }

    static void setRandomMasterTimeout(Consumer<TimeValue> setter, TimeValue defaultTimeout, Map<String, String> expectedParams) {
        if (randomBoolean()) {
            TimeValue masterTimeout = TimeValue.parseTimeValue(randomTimeValue(), "random_master_timeout");
            setter.accept(masterTimeout);
            expectedParams.put("master_timeout", masterTimeout.getStringRep());
        } else {
            expectedParams.put("master_timeout", defaultTimeout.getStringRep());
        }
    }

    static void setRandomWaitForActiveShards(Consumer<ActiveShardCount> setter, Map<String, String> expectedParams) {
        setRandomWaitForActiveShards(setter, ActiveShardCount.DEFAULT, expectedParams);
    }

    static void setRandomWaitForActiveShards(Consumer<ActiveShardCount> setter, ActiveShardCount defaultActiveShardCount,
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

    static String[] randomIndicesNames(int minIndicesNum, int maxIndicesNum) {
        int numIndices = randomIntBetween(minIndicesNum, maxIndicesNum);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = "index-" + randomAlphaOfLengthBetween(2, 5).toLowerCase(Locale.ROOT);
        }
        return indices;
    }
}
