/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.TransportBulkActionTookTests.Resolver;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstraction.Index;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.bulk.TransportBulkAction.prohibitCustomRoutingOnDataStream;
import static org.elasticsearch.cluster.metadata.MetadataCreateDataStreamServiceTests.createDataStream;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransportBulkActionTests extends ESTestCase {

    /** Services needed by bulk action */
    private TransportService transportService;
    private ClusterService clusterService;
    private TestThreadPool threadPool;

    private TestTransportBulkAction bulkAction;

    class TestTransportBulkAction extends TransportBulkAction {

        volatile boolean failIndexCreation = false;
        boolean indexCreated = false; // set when the "real" index is created

        TestTransportBulkAction() {
            super(TransportBulkActionTests.this.threadPool, transportService, clusterService, null,
                    null, new ActionFilters(Collections.emptySet()), new Resolver(),
                    new IndexingPressure(Settings.EMPTY), EmptySystemIndices.INSTANCE);
        }

        @Override
        void createIndex(String index, TimeValue timeout, Version minNodeVersion, ActionListener<CreateIndexResponse> listener) {
            indexCreated = true;
            if (failIndexCreation) {
                listener.onFailure(new ResourceAlreadyExistsException("index already exists"));
            } else {
                listener.onResponse(null);
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
            DiscoveryNodeRole.roles(), VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
        clusterService = createClusterService(threadPool, discoveryNode);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createTransportService(clusterService.getSettings(), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        bulkAction = new TestTransportBulkAction();
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }

    public void testDeleteNonExistingDocDoesNotCreateIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest().add(new DeleteRequest("index").id("id"));

        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        ActionTestUtils.execute(bulkAction, null, bulkRequest, future);

        BulkResponse response = future.actionGet();
        assertFalse(bulkAction.indexCreated);
        BulkItemResponse[] bulkResponses = response.getItems();
        assertEquals(bulkResponses.length, 1);
        assertTrue(bulkResponses[0].isFailed());
        assertTrue(bulkResponses[0].getFailure().getCause() instanceof IndexNotFoundException);
        assertEquals("index", bulkResponses[0].getFailure().getIndex());
    }

    public void testDeleteNonExistingDocExternalVersionCreatesIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest()
                .add(new DeleteRequest("index").id("id").versionType(VersionType.EXTERNAL).version(0));

        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        ActionTestUtils.execute(bulkAction, null, bulkRequest, future);
        future.actionGet();
        assertTrue(bulkAction.indexCreated);
    }

    public void testDeleteNonExistingDocExternalGteVersionCreatesIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest()
                .add(new DeleteRequest("index2").id("id").versionType(VersionType.EXTERNAL_GTE).version(0));

        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        ActionTestUtils.execute(bulkAction, null, bulkRequest, future);
        future.actionGet();
        assertTrue(bulkAction.indexCreated);
    }

    public void testGetIndexWriteRequest() throws Exception {
        IndexRequest indexRequest = new IndexRequest("index").id("id1").source(Collections.emptyMap());
        UpdateRequest upsertRequest = new UpdateRequest("index", "id1").upsert(indexRequest).script(mockScript("1"));
        UpdateRequest docAsUpsertRequest = new UpdateRequest("index", "id2").doc(indexRequest).docAsUpsert(true);
        UpdateRequest scriptedUpsert = new UpdateRequest("index", "id2").upsert(indexRequest).script(mockScript("1"))
            .scriptedUpsert(true);

        assertEquals(TransportBulkAction.getIndexWriteRequest(indexRequest), indexRequest);
        assertEquals(TransportBulkAction.getIndexWriteRequest(upsertRequest), indexRequest);
        assertEquals(TransportBulkAction.getIndexWriteRequest(docAsUpsertRequest), indexRequest);
        assertEquals(TransportBulkAction.getIndexWriteRequest(scriptedUpsert), indexRequest);

        DeleteRequest deleteRequest = new DeleteRequest("index", "id");
        assertNull(TransportBulkAction.getIndexWriteRequest(deleteRequest));

        UpdateRequest badUpsertRequest = new UpdateRequest("index", "id1");
        assertNull(TransportBulkAction.getIndexWriteRequest(badUpsertRequest));
    }

    public void testProhibitAppendWritesInBackingIndices() throws Exception {
        String dataStreamName = "logs-foobar";
        ClusterState clusterState = createDataStream(dataStreamName);
        Metadata metadata = clusterState.metadata();

        // Testing create op against backing index fails:
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndexRequest invalidRequest1 = new IndexRequest(backingIndexName).opType(DocWriteRequest.OpType.CREATE);
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> TransportBulkAction.prohibitAppendWritesInBackingIndices(invalidRequest1, metadata));
        assertThat(e.getMessage(), equalTo("index request with op_type=create targeting backing indices is disallowed, " +
            "target corresponding data stream [logs-foobar] instead"));

        // Testing index op against backing index fails:
        IndexRequest invalidRequest2 = new IndexRequest(backingIndexName).opType(DocWriteRequest.OpType.INDEX);
        e = expectThrows(IllegalArgumentException.class,
            () -> TransportBulkAction.prohibitAppendWritesInBackingIndices(invalidRequest2, metadata));
        assertThat(e.getMessage(), equalTo("index request with op_type=index and no if_primary_term and if_seq_no set " +
            "targeting backing indices is disallowed, target corresponding data stream [logs-foobar] instead"));

        // Testing valid writes ops against a backing index:
        DocWriteRequest<?> validRequest = new IndexRequest(backingIndexName).opType(DocWriteRequest.OpType.INDEX)
            .setIfSeqNo(1).setIfPrimaryTerm(1);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata);
        validRequest = new DeleteRequest(backingIndexName);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata);
        validRequest = new UpdateRequest(backingIndexName, "_id");
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata);

        // Testing append only write via ds name
        validRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata);

        validRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.INDEX);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata);

        // Append only for a backing index that doesn't exist is allowed:
        validRequest = new IndexRequest(DataStream.getDefaultBackingIndexName("logs-barbaz", 1))
            .opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata);

        // Some other index names:
        validRequest = new IndexRequest("my-index").opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata);
        validRequest = new IndexRequest("foobar").opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata);
    }

    public void testProhibitCustomRoutingOnDataStream() throws Exception {
        String dataStreamName = "logs-foobar";
        ClusterState clusterState = createDataStream(dataStreamName);
        Metadata metadata = clusterState.metadata();

        // custom routing requests against the data stream are prohibited
        DocWriteRequest<?> writeRequestAgainstDataStream = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.INDEX)
            .routing("custom");
        IllegalArgumentException exception =
            expectThrows(IllegalArgumentException.class, () -> prohibitCustomRoutingOnDataStream(writeRequestAgainstDataStream, metadata));
        assertThat(exception.getMessage(), is(
            "index request targeting data stream [logs-foobar] specifies a custom routing. "
                + "allow_custom_routing within data_stream field must be true when custom routing is enabled."));

        // test custom routing is allowed when the index request targets the backing index
        DocWriteRequest<?> writeRequestAgainstIndex =
            new IndexRequest(DataStream.getDefaultBackingIndexName(dataStreamName, 1L)).opType(DocWriteRequest.OpType.INDEX)
            .routing("custom");
        prohibitCustomRoutingOnDataStream(writeRequestAgainstIndex, metadata);
    }

    public void testOnlySystem() {
        SortedMap<String, IndexAbstraction> indicesLookup = new TreeMap<>();
        Settings settings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        indicesLookup.put(".foo",
            new Index(IndexMetadata.builder(".foo").settings(settings).system(true).numberOfShards(1).numberOfReplicas(0).build()));
        indicesLookup.put(".bar",
            new Index(IndexMetadata.builder(".bar").settings(settings).system(true).numberOfShards(1).numberOfReplicas(0).build()));
        SystemIndices systemIndices = new SystemIndices(
            Map.of("plugin", new SystemIndices.Feature("plugin", "test feature", List.of(new SystemIndexDescriptor(".test", "")))));
        List<String> onlySystem = List.of(".foo", ".bar");
        assertTrue(bulkAction.isOnlySystem(buildBulkRequest(onlySystem), indicesLookup, systemIndices));

        onlySystem = List.of(".foo", ".bar", ".test");
        assertTrue(bulkAction.isOnlySystem(buildBulkRequest(onlySystem), indicesLookup, systemIndices));

        List<String> nonSystem = List.of("foo", "bar");
        assertFalse(bulkAction.isOnlySystem(buildBulkRequest(nonSystem), indicesLookup, systemIndices));

        List<String> mixed = List.of(".foo", ".test", "other");
        assertFalse(bulkAction.isOnlySystem(buildBulkRequest(mixed), indicesLookup, systemIndices));
    }

    public void testRejectionAfterCreateIndexIsPropagated() throws Exception {
        BulkRequest bulkRequest = new BulkRequest().add(new IndexRequest("index").id("id").source(Collections.emptyMap()));
        bulkAction.failIndexCreation = randomBoolean();

        try {
            threadPool.startForcingRejections();
            PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
            ActionTestUtils.execute(bulkAction, null, bulkRequest, future);
            expectThrows(EsRejectedExecutionException.class, future::actionGet);
        } finally {
            threadPool.stopForcingRejections();
        }
    }

    private BulkRequest buildBulkRequest(List<String> indices) {
        BulkRequest request = new BulkRequest();
        for (String index : indices) {
            final DocWriteRequest<?> subRequest;
            switch (randomIntBetween(1, 3)) {
                case 1:
                    subRequest = new IndexRequest(index);
                    break;
                case 2:
                    subRequest = new DeleteRequest(index).id("0");
                    break;
                case 3:
                    subRequest = new UpdateRequest(index, "0");
                    break;
                default:
                    throw new IllegalStateException("only have 3 cases");
            }
            request.add(subRequest);
        }
        return request;
    }
}
