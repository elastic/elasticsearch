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

package org.elasticsearch.action.bulk;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.TransportBulkActionTookTests.Resolver;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
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
    private ThreadPool threadPool;

    private TestTransportBulkAction bulkAction;

    class TestTransportBulkAction extends TransportBulkAction {

        boolean indexCreated = false; // set when the "real" index is created

        TestTransportBulkAction() {
            super(TransportBulkActionTests.this.threadPool, transportService, clusterService, null,
                    null, new ActionFilters(Collections.emptySet()), new Resolver(),
                    new AutoCreateIndex(Settings.EMPTY, clusterService.getClusterSettings(), new Resolver()),
                    new IndexingPressure(Settings.EMPTY));
        }

        @Override
        protected boolean needToCheck() {
            return true;
        }

        @Override
        void createIndex(String index, TimeValue timeout, Version minNodeVersion, ActionListener<CreateIndexResponse> listener) {
            indexCreated = true;
            listener.onResponse(null);
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES, VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
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
        BulkItemResponse[] bulkResponses = ((BulkResponse) response).getItems();
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
        assertThat(exception.getMessage(), is("index request targeting data stream [logs-foobar] specifies a custom routing. target the " +
            "backing indices directly or remove the custom routing."));

        // test custom routing is allowed when the index request targets the backing index
        DocWriteRequest<?> writeRequestAgainstIndex =
            new IndexRequest(DataStream.getDefaultBackingIndexName(dataStreamName, 1L)).opType(DocWriteRequest.OpType.INDEX)
            .routing("custom");
        prohibitCustomRoutingOnDataStream(writeRequestAgainstIndex, metadata);
    }
}
