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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.TransportBulkActionTookTests.Resolver;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;

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
                    new AutoCreateIndex(Settings.EMPTY, clusterService.getClusterSettings(), new Resolver()));
        }

        @Override
        protected boolean needToCheck() {
            return true;
        }

        @Override
        void createIndex(String index, TimeValue timeout, ActionListener<CreateIndexResponse> listener) {
            indexCreated = true;
            listener.onResponse(null);
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("TransportBulkActionTookTests");
        clusterService = createClusterService(threadPool);
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
        BulkRequest bulkRequest = new BulkRequest().add(new DeleteRequest("index", "type", "id"));

        ActionTestUtils.execute(bulkAction, null, bulkRequest, ActionListener.wrap(response -> {
            assertFalse(bulkAction.indexCreated);
            BulkItemResponse[] bulkResponses = ((BulkResponse) response).getItems();
            assertEquals(bulkResponses.length, 1);
            assertTrue(bulkResponses[0].isFailed());
            assertTrue(bulkResponses[0].getFailure().getCause() instanceof IndexNotFoundException);
            assertEquals("index", bulkResponses[0].getFailure().getIndex());
        }, exception -> {
            throw new AssertionError(exception);
        }));
    }

    public void testDeleteNonExistingDocExternalVersionCreatesIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest()
                .add(new DeleteRequest("index", "type", "id").versionType(VersionType.EXTERNAL).version(0));

        ActionTestUtils.execute(bulkAction, null, bulkRequest, ActionListener.wrap(response -> {
            assertTrue(bulkAction.indexCreated);
        }, exception -> {
            throw new AssertionError(exception);
        }));
    }

    public void testDeleteNonExistingDocExternalGteVersionCreatesIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest()
                .add(new DeleteRequest("index2", "type", "id").versionType(VersionType.EXTERNAL_GTE).version(0));

        ActionTestUtils.execute(bulkAction, null, bulkRequest, ActionListener.wrap(response -> {
            assertTrue(bulkAction.indexCreated);
        }, exception -> {
            throw new AssertionError(exception);
        }));
    }

    public void testGetIndexWriteRequest() throws Exception {
        IndexRequest indexRequest = new IndexRequest("index", "type", "id1").source(Collections.emptyMap());
        UpdateRequest upsertRequest = new UpdateRequest("index", "type", "id1").upsert(indexRequest).script(mockScript("1"));
        UpdateRequest docAsUpsertRequest = new UpdateRequest("index", "type", "id2").doc(indexRequest).docAsUpsert(true);
        UpdateRequest scriptedUpsert = new UpdateRequest("index", "type", "id2").upsert(indexRequest).script(mockScript("1"))
            .scriptedUpsert(true);

        assertEquals(TransportBulkAction.getIndexWriteRequest(indexRequest), indexRequest);
        assertEquals(TransportBulkAction.getIndexWriteRequest(upsertRequest), indexRequest);
        assertEquals(TransportBulkAction.getIndexWriteRequest(docAsUpsertRequest), indexRequest);
        assertEquals(TransportBulkAction.getIndexWriteRequest(scriptedUpsert), indexRequest);

        DeleteRequest deleteRequest = new DeleteRequest("index", "id");
        assertNull(TransportBulkAction.getIndexWriteRequest(deleteRequest));

        UpdateRequest badUpsertRequest = new UpdateRequest("index", "type", "id1");
        assertNull(TransportBulkAction.getIndexWriteRequest(badUpsertRequest));
    }
}
