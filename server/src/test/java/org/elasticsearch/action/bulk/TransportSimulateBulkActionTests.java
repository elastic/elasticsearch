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
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class TransportSimulateBulkActionTests extends ESTestCase {

    /** Services needed by bulk action */
    private TransportService transportService;
    private ClusterService clusterService;
    private TestThreadPool threadPool;

    private TestTransportSimulateBulkAction bulkAction;

    class TestTransportSimulateBulkAction extends TransportSimulateBulkAction {

        volatile boolean failIndexCreation = false;
        boolean indexCreated = false; // set when the "real" index is created
        Runnable beforeIndexCreation = null;

        TestTransportSimulateBulkAction() {
            super(
                TransportSimulateBulkActionTests.this.threadPool,
                transportService,
                clusterService,
                null,
                null,
                new ActionFilters(Collections.emptySet()),
                new TransportBulkActionTookTests.Resolver(),
                new IndexingPressure(Settings.EMPTY),
                EmptySystemIndices.INSTANCE
            );
        }

        @Override
        void createIndex(String index, TimeValue timeout, ActionListener<CreateIndexResponse> listener) {
            indexCreated = true;
            if (beforeIndexCreation != null) {
                beforeIndexCreation.run();
            }
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
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.builder("node")
            .version(
                VersionUtils.randomCompatibleVersion(random(), Version.CURRENT),
                IndexVersions.MINIMUM_COMPATIBLE,
                IndexVersionUtils.randomCompatibleVersion(random())
            )
            .build();
        clusterService = createClusterService(threadPool, discoveryNode);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        bulkAction = new TestTransportSimulateBulkAction();
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }

    public void testIndexData() {
        Task task = mock(Task.class); // unused
        BulkRequest bulkRequest = new SimulateBulkRequest((Map<String, Map<String, Object>>) null);
        int bulkItemCount = randomIntBetween(0, 200);
        for (int i = 0; i < bulkItemCount; i++) {
            Map<String, ?> source = Map.of(randomAlphaOfLength(10), randomAlphaOfLength(5));
            IndexRequest indexRequest = new IndexRequest(randomAlphaOfLength(10)).id(randomAlphaOfLength(10)).source(source);
            for (int j = 0; j < randomIntBetween(0, 10); j++) {
                indexRequest.addPipeline(randomAlphaOfLength(12));
            }
            bulkRequest.add();
        }
        AtomicBoolean onResponseCalled = new AtomicBoolean(false);
        ActionListener<BulkResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse response) {
                onResponseCalled.set(true);
                BulkItemResponse[] responseItems = response.getItems();
                assertThat(responseItems.length, equalTo(bulkRequest.requests().size()));
                for (int i = 0; i < responseItems.length; i++) {
                    BulkItemResponse responseItem = responseItems[i];
                    IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(i);
                    assertNull(responseItem.getFailure());
                    assertThat(responseItem.getResponse(), instanceOf(SimulateIndexResponse.class));
                    SimulateIndexResponse simulateIndexResponse = responseItem.getResponse();
                    assertThat(simulateIndexResponse.getIndex(), equalTo(indexRequest.index()));
                    /*
                     * SimulateIndexResponse doesn't have an equals() method, and most of its state is private. So we check that
                     * its toXContent method produces the expected output.
                     */
                    String output = Strings.toString(simulateIndexResponse);
                    try {
                        assertEquals(
                            XContentHelper.stripWhitespace(
                                Strings.format(
                                    """
                                        {
                                          "_index": "%s",
                                          "_source": %s,
                                          "executed_pipelines": [%s]
                                        }""",
                                    indexRequest.index(),
                                    indexRequest.source(),
                                    indexRequest.getExecutedPipelines()
                                        .stream()
                                        .map(pipeline -> "\"" + pipeline + "\"")
                                        .collect(Collectors.joining(","))
                                )
                            ),
                            output
                        );
                    } catch (IOException e) {
                        fail(e);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail(e, "Unexpected error");
            }
        };
        Set<String> autoCreateIndices = Set.of(); // unused
        Map<String, IndexNotFoundException> indicesThatCannotBeCreated = Map.of(); // unused
        long startTime = 0;
        bulkAction.createMissingIndicesAndIndexData(
            task,
            bulkRequest,
            randomAlphaOfLength(10),
            listener,
            autoCreateIndices,
            indicesThatCannotBeCreated,
            startTime
        );
        assertThat(onResponseCalled.get(), equalTo(true));
    }
}
