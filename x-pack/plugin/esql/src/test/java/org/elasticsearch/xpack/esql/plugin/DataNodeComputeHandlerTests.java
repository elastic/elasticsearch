/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeFailure;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class DataNodeComputeHandlerTests extends ESTestCase {

    /**
     * Documents that leave a data node name a context the fetch phase finds again. A request without fetch contexts
     * would name contexts that close with the request.
     */
    public void testDocumentsLeaveTheirNodeOnlyWithFetchContexts() {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        ExchangeSourceExec source = new ExchangeSourceExec(Source.EMPTY, List.of(doc), false);
        ExchangeSinkExec plan = new ExchangeSinkExec(Source.EMPTY, source.output(), false, source);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DataNodeComputeHandler.validateFetchContexts(plan, request(plan, null))
        );
        assertThat(e.getMessage(), equalTo("documents can only leave their node for a fetch, but the request kept no fetch contexts"));
        DataNodeComputeHandler.validateFetchContexts(plan, request(plan, randomPositiveTimeValue()));
    }

    /**
     * A node keeps no fetch context longer than its {@code search.max_keep_alive}, also when the coordinator asks for a
     * longer keep-alive.
     */
    public void testTheReaderContextKeepAliveStaysWithinTheMaxKeepAliveOfTheNode() {
        TimeValue maxKeepAlive = TimeValue.timeValueMinutes(between(1, 60));
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(SearchService.MAX_KEEPALIVE_SETTING.getKey(), maxKeepAlive).build(),
            Set.of(SearchService.MAX_KEEPALIVE_SETTING)
        );
        ExchangeSinkExec plan = new ExchangeSinkExec(
            Source.EMPTY,
            List.of(),
            false,
            new ExchangeSourceExec(Source.EMPTY, List.of(), false)
        );

        TimeValue shorter = TimeValue.timeValueMillis(between(1, (int) maxKeepAlive.millis()));
        assertThat(DataNodeComputeHandler.readerContextKeepAlive(request(plan, shorter), clusterSettings), equalTo(shorter));
        TimeValue longer = TimeValue.timeValueMillis(maxKeepAlive.millis() + between(1, 3_600_000));
        assertThat(DataNodeComputeHandler.readerContextKeepAlive(request(plan, longer), clusterSettings), equalTo(maxKeepAlive));
    }

    /**
     * A node that returned a page, or that was sent an INTO, may hold results of the query. A node that was sent a read and
     * returned no page does not, and neither does one whose INTO was never sent.
     */
    public void testMayHaveReceivedData() {
        for (boolean writesInto : List.of(true, false)) {
            for (boolean requestSent : List.of(true, false)) {
                assertTrue(DataNodeComputeHandler.mayHaveReceivedData(between(1, 100), writesInto, requestSent));
            }
        }
        assertFalse(DataNodeComputeHandler.mayHaveReceivedData(0, false, false));
        assertFalse(DataNodeComputeHandler.mayHaveReceivedData(0, false, true));
        assertFalse(DataNodeComputeHandler.mayHaveReceivedData(0, true, false));
        assertTrue("an INTO may have written rows without returning a page", DataNodeComputeHandler.mayHaveReceivedData(0, true, true));
    }

    /**
     * The listener of a data node tells the sender what the node may hold as of the time it fails, not as of the time the listener
     * was built: a failure after the request of an INTO was sent must not send the INTO to another copy of the shards, also when
     * the failure says that the node was unreachable, which would otherwise have the shards resolved again.
     */
    public void testNodeRequestListenerTellsTheSenderWhatTheNodeMayHold() {
        assertNodeRequestListenerTellsTheSenderWhatTheNodeMayHold(true);
        assertNodeRequestListenerTellsTheSenderWhatTheNodeMayHold(false);
    }

    private static void assertNodeRequestListenerTellsTheSenderWhatTheNodeMayHold(boolean writesInto) {
        AtomicLong pagesFetched = new AtomicLong();
        AtomicBoolean requestSent = new AtomicBoolean();
        AtomicReference<NodeFailure> computeNodeFailure = new AtomicReference<>();
        AtomicReference<ReportedFailure> reported = new AtomicReference<>();
        ActionListener<DataNodeComputeResponse> listener = DataNodeComputeHandler.nodeRequestListener(
            recordFailure(reported),
            pagesFetched,
            writesInto,
            requestSent,
            computeNodeFailure
        );
        DiscoveryNode node = DiscoveryNodeUtils.create("data-node");

        // the open failed: nothing was sent, and the failure is classified as the transport delivered it
        Exception openFailure = new NodeDisconnectedException(node, "open_exchange");
        listener.onFailure(openFailure);
        assertThat(reported.get(), equalTo(new ReportedFailure(openFailure, false, NodeFailure.UNREACHABLE)));

        // the data node request was sent and lost: only an INTO may have left something behind
        computeNodeFailure.set(NodeFailure.UNREACHABLE);
        requestSent.set(true);
        Exception computeFailure = new IOException("picked by the compute listener");
        listener.onFailure(computeFailure);
        assertThat(reported.get(), equalTo(new ReportedFailure(computeFailure, writesInto, NodeFailure.UNREACHABLE)));

        // a page arrived
        pagesFetched.incrementAndGet();
        computeNodeFailure.set(NodeFailure.OTHER);
        listener.onFailure(computeFailure);
        assertThat(reported.get(), equalTo(new ReportedFailure(computeFailure, true, NodeFailure.OTHER)));
    }

    /**
     * A data node request counts as sent from before it leaves, so that a failure of it, however early it comes back, finds the
     * node marked: the connection that drops under a sent INTO must not have the INTO sent to another copy of the shards.
     */
    public void testDataNodeRequestIsMarkedSentBeforeItLeaves() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            assertDataNodeRequestIsMarkedSentBeforeItLeaves(threadPool, true);
            assertDataNodeRequestIsMarkedSentBeforeItLeaves(threadPool, false);
        } finally {
            terminate(threadPool);
        }
    }

    private static void assertDataNodeRequestIsMarkedSentBeforeItLeaves(ThreadPool threadPool, boolean writesInto) {
        boolean allowPartialResults = randomBoolean();
        AtomicBoolean requestSent = new AtomicBoolean();
        AtomicReference<Boolean> sentWhenLeaving = new AtomicReference<>();
        AtomicLong sentRequestId = new AtomicLong(-1);
        // Stands for the wire: the request goes nowhere, and the test answers it. This shows the mark at the very moment the
        // request leaves, which a real transport does not expose.
        MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                assertThat(action, equalTo(ComputeService.DATA_ACTION_NAME));
                sentWhenLeaving.set(requestSent.get());
                sentRequestId.set(requestId);
            }
        };
        DiscoveryNode dataNode = DiscoveryNodeUtils.create("data-node");
        TransportService transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> DiscoveryNodeUtils.create("coordinator"),
            null,
            Set.of()
        );
        try (transportService) {
            transportService.start();
            transportService.acceptIncomingRequests();

            AtomicReference<NodeFailure> computeNodeFailure = new AtomicReference<>(NodeFailure.OTHER);
            AtomicReference<ReportedFailure> reported = new AtomicReference<>();
            ActionListener<DataNodeComputeResponse> listener = DataNodeComputeHandler.nodeRequestListener(
                recordFailure(reported),
                new AtomicLong(),
                writesInto,
                requestSent,
                computeNodeFailure
            );
            ExchangeSinkExec plan = new ExchangeSinkExec(
                Source.EMPTY,
                List.of(),
                false,
                new ExchangeSourceExec(Source.EMPTY, List.of(), false)
            );
            DataNodeComputeHandler.sendDataNodeRequest(
                transportService,
                transportService.getConnection(dataNode),
                request(plan, null),
                new Task(randomNonNegativeLong(), "type", "action", "", TaskId.EMPTY_TASK_ID, Map.of()),
                TransportRequestOptions.EMPTY,
                requestSent,
                new ActionListenerResponseHandler<>(
                    DataNodeComputeHandler.classifyingDataRequestFailure(listener, computeNodeFailure, allowPartialResults),
                    in -> {
                        throw new AssertionError("no response is expected here");
                    },
                    EsExecutors.DIRECT_EXECUTOR_SERVICE
                )
            );
            assertThat("the request left unmarked", sentWhenLeaving.get(), equalTo(true));

            NodeDisconnectedException disconnected = new NodeDisconnectedException(dataNode, ComputeService.DATA_ACTION_NAME);
            transport.handleError(sentRequestId.get(), disconnected);
            // without partial results the failure has cancelled the query, and is not read as one of the node
            NodeFailure expected = allowPartialResults ? NodeFailure.UNREACHABLE : NodeFailure.OTHER;
            assertThat(reported.get(), equalTo(new ReportedFailure(disconnected, writesInto, expected)));
        }
    }

    private record ReportedFailure(Exception failure, boolean receivedData, NodeFailure nodeFailure) {}

    private static NodeListener recordFailure(AtomicReference<ReportedFailure> reported) {
        return new NodeListener() {
            @Override
            public void onResponse(DataNodeComputeResponse response) {
                throw new AssertionError("no response is expected here");
            }

            @Override
            public void onFailure(Exception e, boolean receivedData, NodeFailure nodeFailure) {
                reported.set(new ReportedFailure(e, receivedData, nodeFailure));
            }

            @Override
            public void onSkip() {
                throw new AssertionError("no skip is expected here");
            }
        };
    }

    private static DataNodeRequest request(ExchangeSinkExec plan, @Nullable TimeValue readerContextKeepAlive) {
        return new DataNodeRequest(
            "session",
            TaskId.EMPTY_TASK_ID,
            EsqlTestUtils.TEST_CFG,
            "",
            List.of(),
            Map.of(),
            plan,
            new String[0],
            IndicesOptions.STRICT_EXPAND_OPEN,
            true,
            true,
            false,
            List.of(),
            0L,
            readerContextKeepAlive
        );
    }
}
