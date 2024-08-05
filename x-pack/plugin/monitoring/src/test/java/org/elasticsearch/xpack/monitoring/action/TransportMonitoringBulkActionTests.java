/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkDoc;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkRequest;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkResponse;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.MonitoringService;
import org.elasticsearch.xpack.monitoring.MonitoringTestUtils;
import org.elasticsearch.xpack.monitoring.exporter.BytesReferenceMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TransportMonitoringBulkAction}
 */
public class TransportMonitoringBulkActionTests extends ESTestCase {

    private ActionListener<MonitoringBulkResponse> listener;
    private Exporters exporters;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private ActionFilters filters;
    private final MonitoringService monitoringService = mock(MonitoringService.class);

    @Before
    @SuppressWarnings("unchecked")
    public void setUpMocks() {
        final ExecutorService executor = mock(ExecutorService.class);
        final TaskManager taskManager = mock(TaskManager.class);

        listener = mock(ActionListener.class);
        exporters = mock(Exporters.class);
        threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);
        transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        filters = mock(ActionFilters.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);
        when(taskManager.register(anyString(), eq(MonitoringBulkAction.NAME), any(TaskAwareRequest.class))).thenReturn(mock(Task.class));
        when(filters.filters()).thenReturn(new ActionFilter[0]);
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executor);

        // execute in the same thread
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
    }

    public void testExecuteWithGlobalBlock() throws Exception {
        final ClusterBlocks.Builder clusterBlock = ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ALL);
        when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).blocks(clusterBlock).build());

        final TransportMonitoringBulkAction action = new TransportMonitoringBulkAction(
            threadPool,
            clusterService,
            transportService,
            filters,
            exporters,
            monitoringService
        );

        final MonitoringBulkRequest request = randomRequest();

        assertThat(
            asInstanceOf(
                ClusterBlockException.class,
                safeAwaitFailure(MonitoringBulkResponse.class, l -> action.execute(null, request, l))
            ),
            hasToString(containsString("ClusterBlockException: blocked by: [SERVICE_UNAVAILABLE/2/no master]"))
        );
    }

    public void testExecuteIgnoresRequestWhenCollectionIsDisabled() throws Exception {
        when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).build());
        when(monitoringService.isMonitoringActive()).thenReturn(false);

        final TransportMonitoringBulkAction action = new TransportMonitoringBulkAction(
            threadPool,
            clusterService,
            transportService,
            filters,
            exporters,
            monitoringService
        );

        final MonitoringBulkDoc doc = mock(MonitoringBulkDoc.class);
        when(doc.getSource()).thenReturn(new BytesArray("test"));

        final MonitoringBulkRequest request = new MonitoringBulkRequest();
        request.add(doc);

        final MonitoringBulkResponse response = ActionTestUtils.executeBlocking(action, request);

        assertThat(response.status(), is(RestStatus.OK));
        assertThat(response.isIgnored(), is(true));
        assertThat(response.getTookInMillis(), is(0L));
        assertThat(response.getError(), nullValue());
    }

    public void testExecuteEmptyRequest() {
        // it validates the request before it tries to execute it
        when(monitoringService.isMonitoringActive()).thenReturn(randomBoolean());

        final TransportMonitoringBulkAction action = new TransportMonitoringBulkAction(
            threadPool,
            clusterService,
            transportService,
            filters,
            exporters,
            monitoringService
        );

        assertThat(
            asInstanceOf(
                ActionRequestValidationException.class,
                safeAwaitFailure(MonitoringBulkResponse.class, l -> action.execute(null, new MonitoringBulkRequest(), l))
            ),
            hasToString(containsString("no monitoring documents added"))
        );
    }

    @SuppressWarnings("unchecked")
    public void testExecuteRequest() {
        when(monitoringService.isMonitoringActive()).thenReturn(true);

        final DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("_id", new TransportAddress(TransportAddress.META_ADDRESS, 9300));
        when(clusterService.localNode()).thenReturn(discoveryNode);

        final String clusterUUID = UUIDs.randomBase64UUID();
        when(clusterService.state()).thenReturn(
            ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().clusterUUID(clusterUUID).build()).build()
        );

        final MonitoringBulkRequest request = new MonitoringBulkRequest();

        final MonitoredSystem system = randomFrom(MonitoredSystem.KIBANA, MonitoredSystem.LOGSTASH, MonitoredSystem.BEATS);
        final String type = randomAlphaOfLength(5);
        final String id = randomBoolean() ? randomAlphaOfLength(5) : null;
        final long timestamp = randomNonNegativeLong();
        final long interval = randomNonNegativeLong();
        final XContentType xContentType = randomFrom(XContentType.values());

        final int nbDocs = randomIntBetween(1, 50);
        for (int i = 0; i < nbDocs; i++) {
            MonitoringBulkDoc mockBulkDoc = mock(MonitoringBulkDoc.class);
            request.add(mockBulkDoc);

            when(mockBulkDoc.getSystem()).thenReturn(system);
            when(mockBulkDoc.getType()).thenReturn(type);
            when(mockBulkDoc.getId()).thenReturn(id + String.valueOf(i));
            when(mockBulkDoc.getTimestamp()).thenReturn(timestamp);
            when(mockBulkDoc.getIntervalMillis()).thenReturn(interval);
            when(mockBulkDoc.getSource()).thenReturn(RandomObjects.randomSource(random(), xContentType));
            when(mockBulkDoc.getXContentType()).thenReturn(xContentType);
        }

        doAnswer((i) -> {
            final Collection<MonitoringDoc> exportedDocs = (Collection<MonitoringDoc>) i.getArguments()[0];
            assertEquals(nbDocs, exportedDocs.size());
            exportedDocs.forEach(exportedDoc -> {
                assertThat(exportedDoc, instanceOf(BytesReferenceMonitoringDoc.class));
                assertThat(exportedDoc.getSystem(), equalTo(system));
                assertThat(exportedDoc.getType(), equalTo(type));
                assertThat(exportedDoc.getId(), startsWith(id));
                assertThat(exportedDoc.getTimestamp(), equalTo(timestamp));
                assertThat(exportedDoc.getIntervalMillis(), equalTo(interval));
                assertThat(exportedDoc.getNode().getUUID(), equalTo(discoveryNode.getId()));
                assertThat(exportedDoc.getCluster(), equalTo(clusterUUID));
            });

            final ActionListener<?> actionListener = (ActionListener<?>) i.getArguments()[1];
            actionListener.onResponse(null);
            return Void.TYPE;
        }).when(exporters).export(any(Collection.class), any(ActionListener.class));

        final TransportMonitoringBulkAction action = new TransportMonitoringBulkAction(
            threadPool,
            clusterService,
            transportService,
            filters,
            exporters,
            monitoringService
        );
        ActionTestUtils.executeBlocking(action, request);

        verify(threadPool).executor(ThreadPool.Names.GENERIC);
        verify(exporters).export(any(Collection.class), any(ActionListener.class));
        verify(clusterService, times(2)).state();
        verify(clusterService).localNode();
    }

    public void testAsyncActionCreateMonitoringDocsWithNoDocs() {
        final Collection<MonitoringBulkDoc> bulkDocs = new ArrayList<>();
        if (randomBoolean()) {
            final int nbDocs = randomIntBetween(1, 50);
            for (int i = 0; i < nbDocs; i++) {
                MonitoringBulkDoc mockBulkDoc = mock(MonitoringBulkDoc.class);
                bulkDocs.add(mockBulkDoc);
                when(mockBulkDoc.getSystem()).thenReturn(MonitoredSystem.UNKNOWN);
            }
        }

        final Collection<MonitoringDoc> results = new TransportMonitoringBulkAction.AsyncAction(
            threadPool,
            null,
            null,
            null,
            null,
            0L,
            null
        ).createMonitoringDocs(bulkDocs);

        assertThat(results, notNullValue());
        assertThat(results.size(), equalTo(0));
    }

    public void testAsyncActionCreateMonitoringDocs() {
        final List<MonitoringBulkDoc> docs = new ArrayList<>();

        final MonitoredSystem system = randomFrom(MonitoredSystem.KIBANA, MonitoredSystem.LOGSTASH, MonitoredSystem.BEATS);
        final String type = randomAlphaOfLength(5);
        final String id = randomBoolean() ? randomAlphaOfLength(5) : null;
        final long timestamp = randomBoolean() ? randomNonNegativeLong() : 0L;
        final long interval = randomNonNegativeLong();
        final XContentType xContentType = randomFrom(XContentType.values());
        final MonitoringDoc.Node node = MonitoringTestUtils.randomMonitoringNode(random());

        final int nbDocs = randomIntBetween(1, 50);
        for (int i = 0; i < nbDocs; i++) {
            MonitoringBulkDoc mockBulkDoc = mock(MonitoringBulkDoc.class);
            docs.add(mockBulkDoc);

            when(mockBulkDoc.getSystem()).thenReturn(system);
            when(mockBulkDoc.getType()).thenReturn(type);
            when(mockBulkDoc.getId()).thenReturn(id);
            when(mockBulkDoc.getTimestamp()).thenReturn(timestamp);
            when(mockBulkDoc.getIntervalMillis()).thenReturn(interval);
            when(mockBulkDoc.getSource()).thenReturn(BytesArray.EMPTY);
            when(mockBulkDoc.getXContentType()).thenReturn(xContentType);
        }

        final Collection<MonitoringDoc> exportedDocs = new TransportMonitoringBulkAction.AsyncAction(
            threadPool,
            null,
            null,
            null,
            "_cluster",
            123L,
            node
        ).createMonitoringDocs(docs);

        assertThat(exportedDocs, notNullValue());
        assertThat(exportedDocs.size(), equalTo(nbDocs));
        exportedDocs.forEach(exportedDoc -> {
            assertThat(exportedDoc.getSystem(), equalTo(system));
            assertThat(exportedDoc.getType(), equalTo(type));
            assertThat(exportedDoc.getId(), equalTo(id));
            assertThat(exportedDoc.getTimestamp(), equalTo(timestamp != 0L ? timestamp : 123L));
            assertThat(exportedDoc.getIntervalMillis(), equalTo(interval));
            assertThat(exportedDoc.getNode(), equalTo(node));
            assertThat(exportedDoc.getCluster(), equalTo("_cluster"));
        });
    }

    public void testAsyncActionCreateMonitoringDocWithNoTimestamp() {
        final MonitoringBulkDoc monitoringBulkDoc = new MonitoringBulkDoc(
            MonitoredSystem.LOGSTASH,
            "_type",
            "_id",
            0L,
            0L,
            BytesArray.EMPTY,
            XContentType.JSON
        );
        final MonitoringDoc monitoringDoc = new TransportMonitoringBulkAction.AsyncAction(threadPool, null, null, null, "", 456L, null)
            .createMonitoringDoc(monitoringBulkDoc);

        assertThat(monitoringDoc.getTimestamp(), equalTo(456L));
    }

    public void testAsyncActionCreateMonitoringDoc() throws Exception {
        final MonitoringDoc.Node node = new MonitoringDoc.Node("_uuid", "_host", "_addr", "_ip", "_name", 1504169190855L);

        final XContentType xContentType = randomFrom(XContentType.values());
        final BytesReference source = BytesReference.bytes(
            XContentBuilder.builder(xContentType.xContent()).startObject().startObject("_foo").field("_bar", "_baz").endObject().endObject()
        );

        final MonitoringBulkDoc monitoringBulkDoc = new MonitoringBulkDoc(
            MonitoredSystem.LOGSTASH,
            "_type",
            "_id",
            1502107402133L,
            15_000L,
            source,
            xContentType
        );

        final MonitoringDoc monitoringDoc = new TransportMonitoringBulkAction.AsyncAction(
            threadPool,
            null,
            null,
            null,
            "_cluster_uuid",
            3L,
            node
        ).createMonitoringDoc(monitoringBulkDoc);

        final BytesReference xContent = XContentHelper.toXContent(monitoringDoc, XContentType.JSON, randomBoolean());
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "cluster_uuid": "_cluster_uuid",
              "timestamp": "2017-08-07T12:03:22.133Z",
              "interval_ms": 15000,
              "type": "_type",
              "source_node": {
                "uuid": "_uuid",
                "host": "_host",
                "transport_address": "_addr",
                "ip": "_ip",
                "name": "_name",
                "timestamp": "2017-08-31T08:46:30.855Z"
              },
              "_type": {
                "_foo": {
                  "_bar": "_baz"
                }
              }
            }"""), xContent.utf8ToString());
    }

    @SuppressWarnings("unchecked")
    public void testAsyncActionExecuteExport() {
        final int nbDocs = randomIntBetween(1, 25);
        final Collection<MonitoringDoc> docs = new ArrayList<>(nbDocs);
        for (int i = 0; i < nbDocs; i++) {
            docs.add(mock(MonitoringDoc.class));
        }

        doAnswer((i) -> {
            final Collection<MonitoringDoc> exportedDocs = (Collection<MonitoringDoc>) i.getArguments()[0];
            assertThat(exportedDocs, is(docs));

            final ActionListener<?> actionListener = (ActionListener<?>) i.getArguments()[1];
            actionListener.onResponse(null);
            return Void.TYPE;
        }).when(exporters).export(any(Collection.class), any(ActionListener.class));

        final TransportMonitoringBulkAction.AsyncAction asyncAction = new TransportMonitoringBulkAction.AsyncAction(
            threadPool,
            null,
            null,
            exporters,
            null,
            0L,
            null
        );

        asyncAction.executeExport(docs, randomNonNegativeLong(), listener);

        verify(threadPool).executor(ThreadPool.Names.GENERIC);
        verify(exporters).export(eq(docs), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    public void testAsyncActionExportThrowsException() {
        final int nbDocs = randomIntBetween(1, 25);
        final Collection<MonitoringDoc> docs = new ArrayList<>(nbDocs);
        for (int i = 0; i < nbDocs; i++) {
            docs.add(mock(MonitoringDoc.class));
        }

        doThrow(new IllegalStateException("something went wrong")).when(exporters).export(any(Collection.class), any(ActionListener.class));

        final TransportMonitoringBulkAction.AsyncAction asyncAction = new TransportMonitoringBulkAction.AsyncAction(
            threadPool,
            null,
            null,
            exporters,
            null,
            0L,
            null
        );

        asyncAction.executeExport(docs, randomNonNegativeLong(), listener);

        verify(threadPool).executor(ThreadPool.Names.GENERIC);

        final ArgumentCaptor<Collection<MonitoringDoc>> argDocs = ArgumentCaptor.forClass((Class) Collection.class);
        verify(exporters).export(argDocs.capture(), any(ActionListener.class));
        assertThat(argDocs.getValue(), is(docs));

        final ArgumentCaptor<MonitoringBulkResponse> argResponse = ArgumentCaptor.forClass(MonitoringBulkResponse.class);
        verify(listener).onResponse(argResponse.capture());
        assertThat(argResponse.getValue().getError(), notNullValue());
        assertThat(argResponse.getValue().getError().getCause(), instanceOf(IllegalStateException.class));
    }

    /**
     * @return a new MonitoringBulkRequest instance with random number of documents
     */
    private MonitoringBulkRequest randomRequest() throws IOException {
        return randomRequest(scaledRandomIntBetween(1, 100));
    }

    /**
     * Creates a {@link MonitoringBulkRequest} with the given number of {@link MonitoringBulkDoc} in it.
     *
     * @return the {@link MonitoringBulkRequest}
     */
    private MonitoringBulkRequest randomRequest(final int numDocs) throws IOException {
        final MonitoringBulkRequest request = new MonitoringBulkRequest();
        for (int i = 0; i < numDocs; i++) {
            request.add(MonitoringTestUtils.randomMonitoringBulkDoc(random()));
        }
        return request;
    }
}
