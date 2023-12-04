/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorErrorAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorFilteringAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSeenAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSyncStatsAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorPipelineAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorSchedulingAction;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;

public class ConnectorIndexServiceTests extends ESSingleNodeTestCase {

    private static final int REQUEST_TIMEOUT_SECONDS = 10;

    private ConnectorIndexService connectorIndexService;

    @Before
    public void setup() {
        this.connectorIndexService = new ConnectorIndexService(client());
    }

    public void testPutConnector() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        DocWriteResponse resp = awaitPutConnector(connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));
    }

    public void testDeleteConnector() throws Exception {
        int numConnectors = 5;
        List<String> connectorIds = new ArrayList<>();
        for (int i = 0; i < numConnectors; i++) {
            Connector connector = ConnectorTestUtils.getRandomConnector();
            connectorIds.add(connector.getConnectorId());
            DocWriteResponse resp = awaitPutConnector(connector);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
        }

        String connectorIdToDelete = connectorIds.get(0);
        DeleteResponse resp = awaitDeleteConnector(connectorIdToDelete);
        assertThat(resp.status(), equalTo(RestStatus.OK));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetConnector(connectorIdToDelete));

        expectThrows(ResourceNotFoundException.class, () -> awaitDeleteConnector(connectorIdToDelete));
    }

    public void testUpdateConnectorPipeline() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        DocWriteResponse resp = awaitPutConnector(connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        ConnectorIngestPipeline updatedPipeline = new ConnectorIngestPipeline.Builder().setName("test-pipeline")
            .setExtractBinaryContent(false)
            .setReduceWhitespace(true)
            .setRunMlInference(false)
            .build();

        UpdateConnectorPipelineAction.Request updatePipelineRequest = new UpdateConnectorPipelineAction.Request(
            connector.getConnectorId(),
            updatedPipeline
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorPipeline(updatePipelineRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));
        Connector indexedConnector = awaitGetConnector(connector.getConnectorId());
        assertThat(updatedPipeline, equalTo(indexedConnector.getPipeline()));
    }

    public void testUpdateConnectorFiltering() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();

        DocWriteResponse resp = awaitPutConnector(connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        List<ConnectorFiltering> filteringList = IntStream.range(0, 10)
            .mapToObj((i) -> ConnectorTestUtils.getRandomConnectorFiltering())
            .collect(Collectors.toList());

        UpdateConnectorFilteringAction.Request updateFilteringRequest = new UpdateConnectorFilteringAction.Request(
            connector.getConnectorId(),
            filteringList
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorFiltering(updateFilteringRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));
        Connector indexedConnector = awaitGetConnector(connector.getConnectorId());
        assertThat(filteringList, equalTo(indexedConnector.getFiltering()));
    }

    public void testUpdateConnectorLastSeen() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        DocWriteResponse resp = awaitPutConnector(connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        UpdateConnectorLastSeenAction.Request checkInRequest = new UpdateConnectorLastSeenAction.Request(connector.getConnectorId());
        DocWriteResponse updateResponse = awaitUpdateConnectorLastSeen(checkInRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnectorTime1 = awaitGetConnector(connector.getConnectorId());
        assertNotNull(indexedConnectorTime1.getLastSeen());

        checkInRequest = new UpdateConnectorLastSeenAction.Request(connector.getConnectorId());
        updateResponse = awaitUpdateConnectorLastSeen(checkInRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnectorTime2 = awaitGetConnector(connector.getConnectorId());
        assertNotNull(indexedConnectorTime2.getLastSeen());
        assertTrue(indexedConnectorTime2.getLastSeen().isAfter(indexedConnectorTime1.getLastSeen()));

    }

    public void testUpdateConnectorLastSyncStats() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();

        DocWriteResponse resp = awaitPutConnector(connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        ConnectorSyncInfo syncStats = ConnectorTestUtils.getRandomConnectorSyncInfo();

        UpdateConnectorLastSyncStatsAction.Request lastSyncStats = new UpdateConnectorLastSyncStatsAction.Request(
            connector.getConnectorId(),
            syncStats
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorLastSyncStats(lastSyncStats);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connector.getConnectorId());

        assertThat(syncStats, equalTo(indexedConnector.getSyncInfo()));
    }

    public void testUpdateConnectorScheduling() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        DocWriteResponse resp = awaitPutConnector(connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        ConnectorScheduling updatedScheduling = ConnectorTestUtils.getRandomConnectorScheduling();

        UpdateConnectorSchedulingAction.Request updateSchedulingRequest = new UpdateConnectorSchedulingAction.Request(
            connector.getConnectorId(),
            updatedScheduling
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorScheduling(updateSchedulingRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connector.getConnectorId());
        assertThat(updatedScheduling, equalTo(indexedConnector.getScheduling()));
    }

    public void testUpdateConnectorError() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        DocWriteResponse resp = awaitPutConnector(connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        UpdateConnectorErrorAction.Request updateErrorRequest = new UpdateConnectorErrorAction.Request(
            connector.getConnectorId(),
            randomAlphaOfLengthBetween(5, 15)
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorError(updateErrorRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connector.getConnectorId());
        assertThat(updateErrorRequest.getError(), equalTo(indexedConnector.getError()));
    }

    private DeleteResponse awaitDeleteConnector(String connectorId) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DeleteResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.deleteConnector(connectorId, new ActionListener<>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                resp.set(deleteResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for delete request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from delete request", resp.get());
        return resp.get();
    }

    private DocWriteResponse awaitPutConnector(Connector connector) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DocWriteResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.putConnector(connector, new ActionListener<>() {
            @Override
            public void onResponse(DocWriteResponse indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for put request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from put request", resp.get());
        return resp.get();
    }

    private Connector awaitGetConnector(String connectorId) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Connector> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.getConnector(connectorId, new ActionListener<>() {
            @Override
            public void onResponse(Connector connector) {
                resp.set(connector);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for get request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from get request", resp.get());
        return resp.get();
    }

    private ConnectorIndexService.ConnectorResult awaitListConnector(int from, int size) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ConnectorIndexService.ConnectorResult> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.listConnectors(from, size, new ActionListener<>() {
            @Override
            public void onResponse(ConnectorIndexService.ConnectorResult result) {
                resp.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for list request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from list request", resp.get());
        return resp.get();
    }

    private UpdateResponse awaitUpdateConnectorFiltering(UpdateConnectorFilteringAction.Request updateFiltering) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorFiltering(updateFiltering, new ActionListener<>() {

            @Override
            public void onResponse(UpdateResponse indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });

        assertTrue("Timeout waiting for update filtering request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update filtering request", resp.get());
        return resp.get();
    }

    private UpdateResponse awaitUpdateConnectorLastSeen(UpdateConnectorLastSeenAction.Request checkIn) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorLastSeen(checkIn, new ActionListener<>() {
            @Override
            public void onResponse(UpdateResponse indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for check-in request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from check-in request", resp.get());
        return resp.get();
    }

    private UpdateResponse awaitUpdateConnectorLastSyncStats(UpdateConnectorLastSyncStatsAction.Request updateLastSyncStats)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorLastSyncStats(updateLastSyncStats, new ActionListener<>() {
            @Override
            public void onResponse(UpdateResponse indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for update last sync stats request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update last sync stats request", resp.get());
        return resp.get();
    }

    private UpdateResponse awaitUpdateConnectorPipeline(UpdateConnectorPipelineAction.Request updatePipeline) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorPipeline(updatePipeline, new ActionListener<>() {
            @Override
            public void onResponse(UpdateResponse indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for update pipeline request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update pipeline request", resp.get());
        return resp.get();
    }

    private UpdateResponse awaitUpdateConnectorScheduling(UpdateConnectorSchedulingAction.Request updatedScheduling) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorScheduling(updatedScheduling, new ActionListener<>() {
            @Override
            public void onResponse(UpdateResponse indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for update scheduling request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update scheduling request", resp.get());
        return resp.get();
    }

    private UpdateResponse awaitUpdateConnectorError(UpdateConnectorErrorAction.Request updatedError) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorError(updatedError, new ActionListener<>() {
            @Override
            public void onResponse(UpdateResponse indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for update error request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update error request", resp.get());
        return resp.get();
    }
}
