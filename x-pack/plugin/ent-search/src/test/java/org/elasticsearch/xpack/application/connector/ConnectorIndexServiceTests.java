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
import org.elasticsearch.xpack.application.connector.action.PostConnectorAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorConfigurationAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorErrorAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorFilteringAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSeenAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSyncStatsAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorNameAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorPipelineAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorSchedulingAction;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        String connectorId = randomUUID();
        DocWriteResponse resp = awaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(connectorId, equalTo(indexedConnector.getConnectorId()));
    }

    public void testPostConnector() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        PostConnectorAction.Response resp = awaitPostConnector(connector);

        Connector indexedConnector = awaitGetConnector(resp.getId());
        assertThat(resp.getId(), equalTo(indexedConnector.getConnectorId()));
    }

    public void testDeleteConnector() throws Exception {
        int numConnectors = 5;
        List<String> connectorIds = new ArrayList<>();
        for (int i = 0; i < numConnectors; i++) {
            Connector connector = ConnectorTestUtils.getRandomConnector();
            PostConnectorAction.Response resp = awaitPostConnector(connector);
            connectorIds.add(resp.getId());
        }

        String connectorIdToDelete = connectorIds.get(0);
        DeleteResponse resp = awaitDeleteConnector(connectorIdToDelete);
        assertThat(resp.status(), equalTo(RestStatus.OK));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetConnector(connectorIdToDelete));

        expectThrows(ResourceNotFoundException.class, () -> awaitDeleteConnector(connectorIdToDelete));
    }

    public void testUpdateConnectorConfiguration() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = awaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        Map<String, ConnectorConfiguration> connectorConfiguration = connector.getConfiguration()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> ConnectorTestUtils.getRandomConnectorConfigurationField()));

        UpdateConnectorConfigurationAction.Request updateConfigurationRequest = new UpdateConnectorConfigurationAction.Request(
            connectorId,
            connectorConfiguration
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorConfiguration(updateConfigurationRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));
        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(connectorConfiguration, equalTo(indexedConnector.getConfiguration()));
        assertThat(indexedConnector.getStatus(), equalTo(ConnectorStatus.CONFIGURED));
    }

    public void testUpdateConnectorPipeline() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = awaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        ConnectorIngestPipeline updatedPipeline = new ConnectorIngestPipeline.Builder().setName("test-pipeline")
            .setExtractBinaryContent(false)
            .setReduceWhitespace(true)
            .setRunMlInference(false)
            .build();

        UpdateConnectorPipelineAction.Request updatePipelineRequest = new UpdateConnectorPipelineAction.Request(
            connectorId,
            updatedPipeline
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorPipeline(updatePipelineRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));
        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(updatedPipeline, equalTo(indexedConnector.getPipeline()));
    }

    public void testUpdateConnectorFiltering() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        DocWriteResponse resp = awaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        List<ConnectorFiltering> filteringList = IntStream.range(0, 10)
            .mapToObj((i) -> ConnectorTestUtils.getRandomConnectorFiltering())
            .collect(Collectors.toList());

        UpdateConnectorFilteringAction.Request updateFilteringRequest = new UpdateConnectorFilteringAction.Request(
            connectorId,
            filteringList
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorFiltering(updateFilteringRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));
        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(filteringList, equalTo(indexedConnector.getFiltering()));
    }

    public void testUpdateConnectorLastSeen() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        DocWriteResponse resp = awaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        UpdateConnectorLastSeenAction.Request checkInRequest = new UpdateConnectorLastSeenAction.Request(connectorId);
        DocWriteResponse updateResponse = awaitUpdateConnectorLastSeen(checkInRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnectorTime1 = awaitGetConnector(connectorId);
        assertNotNull(indexedConnectorTime1.getLastSeen());

        checkInRequest = new UpdateConnectorLastSeenAction.Request(connectorId);
        updateResponse = awaitUpdateConnectorLastSeen(checkInRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnectorTime2 = awaitGetConnector(connectorId);
        assertNotNull(indexedConnectorTime2.getLastSeen());
        assertTrue(indexedConnectorTime2.getLastSeen().isAfter(indexedConnectorTime1.getLastSeen()));

    }

    public void testUpdateConnectorLastSyncStats() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        DocWriteResponse resp = awaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        ConnectorSyncInfo syncStats = ConnectorTestUtils.getRandomConnectorSyncInfo();

        UpdateConnectorLastSyncStatsAction.Request lastSyncStats = new UpdateConnectorLastSyncStatsAction.Request(connectorId, syncStats);

        DocWriteResponse updateResponse = awaitUpdateConnectorLastSyncStats(lastSyncStats);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);

        assertThat(syncStats, equalTo(indexedConnector.getSyncInfo()));
    }

    public void testUpdateConnectorScheduling() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        DocWriteResponse resp = awaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        ConnectorScheduling updatedScheduling = ConnectorTestUtils.getRandomConnectorScheduling();

        UpdateConnectorSchedulingAction.Request updateSchedulingRequest = new UpdateConnectorSchedulingAction.Request(
            connectorId,
            updatedScheduling
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorScheduling(updateSchedulingRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(updatedScheduling, equalTo(indexedConnector.getScheduling()));
    }

    public void testUpdateConnectorError() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = awaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        UpdateConnectorErrorAction.Request updateErrorRequest = new UpdateConnectorErrorAction.Request(
            connectorId,
            randomAlphaOfLengthBetween(5, 15)
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorError(updateErrorRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(updateErrorRequest.getError(), equalTo(indexedConnector.getError()));
    }

    public void testUpdateConnectorNameOrDescription() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = awaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        UpdateConnectorNameAction.Request updateNameDescriptionRequest = new UpdateConnectorNameAction.Request(
            connectorId,
            randomAlphaOfLengthBetween(5, 15),
            randomAlphaOfLengthBetween(5, 15)
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorName(updateNameDescriptionRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(updateNameDescriptionRequest.getName(), equalTo(indexedConnector.getName()));
        assertThat(updateNameDescriptionRequest.getDescription(), equalTo(indexedConnector.getDescription()));
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

    private DocWriteResponse awaitPutConnector(String docId, Connector connector) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DocWriteResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.putConnector(docId, connector, new ActionListener<>() {
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

    private PostConnectorAction.Response awaitPostConnector(Connector connector) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<PostConnectorAction.Response> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.postConnector(connector, new ActionListener<>() {
            @Override
            public void onResponse(PostConnectorAction.Response indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for post connector request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from post connector request", resp.get());
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

    private UpdateResponse awaitUpdateConnectorConfiguration(UpdateConnectorConfigurationAction.Request updateConfiguration)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorConfiguration(updateConfiguration, new ActionListener<>() {
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
        assertTrue("Timeout waiting for update configuration request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update configuration request", resp.get());
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

    private UpdateResponse awaitUpdateConnectorName(UpdateConnectorNameAction.Request updatedNameOrDescription) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorNameOrDescription(updatedNameOrDescription, new ActionListener<>() {
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
        assertTrue("Timeout waiting for update name request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update name request", resp.get());
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
