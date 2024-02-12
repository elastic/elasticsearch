/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.UpdateScript;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.action.PostConnectorAction;
import org.elasticsearch.xpack.application.connector.action.PutConnectorAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorApiKeyIdAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorConfigurationAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorErrorAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorFilteringAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorIndexNameAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSeenAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSyncStatsAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorNameAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorNativeAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorPipelineAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorSchedulingAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorServiceTypeAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorStatusAction;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(MockPainlessScriptEngine.TestPlugin.class);
        return plugins;
    }

    public void testPutConnector() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(connectorId, equalTo(indexedConnector.getConnectorId()));
    }

    public void testPostConnector() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        PostConnectorAction.Response resp = buildRequestAndAwaitPostConnector(connector);

        Connector indexedConnector = awaitGetConnector(resp.getId());
        assertThat(resp.getId(), equalTo(indexedConnector.getConnectorId()));
    }

    public void testDeleteConnector() throws Exception {
        int numConnectors = 5;
        List<String> connectorIds = new ArrayList<>();
        for (int i = 0; i < numConnectors; i++) {
            Connector connector = ConnectorTestUtils.getRandomConnector();
            PostConnectorAction.Response resp = buildRequestAndAwaitPostConnector(connector);
            connectorIds.add(resp.getId());
        }

        String connectorIdToDelete = connectorIds.get(0);
        DeleteResponse resp = awaitDeleteConnector(connectorIdToDelete);
        assertThat(resp.status(), equalTo(RestStatus.OK));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetConnector(connectorIdToDelete));

        expectThrows(ResourceNotFoundException.class, () -> awaitDeleteConnector(connectorIdToDelete));
    }

    public void testUpdateConnectorConfiguration_FullConfiguration() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        UpdateConnectorConfigurationAction.Request updateConfigurationRequest = new UpdateConnectorConfigurationAction.Request(
            connectorId,
            connector.getConfiguration(),
            null
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorConfiguration(updateConfigurationRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        // Full configuration update is handled via painless script. ScriptEngine is mocked for unit tests.
        // More comprehensive tests are defined in yamlRestTest.
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/enterprise-search-team/issues/6351")
    public void testUpdateConnectorConfiguration_PartialValuesUpdate() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        Map<String, Object> connectorNewConfiguration = connector.getConfiguration()
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> Map.of(ConnectorConfiguration.VALUE_FIELD.getPreferredName(), randomAlphaOfLengthBetween(3, 10))
                )
            );

        UpdateConnectorConfigurationAction.Request updateConfigurationRequest = new UpdateConnectorConfigurationAction.Request(
            connectorId,
            null,
            connectorNewConfiguration
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorConfiguration(updateConfigurationRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);

        Map<String, ConnectorConfiguration> indexedConnectorConfiguration = indexedConnector.getConfiguration();

        for (String configKey : indexedConnectorConfiguration.keySet()) {
            assertThat(indexedConnectorConfiguration.get(configKey).getValue(), equalTo(connectorNewConfiguration.get(configKey)));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/enterprise-search-team/issues/6351")
    public void testUpdateConnectorConfiguration_PartialValuesUpdate_SelectedKeys() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        Set<String> configKeys = connector.getConfiguration().keySet();

        Set<String> keysToUpdate = new HashSet<>(randomSubsetOf(configKeys));

        Map<String, Object> connectorNewConfigurationPartialValuesUpdate = keysToUpdate.stream()
            .collect(
                Collectors.toMap(
                    key -> key,
                    key -> Map.of(ConnectorConfiguration.VALUE_FIELD.getPreferredName(), randomAlphaOfLengthBetween(3, 10))
                )
            );

        UpdateConnectorConfigurationAction.Request updateConfigurationRequest = new UpdateConnectorConfigurationAction.Request(
            connectorId,
            null,
            connectorNewConfigurationPartialValuesUpdate
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorConfiguration(updateConfigurationRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);

        Map<String, ConnectorConfiguration> indexedConnectorConfiguration = indexedConnector.getConfiguration();

        for (String configKey : indexedConnectorConfiguration.keySet()) {
            if (keysToUpdate.contains(configKey)) {
                assertThat(
                    indexedConnectorConfiguration.get(configKey).getValue(),
                    equalTo(connectorNewConfigurationPartialValuesUpdate.get(configKey))
                );
            } else {
                assertThat(
                    indexedConnectorConfiguration.get(configKey).getValue(),
                    equalTo(connector.getConfiguration().get(configKey).getValue())
                );
            }
        }
    }

    public void testUpdateConnectorPipeline() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
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

        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
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

        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
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

        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
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

        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
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

    public void testUpdateConnectorIndexName() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        String newIndexName = randomAlphaOfLengthBetween(3, 10);

        UpdateConnectorIndexNameAction.Request updateIndexNameRequest = new UpdateConnectorIndexNameAction.Request(
            connectorId,
            newIndexName
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorIndexName(updateIndexNameRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(newIndexName, equalTo(indexedConnector.getIndexName()));
    }

    public void testUpdateConnectorIndexName_WithTheSameIndexName() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        UpdateConnectorIndexNameAction.Request updateIndexNameRequest = new UpdateConnectorIndexNameAction.Request(
            connectorId,
            connector.getIndexName()
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorIndexName(updateIndexNameRequest);
        assertThat(updateResponse.getResult(), equalTo(DocWriteResponse.Result.NOOP));
    }

    public void testUpdateConnectorServiceType() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        String newServiceType = randomAlphaOfLengthBetween(3, 10);

        UpdateConnectorServiceTypeAction.Request updateServiceTypeRequest = new UpdateConnectorServiceTypeAction.Request(
            connectorId,
            newServiceType
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorServiceType(updateServiceTypeRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(newServiceType, equalTo(indexedConnector.getServiceType()));
    }

    public void testUpdateConnectorError() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
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
        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
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

    public void testUpdateConnectorNative() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        boolean isNative = randomBoolean();

        UpdateConnectorNativeAction.Request updateNativeRequest = new UpdateConnectorNativeAction.Request(connectorId, isNative);

        DocWriteResponse updateResponse = awaitUpdateConnectorNative(updateNativeRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(isNative, equalTo(indexedConnector.isNative()));
    }

    public void testUpdateConnectorStatus() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        Connector indexedConnector = awaitGetConnector(connectorId);

        ConnectorStatus newStatus = ConnectorTestUtils.getRandomConnectorNextStatus(indexedConnector.getStatus());

        UpdateConnectorStatusAction.Request updateStatusRequest = new UpdateConnectorStatusAction.Request(connectorId, newStatus);

        DocWriteResponse updateResponse = awaitUpdateConnectorStatus(updateStatusRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        indexedConnector = awaitGetConnector(connectorId);
        assertThat(newStatus, equalTo(indexedConnector.getStatus()));
    }

    public void testUpdateConnectorStatus_WithInvalidStatus() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        Connector indexedConnector = awaitGetConnector(connectorId);

        ConnectorStatus newInvalidStatus = ConnectorTestUtils.getRandomInvalidConnectorNextStatus(indexedConnector.getStatus());

        UpdateConnectorStatusAction.Request updateStatusRequest = new UpdateConnectorStatusAction.Request(connectorId, newInvalidStatus);

        expectThrows(ElasticsearchStatusException.class, () -> awaitUpdateConnectorStatus(updateStatusRequest));
    }

    public void testUpdateConnectorApiKeyIdOrApiKeySecretId() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        DocWriteResponse resp = buildRequestAndAwaitPutConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        UpdateConnectorApiKeyIdAction.Request updateApiKeyIdRequest = new UpdateConnectorApiKeyIdAction.Request(
            connectorId,
            randomAlphaOfLengthBetween(5, 15),
            randomAlphaOfLengthBetween(5, 15)
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorApiKeyIdOrApiKeySecretId(updateApiKeyIdRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(updateApiKeyIdRequest.getApiKeyId(), equalTo(indexedConnector.getApiKeyId()));
        assertThat(updateApiKeyIdRequest.getApiKeySecretId(), equalTo(indexedConnector.getApiKeySecretId()));
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

    private DocWriteResponse buildRequestAndAwaitPutConnector(String docId, Connector connector) throws Exception {
        PutConnectorAction.Request putConnectorRequest = new PutConnectorAction.Request(
            docId,
            connector.getDescription(),
            connector.getIndexName(),
            connector.isNative(),
            connector.getLanguage(),
            connector.getName(),
            connector.getServiceType()
        );
        return awaitPutConnector(putConnectorRequest);
    }

    private DocWriteResponse awaitPutConnector(PutConnectorAction.Request request) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DocWriteResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.createConnectorWithDocId(request, new ActionListener<>() {
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

    private PostConnectorAction.Response buildRequestAndAwaitPostConnector(Connector connector) throws Exception {
        PostConnectorAction.Request postConnectorRequest = new PostConnectorAction.Request(
            connector.getDescription(),
            connector.getIndexName(),
            connector.isNative(),
            connector.getLanguage(),
            connector.getName(),
            connector.getServiceType()
        );
        return awaitPostConnector(postConnectorRequest);
    }

    private PostConnectorAction.Response awaitPostConnector(PostConnectorAction.Request request) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<PostConnectorAction.Response> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.createConnectorWithAutoGeneratedId(request, new ActionListener<>() {
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
            public void onResponse(ConnectorSearchResult connectorResult) {
                // Serialize the sourceRef to Connector class for unit tests
                Connector connector = Connector.fromXContentBytes(
                    connectorResult.getSourceRef(),
                    connectorResult.getDocId(),
                    XContentType.JSON
                );
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

    private ConnectorIndexService.ConnectorResult awaitListConnector(
        int from,
        int size,
        List<String> indexNames,
        List<String> names,
        List<String> serviceTypes,
        String searchQuery
    ) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ConnectorIndexService.ConnectorResult> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.listConnectors(from, size, indexNames, names, serviceTypes, searchQuery, new ActionListener<>() {
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

    private UpdateResponse awaitUpdateConnectorIndexName(UpdateConnectorIndexNameAction.Request updateIndexNameRequest) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorIndexName(updateIndexNameRequest, new ActionListener<>() {
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

        assertTrue("Timeout waiting for update index name request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update index name request", resp.get());

        return resp.get();
    }

    private UpdateResponse awaitUpdateConnectorStatus(UpdateConnectorStatusAction.Request updateStatusRequest) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorStatus(updateStatusRequest, new ActionListener<>() {
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

        assertTrue("Timeout waiting for update status request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update status request", resp.get());

        return resp.get();
    }

    private UpdateResponse awaitUpdateConnectorLastSeen(UpdateConnectorLastSeenAction.Request checkIn) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.checkInConnector(checkIn.getConnectorId(), new ActionListener<>() {
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

    private UpdateResponse awaitUpdateConnectorNative(UpdateConnectorNativeAction.Request updateIndexNameRequest) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorNative(updateIndexNameRequest, new ActionListener<>() {
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
        assertTrue("Timeout waiting for update is_native request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update is_native request", resp.get());
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

    private UpdateResponse awaitUpdateConnectorServiceType(UpdateConnectorServiceTypeAction.Request updateServiceTypeRequest)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorServiceType(updateServiceTypeRequest, new ActionListener<>() {
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
        assertTrue("Timeout waiting for update service type request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update service type request", resp.get());
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

    private UpdateResponse awaitUpdateConnectorApiKeyIdOrApiKeySecretId(
        UpdateConnectorApiKeyIdAction.Request updatedApiKeyIdOrApiKeySecretId
    ) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorApiKeyIdOrApiKeySecretId(updatedApiKeyIdOrApiKeySecretId, new ActionListener<>() {
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
        assertTrue("Timeout waiting for update api key id request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update api key id request", resp.get());
        return resp.get();
    }

    /**
     * Update configuration action is handled via painless script. This implementation mocks the painless script engine
     * for unit tests.
     */
    private static class MockPainlessScriptEngine extends MockScriptEngine {

        public static final String NAME = "painless";

        public static class TestPlugin extends MockScriptPlugin {
            @Override
            public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                return new ConnectorIndexServiceTests.MockPainlessScriptEngine();
            }

            @Override
            protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
                return Collections.emptyMap();
            }
        }

        @Override
        public String getType() {
            return NAME;
        }

        @Override
        public <T> T compile(String name, String script, ScriptContext<T> context, Map<String, String> options) {
            if (context.instanceClazz.equals(UpdateScript.class)) {
                UpdateScript.Factory factory = (params, ctx) -> new UpdateScript(params, ctx) {
                    @Override
                    public void execute() {

                    }
                };
                return context.factoryClazz.cast(factory);
            }
            throw new IllegalArgumentException("mock painless does not know how to handle context [" + context.name + "]");
        }
    }

}
