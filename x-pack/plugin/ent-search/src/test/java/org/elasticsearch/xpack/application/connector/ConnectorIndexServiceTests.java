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
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.UpdateScript;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.action.ConnectorCreateActionResponse;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorApiKeyIdAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorConfigurationAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorIndexNameAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSeenAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorLastSyncStatsAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorNameAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorNativeAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorPipelineAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorSchedulingAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorServiceTypeAction;
import org.elasticsearch.xpack.application.connector.action.UpdateConnectorStatusAction;
import org.elasticsearch.xpack.application.connector.filtering.FilteringAdvancedSnippet;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRule;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationInfo;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationState;
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

import static org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry.MANAGED_CONNECTOR_INDEX_PREFIX;
import static org.elasticsearch.xpack.application.connector.ConnectorTestUtils.getRandomConnectorFeatures;
import static org.elasticsearch.xpack.application.connector.ConnectorTestUtils.getRandomCronExpression;
import static org.elasticsearch.xpack.application.connector.ConnectorTestUtils.randomConnectorFeatureEnabled;
import static org.elasticsearch.xpack.application.connector.ConnectorTestUtils.registerSimplifiedConnectorIndexTemplates;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

public class ConnectorIndexServiceTests extends ESSingleNodeTestCase {

    private static final int REQUEST_TIMEOUT_SECONDS = 10;

    private ConnectorIndexService connectorIndexService;

    @Before
    public void setup() {
        registerSimplifiedConnectorIndexTemplates(indicesAdmin());
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
        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(connectorId, equalTo(indexedConnector.getConnectorId()));
    }

    public void testPostConnector() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        ConnectorCreateActionResponse resp = awaitCreateConnector(null, connector);

        Connector indexedConnector = awaitGetConnector(resp.getId());
        assertThat(resp.getId(), equalTo(indexedConnector.getConnectorId()));
    }

    public void testDeleteConnector_expectSoftDeletionSingle() throws Exception {
        int numConnectors = 5;
        List<String> connectorIds = new ArrayList<>();
        for (int i = 0; i < numConnectors; i++) {
            Connector connector = ConnectorTestUtils.getRandomConnector();
            ConnectorCreateActionResponse resp = awaitCreateConnector(null, connector);
            connectorIds.add(resp.getId());
        }

        String connectorIdToDelete = connectorIds.get(0);
        DocWriteResponse resp = awaitDeleteConnector(connectorIdToDelete, false, false);
        assertThat(resp.status(), equalTo(RestStatus.OK));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetConnector(connectorIdToDelete));
        expectThrows(ResourceNotFoundException.class, () -> awaitDeleteConnector(connectorIdToDelete, false, false));
    }

    public void testDeleteConnector_expectSoftDeletion() throws Exception {
        int numConnectors = 5;
        List<String> connectorIds = new ArrayList<>();
        List<Connector> connectors = new ArrayList<>();
        for (int i = 0; i < numConnectors; i++) {
            Connector connector = ConnectorTestUtils.getRandomConnector();
            ConnectorCreateActionResponse resp = awaitCreateConnector(null, connector);
            connectorIds.add(resp.getId());
            connectors.add(connector);
        }

        String connectorIdToDelete = connectorIds.get(0);
        DocWriteResponse resp = awaitDeleteConnector(connectorIdToDelete, false, false);
        assertThat(resp.status(), equalTo(RestStatus.OK));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetConnector(connectorIdToDelete));
        expectThrows(ResourceNotFoundException.class, () -> awaitDeleteConnector(connectorIdToDelete, false, false));
        Connector softDeletedConnector = awaitGetConnectorIncludeDeleted(connectorIdToDelete);
        assertThat(softDeletedConnector.getConnectorId(), equalTo(connectorIdToDelete));
        assertThat(softDeletedConnector.getServiceType(), equalTo(connectors.get(0).getServiceType()));
    }

    public void testDeleteConnector_expectSoftDeletionMultipleConnectors() throws Exception {
        int numConnectors = 5;
        List<String> connectorIds = new ArrayList<>();
        for (int i = 0; i < numConnectors; i++) {
            Connector connector = ConnectorTestUtils.getRandomConnector();
            ConnectorCreateActionResponse resp = awaitCreateConnector(null, connector);
            connectorIds.add(resp.getId());
        }

        for (int i = 0; i < numConnectors; i++) {
            String connectorIdToDelete = connectorIds.get(i);
            DocWriteResponse resp = awaitDeleteConnector(connectorIdToDelete, false, false);
            assertThat(resp.status(), equalTo(RestStatus.OK));
        }

        for (int i = 0; i < numConnectors; i++) {
            String connectorId = connectorIds.get(i);
            expectThrows(ResourceNotFoundException.class, () -> awaitGetConnector(connectorId));
        }

        for (int i = 0; i < numConnectors; i++) {
            String connectorId = connectorIds.get(i);
            Connector softDeletedConnector = awaitGetConnectorIncludeDeleted(connectorId);
            assertThat(softDeletedConnector.getConnectorId(), equalTo(connectorId));
        }
    }

    public void testDeleteConnector_expectHardDeletionSingle() throws Exception {
        int numConnectors = 3;
        List<String> connectorIds = new ArrayList<>();
        for (int i = 0; i < numConnectors; i++) {
            Connector connector = ConnectorTestUtils.getRandomConnector();
            ConnectorCreateActionResponse resp = awaitCreateConnector(null, connector);
            connectorIds.add(resp.getId());
        }

        String connectorIdToDelete = connectorIds.get(0);
        DocWriteResponse resp = awaitDeleteConnector(connectorIdToDelete, true, false);
        assertThat(resp.status(), equalTo(RestStatus.OK));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetConnector(connectorIdToDelete));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetConnectorIncludeDeleted(connectorIdToDelete));
        expectThrows(ResourceNotFoundException.class, () -> awaitDeleteConnector(connectorIdToDelete, true, false));
    }

    public void testDeleteConnector_expectHardDeletionMultipleConnectors() throws Exception {
        int numConnectors = 5;
        List<String> connectorIds = new ArrayList<>();
        for (int i = 0; i < numConnectors; i++) {
            Connector connector = ConnectorTestUtils.getRandomConnector();
            ConnectorCreateActionResponse resp = awaitCreateConnector(null, connector);
            connectorIds.add(resp.getId());
        }

        for (int i = 0; i < numConnectors; i++) {
            String connectorIdToDelete = connectorIds.get(i);
            DocWriteResponse resp = awaitDeleteConnector(connectorIdToDelete, true, false);
            assertThat(resp.status(), equalTo(RestStatus.OK));
        }

        for (String connectorId : connectorIds) {
            expectThrows(ResourceNotFoundException.class, () -> awaitGetConnector(connectorId));
        }

        for (String connectorId : connectorIds) {
            expectThrows(ResourceNotFoundException.class, () -> awaitGetConnectorIncludeDeleted(connectorId));
        }
    }

    public void testUpdateConnectorConfiguration_FullConfiguration() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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
        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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
        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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
        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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

    public void testUpdateConnectorFeatures() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        ConnectorFeatures newFeatures = getRandomConnectorFeatures();

        DocWriteResponse updateResponse = awaitUpdateConnectorFeatures(connectorId, newFeatures);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));
        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(newFeatures, equalTo(indexedConnector.getFeatures()));

    }

    public void testUpdateConnectorFeatures_partialUpdate() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        ConnectorFeatures features = getRandomConnectorFeatures();

        awaitUpdateConnectorFeatures(connectorId, features);

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(features, equalTo(indexedConnector.getFeatures()));

        // Partial update of DLS feature
        ConnectorFeatures dlsFeature = new ConnectorFeatures.Builder().setDocumentLevelSecurityEnabled(randomConnectorFeatureEnabled())
            .build();
        awaitUpdateConnectorFeatures(connectorId, dlsFeature);
        indexedConnector = awaitGetConnector(connectorId);

        // Assert that partial update was applied
        assertThat(dlsFeature.getDocumentLevelSecurityEnabled(), equalTo(indexedConnector.getFeatures().getDocumentLevelSecurityEnabled()));

        // Assert other features are unchanged
        assertThat(features.getSyncRulesFeatures(), equalTo(indexedConnector.getFeatures().getSyncRulesFeatures()));
        assertThat(features.getNativeConnectorAPIKeysEnabled(), equalTo(indexedConnector.getFeatures().getNativeConnectorAPIKeysEnabled()));
        assertThat(features.getIncrementalSyncEnabled(), equalTo(indexedConnector.getFeatures().getIncrementalSyncEnabled()));
    }

    public void testUpdateConnectorFiltering() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        List<ConnectorFiltering> filteringList = IntStream.range(0, 10)
            .mapToObj((i) -> ConnectorTestUtils.getRandomConnectorFiltering())
            .collect(Collectors.toList());

        DocWriteResponse updateResponse = awaitUpdateConnectorFiltering(connectorId, filteringList);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));
        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(filteringList, equalTo(indexedConnector.getFiltering()));
    }

    public void testUpdateConnectorFiltering_updateDraft() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        FilteringAdvancedSnippet advancedSnippet = ConnectorTestUtils.getRandomConnectorFiltering().getDraft().getAdvancedSnippet();
        List<FilteringRule> rules = ConnectorTestUtils.getRandomConnectorFiltering().getDraft().getRules();

        DocWriteResponse updateResponse = awaitUpdateConnectorFilteringDraft(connectorId, advancedSnippet, rules);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));
        Connector indexedConnector = awaitGetConnector(connectorId);

        // Assert that draft got updated
        assertThat(advancedSnippet, equalTo(indexedConnector.getFiltering().get(0).getDraft().getAdvancedSnippet()));
        assertThat(rules, equalTo(indexedConnector.getFiltering().get(0).getDraft().getRules()));
        // Assert that draft is marked as EDITED
        assertThat(
            FilteringValidationInfo.getInitialDraftValidationInfo(),
            equalTo(indexedConnector.getFiltering().get(0).getDraft().getFilteringValidationInfo())
        );
        // Assert that default active rules are unchanged, avoid comparing timestamps
        assertThat(
            ConnectorFiltering.getDefaultConnectorFilteringConfig().getActive().getAdvancedSnippet().getAdvancedSnippetValue(),
            equalTo(indexedConnector.getFiltering().get(0).getActive().getAdvancedSnippet().getAdvancedSnippetValue())
        );
        // Assert that domain is unchanged
        assertThat(
            ConnectorFiltering.getDefaultConnectorFilteringConfig().getDomain(),
            equalTo(indexedConnector.getFiltering().get(0).getDomain())
        );
    }

    public void testUpdateConnectorFiltering_updateDraftWithDefaultRuleOnly() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        FilteringAdvancedSnippet advancedSnippet = ConnectorTestUtils.getRandomConnectorFiltering().getDraft().getAdvancedSnippet();
        List<FilteringRule> rules = ConnectorTestUtils.getRandomConnectorFiltering().getDraft().getRules();

        DocWriteResponse updateResponse = awaitUpdateConnectorFilteringDraft(connectorId, advancedSnippet, rules);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        List<FilteringRule> defaultRules = List.of(ConnectorFiltering.getDefaultFilteringRuleWithOrder(0));

        DocWriteResponse defaultUpdateResponse = awaitUpdateConnectorFilteringDraft(connectorId, advancedSnippet, defaultRules);
        assertThat(defaultUpdateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);

        // Assert that draft has correct rules
        assertTrue(
            indexedConnector.getFiltering()
                .get(0)
                .getDraft()
                .getRules()
                .get(0)
                .equalsExceptForTimestampsAndOrder(ConnectorFiltering.getDefaultFilteringRuleWithOrder(0))
        );

        // Assert that draft is marked as EDITED
        assertThat(
            FilteringValidationInfo.getInitialDraftValidationInfo(),
            equalTo(indexedConnector.getFiltering().get(0).getDraft().getFilteringValidationInfo())
        );
    }

    public void testUpdateConnectorFiltering_updateDraftWithEmptyRules() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        FilteringAdvancedSnippet advancedSnippet = ConnectorTestUtils.getRandomConnectorFiltering().getDraft().getAdvancedSnippet();
        List<FilteringRule> rules = ConnectorTestUtils.getRandomConnectorFiltering().getDraft().getRules();

        DocWriteResponse updateResponse = awaitUpdateConnectorFilteringDraft(connectorId, advancedSnippet, rules);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        List<FilteringRule> emptyRules = Collections.emptyList();

        DocWriteResponse emptyUpdateResponse = awaitUpdateConnectorFilteringDraft(connectorId, advancedSnippet, emptyRules);
        assertThat(emptyUpdateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);

        // Assert that draft got updated
        assertThat(emptyRules, not(equalTo(indexedConnector.getFiltering().get(0).getDraft().getRules())));
        assertTrue(
            indexedConnector.getFiltering()
                .get(0)
                .getDraft()
                .getRules()
                .get(0)
                .equalsExceptForTimestampsAndOrder(ConnectorFiltering.getDefaultFilteringRuleWithOrder(0))
        );

        // Assert that draft is marked as EDITED
        assertThat(
            FilteringValidationInfo.getInitialDraftValidationInfo(),
            equalTo(indexedConnector.getFiltering().get(0).getDraft().getFilteringValidationInfo())
        );
    }

    public void testUpdateConnectorFilteringValidation() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        FilteringValidationInfo validationInfo = ConnectorTestUtils.getRandomFilteringValidationInfo();

        DocWriteResponse validationInfoUpdateResponse = awaitUpdateConnectorDraftFilteringValidation(connectorId, validationInfo);
        assertThat(validationInfoUpdateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);

        assertThat(validationInfo, equalTo(indexedConnector.getFiltering().get(0).getDraft().getFilteringValidationInfo()));
    }

    public void testActivateConnectorDraftFiltering_draftValid_shouldActivate() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        // Populate draft filtering
        FilteringAdvancedSnippet advancedSnippet = ConnectorTestUtils.getRandomConnectorFiltering().getDraft().getAdvancedSnippet();
        List<FilteringRule> rules = ConnectorTestUtils.getRandomConnectorFiltering().getDraft().getRules();

        DocWriteResponse updateResponse = awaitUpdateConnectorFilteringDraft(connectorId, advancedSnippet, rules);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        FilteringValidationInfo validationSuccess = new FilteringValidationInfo.Builder().setValidationState(FilteringValidationState.VALID)
            .setValidationErrors(Collections.emptyList())
            .build();

        DocWriteResponse validationInfoUpdateResponse = awaitUpdateConnectorDraftFilteringValidation(connectorId, validationSuccess);
        assertThat(validationInfoUpdateResponse.status(), equalTo(RestStatus.OK));

        DocWriteResponse activateFilteringResponse = awaitActivateConnectorDraftFiltering(connectorId);
        assertThat(activateFilteringResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);

        // Assert that draft is activated
        assertThat(advancedSnippet, equalTo(indexedConnector.getFiltering().get(0).getActive().getAdvancedSnippet()));
        assertThat(rules, equalTo(indexedConnector.getFiltering().get(0).getActive().getRules()));
    }

    public void testActivateConnectorDraftFiltering_draftNotValid_expectFailure() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        FilteringValidationInfo validationFailure = new FilteringValidationInfo.Builder().setValidationState(
            FilteringValidationState.INVALID
        ).setValidationErrors(Collections.emptyList()).build();

        DocWriteResponse validationInfoUpdateResponse = awaitUpdateConnectorDraftFilteringValidation(connectorId, validationFailure);
        assertThat(validationInfoUpdateResponse.status(), equalTo(RestStatus.OK));

        expectThrows(ElasticsearchStatusException.class, () -> awaitActivateConnectorDraftFiltering(connectorId));
    }

    public void testUpdateConnectorLastSeen() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        ConnectorSyncInfo syncStats = ConnectorTestUtils.getRandomConnectorSyncInfo();

        UpdateConnectorLastSyncStatsAction.Request lastSyncStats = new UpdateConnectorLastSyncStatsAction.Request.Builder().setConnectorId(
            connectorId
        ).setSyncInfo(syncStats).build();

        DocWriteResponse updateResponse = awaitUpdateConnectorLastSyncStats(lastSyncStats);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);

        assertThat(syncStats, equalTo(indexedConnector.getSyncInfo()));
    }

    public void testUpdateConnectorLastSyncStats_withPartialUpdate() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        ConnectorSyncInfo syncStats = new ConnectorSyncInfo.Builder().setLastSyncError(randomAlphaOfLengthBetween(5, 10))
            .setLastIndexedDocumentCount(randomLong())
            .setLastDeletedDocumentCount(randomLong())
            .build();

        UpdateConnectorLastSyncStatsAction.Request lastSyncStats = new UpdateConnectorLastSyncStatsAction.Request.Builder().setConnectorId(
            connectorId
        ).setSyncInfo(syncStats).build();

        DocWriteResponse updateResponse = awaitUpdateConnectorLastSyncStats(lastSyncStats);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);

        // Check fields from the partial update of last sync stats
        assertThat(syncStats.getLastSyncError(), equalTo(indexedConnector.getSyncInfo().getLastSyncError()));
        assertThat(syncStats.getLastDeletedDocumentCount(), equalTo(indexedConnector.getSyncInfo().getLastDeletedDocumentCount()));
        assertThat(syncStats.getLastIndexedDocumentCount(), equalTo(indexedConnector.getSyncInfo().getLastIndexedDocumentCount()));

        ConnectorSyncInfo nextSyncStats = new ConnectorSyncInfo.Builder().setLastIndexedDocumentCount(randomLong()).build();

        lastSyncStats = new UpdateConnectorLastSyncStatsAction.Request.Builder().setConnectorId(connectorId)
            .setSyncInfo(nextSyncStats)
            .build();

        updateResponse = awaitUpdateConnectorLastSyncStats(lastSyncStats);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        indexedConnector = awaitGetConnector(connectorId);

        // Check fields from the partial update of last sync stats
        assertThat(nextSyncStats.getLastIndexedDocumentCount(), equalTo(indexedConnector.getSyncInfo().getLastIndexedDocumentCount()));

        // Check that other fields remained unchanged
        assertThat(syncStats.getLastSyncError(), equalTo(indexedConnector.getSyncInfo().getLastSyncError()));
        assertThat(syncStats.getLastDeletedDocumentCount(), equalTo(indexedConnector.getSyncInfo().getLastDeletedDocumentCount()));

    }

    public void testUpdateConnectorLastSyncStats_syncCursor() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        Map<String, String> syncCursor = randomMap(2, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4)));

        UpdateConnectorLastSyncStatsAction.Request lastSyncStats = new UpdateConnectorLastSyncStatsAction.Request.Builder().setConnectorId(
            connectorId
        ).setSyncInfo(new ConnectorSyncInfo.Builder().build()).setSyncCursor(syncCursor).build();

        DocWriteResponse updateResponse = awaitUpdateConnectorLastSyncStats(lastSyncStats);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        // Check sync_cursor got updated
        assertThat(syncCursor, equalTo(indexedConnector.getSyncCursor()));
    }

    public void testUpdateConnectorScheduling() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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

    public void testUpdateConnectorScheduling_OnlyFullSchedule() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        // Update scheduling for full, incremental and access_control
        ConnectorScheduling initialScheduling = ConnectorTestUtils.getRandomConnectorScheduling();
        UpdateConnectorSchedulingAction.Request updateSchedulingRequest = new UpdateConnectorSchedulingAction.Request(
            connectorId,
            initialScheduling
        );
        DocWriteResponse updateResponse = awaitUpdateConnectorScheduling(updateSchedulingRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        // Update full scheduling only
        ConnectorScheduling.ScheduleConfig fullSyncSchedule = new ConnectorScheduling.ScheduleConfig.Builder().setEnabled(randomBoolean())
            .setInterval(getRandomCronExpression())
            .build();

        UpdateConnectorSchedulingAction.Request updateSchedulingRequestWithFullSchedule = new UpdateConnectorSchedulingAction.Request(
            connectorId,
            new ConnectorScheduling.Builder().setFull(fullSyncSchedule).build()
        );

        updateResponse = awaitUpdateConnectorScheduling(updateSchedulingRequestWithFullSchedule);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        // Assert that full schedule is updated
        assertThat(fullSyncSchedule, equalTo(indexedConnector.getScheduling().getFull()));
        // Assert that other schedules stay unchanged
        assertThat(initialScheduling.getAccessControl(), equalTo(indexedConnector.getScheduling().getAccessControl()));
        assertThat(initialScheduling.getIncremental(), equalTo(indexedConnector.getScheduling().getIncremental()));
    }

    public void testUpdateConnectorIndexName_ForSelfManagedConnector() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomSelfManagedConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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

    public void testUpdateConnectorIndexName_ForSelfManagedConnector_WithTheSameIndexName() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomSelfManagedConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        UpdateConnectorIndexNameAction.Request updateIndexNameRequest = new UpdateConnectorIndexNameAction.Request(
            connectorId,
            connector.getIndexName()
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorIndexName(updateIndexNameRequest);
        assertThat(updateResponse.getResult(), equalTo(DocWriteResponse.Result.NOOP));
    }

    public void testUpdateConnectorIndexName_ForManagedConnector_WithIllegalIndexName() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomElasticManagedConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        UpdateConnectorIndexNameAction.Request updateIndexNameRequest = new UpdateConnectorIndexNameAction.Request(
            connectorId,
            "wrong-prefix-" + randomAlphaOfLengthBetween(3, 10)
        );

        expectThrows(ElasticsearchStatusException.class, () -> awaitUpdateConnectorIndexName(updateIndexNameRequest));
    }

    public void testUpdateConnectorIndexName_ForManagedConnector_WithPrefixedIndexName() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomElasticManagedConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        String newIndexName = MANAGED_CONNECTOR_INDEX_PREFIX + randomAlphaOfLengthBetween(3, 10);

        UpdateConnectorIndexNameAction.Request updateIndexNameRequest = new UpdateConnectorIndexNameAction.Request(
            connectorId,
            newIndexName
        );

        DocWriteResponse updateResponse = awaitUpdateConnectorIndexName(updateIndexNameRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(newIndexName, equalTo(indexedConnector.getIndexName()));
    }

    public void testUpdateConnectorServiceType() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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
        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));
        String error = randomAlphaOfLengthBetween(5, 15);

        DocWriteResponse updateResponse = awaitUpdateConnectorError(connectorId, error);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(indexedConnector.getError(), equalTo(error));
        assertThat(indexedConnector.getStatus(), equalTo(ConnectorStatus.ERROR));
    }

    public void testUpdateConnectorError_resetWithNull() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        DocWriteResponse updateResponse = awaitUpdateConnectorError(connectorId, null);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertNull(indexedConnector.getError());
        assertThat(indexedConnector.getStatus(), equalTo(ConnectorStatus.CONNECTED));
    }

    public void testUpdateConnectorNameOrDescription() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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
        Connector connector = ConnectorTestUtils.getRandomConnectorWithDetachedIndex();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        boolean isNative = randomBoolean();

        UpdateConnectorNativeAction.Request updateNativeRequest = new UpdateConnectorNativeAction.Request(connectorId, isNative);

        DocWriteResponse updateResponse = awaitUpdateConnectorNative(updateNativeRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(isNative, equalTo(indexedConnector.isNative()));
    }

    public void testUpdateConnectorNativeTrue_WhenIllegalIndexPrefix() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnectorWithAttachedIndex("wrong-prefix-" + randomAlphaOfLength(10));
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        boolean isNative = true;

        UpdateConnectorNativeAction.Request updateNativeRequest = new UpdateConnectorNativeAction.Request(connectorId, isNative);

        expectThrows(ElasticsearchStatusException.class, () -> awaitUpdateConnectorNative(updateNativeRequest));
    }

    public void testUpdateConnectorNativeTrue_WithCorrectIndexPrefix() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnectorWithAttachedIndex(
            MANAGED_CONNECTOR_INDEX_PREFIX + randomAlphaOfLength(10)
        );
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
        assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));

        boolean isNative = true;

        UpdateConnectorNativeAction.Request updateNativeRequest = new UpdateConnectorNativeAction.Request(connectorId, isNative);
        DocWriteResponse updateResponse = awaitUpdateConnectorNative(updateNativeRequest);
        assertThat(updateResponse.status(), equalTo(RestStatus.OK));

        Connector indexedConnector = awaitGetConnector(connectorId);
        assertThat(isNative, equalTo(indexedConnector.isNative()));
    }

    public void testUpdateConnectorStatus() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();

        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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

        awaitCreateConnector(connectorId, connector);
        Connector indexedConnector = awaitGetConnector(connectorId);

        ConnectorStatus newInvalidStatus = ConnectorTestUtils.getRandomInvalidConnectorNextStatus(indexedConnector.getStatus());

        UpdateConnectorStatusAction.Request updateStatusRequest = new UpdateConnectorStatusAction.Request(connectorId, newInvalidStatus);

        expectThrows(ElasticsearchStatusException.class, () -> awaitUpdateConnectorStatus(updateStatusRequest));
    }

    public void testUpdateConnectorApiKeyIdOrApiKeySecretId() throws Exception {
        Connector connector = ConnectorTestUtils.getRandomConnector();
        String connectorId = randomUUID();
        ConnectorCreateActionResponse resp = awaitCreateConnector(connectorId, connector);
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

    private DocWriteResponse awaitDeleteConnector(String connectorId, boolean hardDelete, boolean deleteConnectorSyncJobs)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DocWriteResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.deleteConnector(connectorId, hardDelete, deleteConnectorSyncJobs, new ActionListener<>() {
            @Override
            public void onResponse(DocWriteResponse deleteResponse) {
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

    private ConnectorCreateActionResponse awaitCreateConnector(String connectorId, Connector connector) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ConnectorCreateActionResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.createConnector(
            connectorId,
            connector.getDescription(),
            connector.getIndexName(),
            connector.isNative(),
            connector.getLanguage(),
            connector.getName(),
            connector.getServiceType(),
            new ActionListener<>() {
                @Override
                public void onResponse(ConnectorCreateActionResponse createResponse) {
                    resp.set(createResponse);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    exc.set(e);
                    latch.countDown();
                }
            }
        );
        assertTrue("Timeout waiting for create connector request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from create connector request", resp.get());
        return resp.get();
    }

    private Connector awaitGetConnectorIncludeDeleted(String connectorId) throws Exception {
        return awaitGetConnector(connectorId, true);
    }

    private Connector awaitGetConnector(String connectorId) throws Exception {
        return awaitGetConnector(connectorId, false);
    }

    private Connector awaitGetConnector(String connectorId, Boolean isDeleted) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Connector> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.getConnector(connectorId, isDeleted, new ActionListener<>() {
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
        return awaitListConnector(from, size, indexNames, names, serviceTypes, searchQuery, false);
    }

    private ConnectorIndexService.ConnectorResult awaitListConnector(
        int from,
        int size,
        List<String> indexNames,
        List<String> names,
        List<String> serviceTypes,
        String searchQuery,
        Boolean isDeleted
    ) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ConnectorIndexService.ConnectorResult> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.listConnectors(from, size, indexNames, names, serviceTypes, searchQuery, isDeleted, new ActionListener<>() {
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

    private UpdateResponse awaitUpdateConnectorFeatures(String connectorId, ConnectorFeatures features) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorFeatures(connectorId, features, new ActionListener<>() {
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

        assertTrue("Timeout waiting for update features request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update features request", resp.get());
        return resp.get();
    }

    private UpdateResponse awaitUpdateConnectorFiltering(String connectorId, List<ConnectorFiltering> filtering) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorFiltering(connectorId, filtering, new ActionListener<>() {

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

    private UpdateResponse awaitUpdateConnectorDraftFilteringValidation(String connectorId, FilteringValidationInfo validationInfo)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorDraftFilteringValidation(connectorId, validationInfo, new ActionListener<>() {
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

        assertTrue("Timeout waiting for update filtering validation request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from update filtering validation request", resp.get());
        return resp.get();
    }

    private UpdateResponse awaitActivateConnectorDraftFiltering(String connectorId) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.activateConnectorDraftFiltering(connectorId, new ActionListener<>() {
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

        assertTrue("Timeout waiting for activate draft filtering request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from activate draft filtering request", resp.get());
        return resp.get();
    }

    private UpdateResponse awaitUpdateConnectorFilteringDraft(
        String connectorId,
        FilteringAdvancedSnippet advancedSnippet,
        List<FilteringRule> rules
    ) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorFilteringDraft(connectorId, advancedSnippet, rules, new ActionListener<>() {
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

    private UpdateResponse awaitUpdateConnectorError(String connectorId, String error) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<UpdateResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        connectorIndexService.updateConnectorError(connectorId, error, new ActionListener<>() {
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
