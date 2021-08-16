/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersResponse;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.AllocationRoutedStep;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createIndexWithSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getOnlyIndexSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MigrateToDataTiersIT extends ESRestTestCase {
    private static final Logger logger = LogManager.getLogger(MigrateToDataTiersIT.class);

    private String index;
    private String policy;
    private String alias;

    @Before
    public void refreshIndexAndStartILM() throws IOException {
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        alias = "alias-" + randomAlphaOfLength(5);
        assertOK(client().performRequest(new Request("POST", "_ilm/start")));
    }

    @AfterClass
    public static void restartIlm() throws IOException {
        // some tests might stop ILM in order to perform the migration to data tiers, let's restart it
        assertOK(client().performRequest(new Request("POST", "_ilm/start")));
    }

    public void testAPIFailsIfILMIsNotStopped() throws IOException {
        Request migrateRequest = new Request("POST", "_ilm/migrate_to_data_tiers");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(migrateRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(RestStatus.INTERNAL_SERVER_ERROR.getStatus()));
        assertThat(e.getMessage(), containsString("stop ILM before migrating to data tiers, current state is [RUNNING]"));
    }

    @SuppressWarnings("unchecked")
    public void testMigrateToDataTiersAction() throws Exception {
        // creating a legacy template to use in the migrate API
        String templateName = randomAlphaOfLengthBetween(10, 15).toLowerCase(Locale.ROOT);
        createLegacyTemplate(templateName);

        // let's create a policy that'll need migrating, with a long `min_age` for the cold phase such that managed indices stop in
        // Warm/Complete/Complete - this will ensure the migration will have to update the cached phase for these indices
        Map<String, LifecycleAction> hotActions = new HashMap<>();
        hotActions.put(SetPriorityAction.NAME, new SetPriorityAction(100));
        Map<String, LifecycleAction> warmActions = new HashMap<>();
        warmActions.put(SetPriorityAction.NAME, new SetPriorityAction(50));
        warmActions.put(ForceMergeAction.NAME, new ForceMergeAction(1, null));
        warmActions.put(AllocateAction.NAME, new AllocateAction(null, null, singletonMap("data", "warm"), null, null));
        warmActions.put(ShrinkAction.NAME, new ShrinkAction(1, null));
        Map<String, LifecycleAction> coldActions = new HashMap<>();
        coldActions.put(SetPriorityAction.NAME, new SetPriorityAction(0));
        coldActions.put(AllocateAction.NAME, new AllocateAction(0, null, null, null, singletonMap("data", "cold")));

        createPolicy(client(), policy,
            new Phase("hot", TimeValue.ZERO, hotActions),
            new Phase("warm", TimeValue.ZERO, warmActions),
            new Phase("cold", TimeValue.timeValueDays(100), coldActions),
            null,
            new Phase("delete", TimeValue.ZERO, singletonMap(DeleteAction.NAME, new DeleteAction()))
        );

        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy)
            .putNull(DataTierAllocationDecider.INDEX_ROUTING_PREFER)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
        );

        // wait for the index to advance to the warm phase
        assertBusy(() ->
            assertThat(getStepKeyForIndex(client(), index).getPhase(), equalTo("warm")), 30, TimeUnit.SECONDS);
        // let's wait for this index to have received the `require.data` configuration from the warm phase/allocate action
        assertBusy(() ->
            assertThat(getStepKeyForIndex(client(), index).getName(), equalTo(AllocationRoutedStep.NAME)), 30, TimeUnit.SECONDS);

        // let's also have a policy that doesn't need migrating
        String rolloverOnlyPolicyName = "rollover-policy";
        createNewSingletonPolicy(client(), rolloverOnlyPolicyName, "hot", new RolloverAction(null, null, null, 1L));

        String rolloverIndexPrefix = "rolloverpolicytest_index";
        for (int i = 1; i < randomIntBetween(2, 5); i++) {
            // assign the rollover-only policy to a few other indices - these indices and the rollover-only policy should not be migrated
            // in any way
            createIndexWithSettings(client(), rolloverIndexPrefix + "-00000" + i, alias + i, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .putNull(DataTierAllocationDecider.INDEX_ROUTING_PREFER)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias + i)
            );
        }

        // let's stop ILM so we can perform the migration
        client().performRequest(new Request("POST", "_ilm/stop"));
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "_ilm/status"));
            assertThat(EntityUtils.toString(response.getEntity()), containsString(OperationMode.STOPPED.toString()));
        });

        String indexWithDataWarmRouting = "indexwithdatawarmrouting";
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "data", "warm");
        createIndex(indexWithDataWarmRouting, settings.build());

        Request migrateRequest = new Request("POST", "_ilm/migrate_to_data_tiers");
        migrateRequest.setJsonEntity(
            "{\"legacy_template_to_delete\": \"" + templateName + "\", \"node_attribute\": \"data\"}"
        );
        Response migrateDeploymentResponse = client().performRequest(migrateRequest);
        assertOK(migrateDeploymentResponse);

        Map<String, Object> migrateResponseAsMap = responseAsMap(migrateDeploymentResponse);
        assertThat((ArrayList<String>) migrateResponseAsMap.get(MigrateToDataTiersResponse.MIGRATED_ILM_POLICIES.getPreferredName()),
            containsInAnyOrder(policy));
        assertThat((ArrayList<String>) migrateResponseAsMap.get(MigrateToDataTiersResponse.MIGRATED_INDICES.getPreferredName()),
            containsInAnyOrder(index, indexWithDataWarmRouting));
        assertThat(migrateResponseAsMap.get(MigrateToDataTiersResponse.REMOVED_LEGACY_TEMPLATE.getPreferredName()),
            is(templateName));

        // let's verify the legacy template doesn't exist anymore
        Request getTemplateRequest = new Request("HEAD", "_template/" + templateName);
        assertThat(client().performRequest(getTemplateRequest).getStatusLine().getStatusCode(), is(RestStatus.NOT_FOUND.getStatus()));

        // let's assert the require.data:warm configuration the "indexWithDataWarmRouting" had was migrated to
        // _tier_preference:data_warm,data_hot
        Map<String, Object> indexSettings = getOnlyIndexSettings(client(), indexWithDataWarmRouting);
        assertThat(indexSettings.get(DataTierAllocationDecider.INDEX_ROUTING_PREFER), is("data_warm,data_hot"));

        // let's retrieve the migrated policy and check it was migrated correctly - namely the warm phase should not contain any allocate
        // action anymore and the cold phase should contain an allocate action that only configures the number of replicas
        Request getPolicy = new Request("GET", "/_ilm/policy/" + policy);
        Map<String, Object> policyAsMap = (Map<String, Object>) responseAsMap(client().performRequest(getPolicy)).get(policy);
        Map<String, Object> warmActionsMap = getActionsForPhase(policyAsMap, "warm");
        assertThat(warmActionsMap.size(), is(3));
        assertThat(warmActionsMap.get(AllocateAction.NAME), nullValue());
        Map<String, Object> coldActionsMap = getActionsForPhase(policyAsMap, "cold");
        assertThat(coldActionsMap.size(), is(2));
        assertThat(coldActionsMap.get(AllocateAction.NAME), notNullValue());
        Map<String, Object> coldAllocateActionMap = (Map<String, Object>) coldActionsMap.get(AllocateAction.NAME);
        assertThat((Map<String, Object>) coldAllocateActionMap.get("include"), is(anEmptyMap()));
        assertThat((Map<String, Object>) coldAllocateActionMap.get("require"), is(anEmptyMap()));
        assertThat((Map<String, Object>) coldAllocateActionMap.get("exclude"), is(anEmptyMap()));

        Request getClusterMetadataRequest = new Request("GET", "/_cluster/state/metadata/" + index);
        Response clusterMetadataResponse = client().performRequest(getClusterMetadataRequest);

        String cachedPhaseDefinition = getCachedPhaseDefAsMap(clusterMetadataResponse, index);
        // let's also verify the cached phase definition was updated - as the managed index was in the warm phase, which after migration
        // does not contain the allocate action anymore, the cached warm phase should not contain the allocate action either
        assertThat("the cached phase definition should reflect the migrated warm phase which must NOT contain an allocate action anymore",
            cachedPhaseDefinition, not(containsString(AllocateAction.NAME)));
        assertThat(cachedPhaseDefinition, containsString(ShrinkAction.NAME));
        assertThat(cachedPhaseDefinition, containsString(SetPriorityAction.NAME));
        assertThat(cachedPhaseDefinition, containsString(ForceMergeAction.NAME));
    }

    @SuppressWarnings("unchecked")
    public void testMigrationDryRun() throws Exception {
        String templateName = randomAlphaOfLengthBetween(10, 15).toLowerCase(Locale.ROOT);
        createLegacyTemplate(templateName);

        Map<String, LifecycleAction> hotActions = new HashMap<>();
        hotActions.put(SetPriorityAction.NAME, new SetPriorityAction(100));
        Map<String, LifecycleAction> warmActions = new HashMap<>();
        warmActions.put(SetPriorityAction.NAME, new SetPriorityAction(50));
        warmActions.put(ForceMergeAction.NAME, new ForceMergeAction(1, null));
        warmActions.put(AllocateAction.NAME, new AllocateAction(null, null, singletonMap("data", "warm"), null, null));
        warmActions.put(ShrinkAction.NAME, new ShrinkAction(1, null));
        Map<String, LifecycleAction> coldActions = new HashMap<>();
        coldActions.put(SetPriorityAction.NAME, new SetPriorityAction(0));
        coldActions.put(AllocateAction.NAME, new AllocateAction(0, null, null, null, singletonMap("data", "cold")));

        createPolicy(client(), policy,
            new Phase("hot", TimeValue.ZERO, hotActions),
            new Phase("warm", TimeValue.ZERO, warmActions),
            new Phase("cold", TimeValue.timeValueDays(100), coldActions),
            null,
            new Phase("delete", TimeValue.ZERO, singletonMap(DeleteAction.NAME, new DeleteAction()))
        );

        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy)
            .putNull(DataTierAllocationDecider.INDEX_ROUTING_PREFER)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
        );

        // wait for the index to advance to the warm phase
        assertBusy(() ->
            assertThat(getStepKeyForIndex(client(), index).getPhase(), equalTo("warm")), 30, TimeUnit.SECONDS);
        // let's wait for this index to have received the `require.data` configuration from the warm phase/allocate action
        assertBusy(() ->
            assertThat(getStepKeyForIndex(client(), index).getName(), equalTo(AllocationRoutedStep.NAME)), 30, TimeUnit.SECONDS);

        // let's stop ILM so we can simulate the migration
        client().performRequest(new Request("POST", "_ilm/stop"));
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "_ilm/status"));
            assertThat(EntityUtils.toString(response.getEntity()), containsString(OperationMode.STOPPED.toString()));
        });

        String indexWithDataWarmRouting = "indexwithdatawarmrouting";
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "data", "warm");
        createIndex(indexWithDataWarmRouting, settings.build());

        Request migrateRequest = new Request("POST", "_ilm/migrate_to_data_tiers");
        migrateRequest.addParameter("dry_run", "true");
        migrateRequest.setJsonEntity(
            "{\"legacy_template_to_delete\": \"" + templateName + "\", \"node_attribute\": \"data\"}"
        );
        Response migrateDeploymentResponse = client().performRequest(migrateRequest);
        assertOK(migrateDeploymentResponse);

        // response should contain the correct "to migrate" entities
        Map<String, Object> migrateResponseAsMap = responseAsMap(migrateDeploymentResponse);
        assertThat((ArrayList<String>) migrateResponseAsMap.get(MigrateToDataTiersResponse.MIGRATED_ILM_POLICIES.getPreferredName()),
            containsInAnyOrder(policy));
        assertThat((ArrayList<String>) migrateResponseAsMap.get(MigrateToDataTiersResponse.MIGRATED_INDICES.getPreferredName()),
            containsInAnyOrder(index, indexWithDataWarmRouting));
        assertThat(migrateResponseAsMap.get(MigrateToDataTiersResponse.REMOVED_LEGACY_TEMPLATE.getPreferredName()),
            is(templateName));

        // however the entities should NOT have been changed
        // the index template should still exist
        Request getTemplateRequest = new Request("HEAD", "_template/" + templateName);
        assertThat(client().performRequest(getTemplateRequest).getStatusLine().getStatusCode(), is(RestStatus.OK.getStatus()));

        // the index settings should not contain the _tier_preference
        Map<String, Object> indexSettings = getOnlyIndexSettings(client(), indexWithDataWarmRouting);
        assertThat(indexSettings.get(DataTierAllocationDecider.INDEX_ROUTING_PREFER), nullValue());

        // let's check the ILM policy was not migrated - ie. the warm phase still contains the allocate action
        Request getPolicy = new Request("GET", "/_ilm/policy/" + policy);
        Map<String, Object> policyAsMap = (Map<String, Object>) responseAsMap(client().performRequest(getPolicy)).get(policy);
        Map<String, Object> warmActionsMap = getActionsForPhase(policyAsMap, "warm");
        assertThat(warmActionsMap.size(), is(4));
        assertThat(warmActionsMap.get(AllocateAction.NAME), notNullValue());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getActionsForPhase(Map<String, Object> policyAsMap, String phase) {
        Map<String, Object> phases = (Map<String, Object>) ((Map<String, Object>) policyAsMap.get("policy")).get("phases");
        return (Map<String, Object>) ((Map<String, Object>) phases.get(phase)).get("actions");
    }

    @SuppressWarnings("unchecked")
    private String getCachedPhaseDefAsMap(Response clusterMetadataResponse, String indexName) throws IOException {
        Map<String, Object> clusterMetadataMap = responseAsMap(clusterMetadataResponse);
        Map<String, Object> metadata = (Map<String, Object>) clusterMetadataMap.get("metadata");
        Map<String, Object> indices = (Map<String, Object>) metadata.get("indices");
        Map<String, Object> indexMetadata = (Map<String, Object>) indices.get(indexName);
        Map<String, Object> ilmMetadata = (Map<String, Object>) indexMetadata.get("ilm");
        return (String) ilmMetadata.get("phase_definition");
    }

    private void createLegacyTemplate(String templateName) throws IOException {
        String indexPrefix = randomAlphaOfLengthBetween(5, 15).toLowerCase(Locale.ROOT);
        final StringEntity template = new StringEntity("{\n" +
            "  \"index_patterns\": \"" + indexPrefix + "*\",\n" +
            "  \"settings\": {\n" +
            "    \"index\": {\n" +
            "      \"lifecycle\": {\n" +
            "        \"name\": \"does_not_exist\",\n" +
            "        \"rollover_alias\": \"test_alias\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);
        Request templateRequest = new Request("PUT", "_template/" + templateName);
        templateRequest.setEntity(template);
        templateRequest.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        client().performRequest(templateRequest);
    }
}
