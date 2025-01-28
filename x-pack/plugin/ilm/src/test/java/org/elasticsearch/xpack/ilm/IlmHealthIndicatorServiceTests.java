/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.xpack.core.ilm.OperationMode.RUNNING;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPED;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPING;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.NAME;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.RULES_BY_ACTION_CONFIG;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.STAGNATING_ACTION_DEFINITIONS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class IlmHealthIndicatorServiceTests extends ESTestCase {

    public void testIsGreenWhenRunningAndPoliciesConfiguredAndNoStagnatingIndices() {
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(createIlmPolicy(), RUNNING));
        var stagnatingIndicesFinder = mockedStagnatingIndicesFinder(List.of());
        var service = createIlmHealthIndicatorService(clusterState, stagnatingIndicesFinder);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    "Index Lifecycle Management is running",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", RUNNING, "policies", 1, "stagnating_indices", 0)),
                    List.of(),
                    List.of()
                )
            )
        );
        verify(stagnatingIndicesFinder, times(1)).find();
    }

    public void testIsYellowIfThereIsOneStagnatingIndices() throws IOException {
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(createIlmPolicy(), RUNNING));
        var action = randomAction();
        var policyName = randomAlphaOfLength(10);
        var indexName = randomAlphaOfLength(10);
        var stagnatingIndicesFinder = mockedStagnatingIndicesFinder(List.of(indexMetadata(indexName, policyName, action)));
        var service = createIlmHealthIndicatorService(clusterState, stagnatingIndicesFinder);

        var indicatorResult = service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(indicatorResult.name(), NAME);
        assertEquals(indicatorResult.status(), YELLOW);
        assertEquals(indicatorResult.symptom(), "An index has stayed on the same action longer than expected.");
        var expectedILMSummary = new HashMap<>();
        // Uncomment next line after https://github.com/elastic/elasticsearch/issues/96705 is fixed
        // expectedILMSummary.put("downsample", 0);
        expectedILMSummary.put("allocate", 0);
        expectedILMSummary.put("shrink", 0);
        expectedILMSummary.put("searchable_snapshot", 0);
        expectedILMSummary.put("rollover", 0);
        expectedILMSummary.put("forcemerge", 0);
        expectedILMSummary.put("delete", 0);
        expectedILMSummary.put("migrate", 0);
        expectedILMSummary.put(action, 1);

        assertEquals(
            xContentToMap(indicatorResult.details()),
            Map.of(
                "policies",
                1,
                "ilm_status",
                RUNNING.toString(),
                "stagnating_indices",
                1,
                "stagnating_indices_per_action",
                expectedILMSummary
            )
        );
        assertThat(indicatorResult.impacts(), hasSize(1));
        assertThat(
            indicatorResult.impacts().get(0),
            equalTo(
                new HealthIndicatorImpact(
                    NAME,
                    IlmHealthIndicatorService.STAGNATING_INDEX_IMPACT_ID,
                    3,
                    "Automatic index lifecycle and data retention management cannot make progress on one or more indices. "
                        + "The performance and stability of the indices and/or the cluster could be impacted.",
                    List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                )
            )
        );
        assertThat(indicatorResult.diagnosisList(), hasSize(1));
        assertEquals(indicatorResult.diagnosisList().get(0).definition(), STAGNATING_ACTION_DEFINITIONS.get(action));

        var affectedResources = indicatorResult.diagnosisList().get(0).affectedResources();
        assertThat(affectedResources, hasSize(2));
        assertEquals(affectedResources.get(0).getType(), Diagnosis.Resource.Type.ILM_POLICY);
        assertThat(affectedResources.get(0).getValues(), hasSize(1));
        assertThat(affectedResources.get(0).getValues(), containsInAnyOrder(policyName));
        assertThat(affectedResources.get(1).getValues(), hasSize(1));
        assertEquals(affectedResources.get(1).getType(), Diagnosis.Resource.Type.INDEX);
        assertThat(affectedResources.get(1).getValues(), containsInAnyOrder(indexName));

        verify(stagnatingIndicesFinder, times(1)).find();
    }

    private static String randomAction() {
        return randomFrom(RULES_BY_ACTION_CONFIG.keySet());
    }

    private static IndexMetadata indexMetadata(String indexName, String policyName, String action) {
        return IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, Map.of("action", action))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
    }

    public void testIsYellowWhenNotRunningAndPoliciesConfigured() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(createIlmPolicy(), status));
        var stagnatingIndicesFinder = mockedStagnatingIndicesFinder(List.of());
        var service = createIlmHealthIndicatorService(clusterState, stagnatingIndicesFinder);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Index Lifecycle Management is not running",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", status, "policies", 1, "stagnating_indices", 0)),
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            IlmHealthIndicatorService.AUTOMATION_DISABLED_IMPACT_ID,
                            3,
                            "Automatic index lifecycle and data retention management is disabled. The performance and stability of the "
                                + "cluster could be impacted.",
                            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                        )
                    ),
                    List.of(IlmHealthIndicatorService.ILM_NOT_RUNNING)
                )
            )
        );
        verifyNoInteractions(stagnatingIndicesFinder);
    }

    public void testIsGreenWhenNotRunningAndNoPolicies() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(Map.of(), status));
        var stagnatingIndicesFinder = mockedStagnatingIndicesFinder(List.of());
        var service = createIlmHealthIndicatorService(clusterState, stagnatingIndicesFinder);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    "No Index Lifecycle Management policies configured",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", status, "policies", 0, "stagnating_indices", 0)),
                    List.of(),
                    List.of()
                )
            )
        );
        verifyNoInteractions(stagnatingIndicesFinder);
    }

    public void testIsGreenWhenNoMetadata() {
        var clusterState = createClusterStateWith(null);
        var stagnatingIndicesFinder = mockedStagnatingIndicesFinder(List.of());
        var service = createIlmHealthIndicatorService(clusterState, stagnatingIndicesFinder);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    "No Index Lifecycle Management policies configured",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", RUNNING, "policies", 0, "stagnating_indices", 0)),
                    List.of(),
                    List.of()
                )
            )
        );
        verifyNoInteractions(stagnatingIndicesFinder);
    }

    public void testSkippingFieldsWhenVerboseIsFalse() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(createIlmPolicy(), status));
        var stagnatingIndicesFinder = mockedStagnatingIndicesFinder(List.of());
        var service = createIlmHealthIndicatorService(clusterState, stagnatingIndicesFinder);

        assertThat(
            service.calculate(false, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Index Lifecycle Management is not running",
                    HealthIndicatorDetails.EMPTY,
                    List.of(
                        new HealthIndicatorImpact(
                            NAME,
                            IlmHealthIndicatorService.AUTOMATION_DISABLED_IMPACT_ID,
                            3,
                            "Automatic index lifecycle and data retention management is disabled. The performance and stability of the "
                                + "cluster could be impacted.",
                            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                        )
                    ),
                    List.of()
                )
            )
        );
    }

    // We expose the indicator name and the diagnoses in the x-pack usage API. In order to index them properly in a telemetry index
    // they need to be declared in the health-api-indexer.edn in the telemetry repository.
    public void testMappedFieldsForTelemetry() {
        assertThat(IlmHealthIndicatorService.NAME, equalTo("ilm"));
        assertThat(
            IlmHealthIndicatorService.ILM_NOT_RUNNING.definition().getUniqueId(),
            equalTo("elasticsearch:health:ilm:diagnosis:ilm_disabled")
        );
        var definitionIds = STAGNATING_ACTION_DEFINITIONS.values().stream().map(Diagnosis.Definition::getUniqueId).toList();

        assertThat(
            definitionIds,
            containsInAnyOrder(
                "elasticsearch:health:ilm:diagnosis:stagnating_action:rollover",
                "elasticsearch:health:ilm:diagnosis:stagnating_action:migrate",
                "elasticsearch:health:ilm:diagnosis:stagnating_action:searchable_snapshot",
                "elasticsearch:health:ilm:diagnosis:stagnating_action:delete",
                "elasticsearch:health:ilm:diagnosis:stagnating_action:shrink",
                "elasticsearch:health:ilm:diagnosis:stagnating_action:forcemerge",
                "elasticsearch:health:ilm:diagnosis:stagnating_action:allocate"
                // "elasticsearch:health:ilm:diagnosis:stagnating_action:downsample"
            )
        );
    }

    private static ClusterState createClusterStateWith(IndexLifecycleMetadata metadata) {
        var builder = new ClusterState.Builder(new ClusterName("test-cluster"));
        if (metadata != null) {
            builder.metadata(new Metadata.Builder().putCustom(IndexLifecycleMetadata.TYPE, metadata));
        }
        return builder.build();
    }

    private static Map<String, LifecyclePolicyMetadata> createIlmPolicy() {
        return Map.of(
            "test-policy",
            new LifecyclePolicyMetadata(new LifecyclePolicy("test-policy", Map.of()), Map.of(), 1L, System.currentTimeMillis())
        );
    }

    private static IlmHealthIndicatorService createIlmHealthIndicatorService(
        ClusterState clusterState,
        IlmHealthIndicatorService.StagnatingIndicesFinder stagnatingIndicesFinder
    ) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                Settings.EMPTY,
                Set.of(
                    IlmHealthIndicatorService.MAX_TIME_ON_ACTION_SETTING,
                    IlmHealthIndicatorService.MAX_TIME_ON_STEP_SETTING,
                    IlmHealthIndicatorService.MAX_RETRIES_PER_STEP_SETTING
                )
            )
        );

        return new IlmHealthIndicatorService(clusterService, stagnatingIndicesFinder);
    }

    private static IlmHealthIndicatorService.StagnatingIndicesFinder mockedStagnatingIndicesFinder(List<IndexMetadata> states) {
        var finder = mock(IlmHealthIndicatorService.StagnatingIndicesFinder.class);
        when(finder.find()).thenReturn(states);
        return finder;
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }
}
