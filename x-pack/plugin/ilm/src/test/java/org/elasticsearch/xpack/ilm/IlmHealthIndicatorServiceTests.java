/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.xpack.core.ilm.OperationMode.RUNNING;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPED;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPING;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.ACTION_STUCK_DEFINITIONS;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.NAME;
import static org.elasticsearch.xpack.ilm.IlmHealthIndicatorService.RULES_BY_ACTION_CONFIG;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class IlmHealthIndicatorServiceTests extends ESTestCase {

    public void testIsGreenWhenRunningAndPoliciesConfiguredAndNoStuckIndices() {
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(createIlmPolicy(), RUNNING));
        var stuckIndicesFinder = mockedStuckIndicesFinder(List.of());
        var service = createIlmHealthIndicatorService(clusterState, stuckIndicesFinder);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    "Index Lifecycle Management is running",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", RUNNING, "policies", 1, "stuck_indices", 0)),
                    List.of(),
                    List.of()
                )
            )
        );
        verify(stuckIndicesFinder, times(1)).find();
    }

    public void testIsYellowIfThereIsOneStuckIndicesAndDetailsEmptyIfNoVerbose() throws IOException {
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(createIlmPolicy(), RUNNING));
        var action = randomAction();
        var policyName = randomAlphaOfLength(10);
        var indexName = randomAlphaOfLength(10);
        var stuckIndicesFinder = mockedStuckIndicesFinder(List.of(indexIlmState(indexName, policyName, action)));
        var service = createIlmHealthIndicatorService(clusterState, stuckIndicesFinder);

        var indicatorResult = service.calculate(false, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(indicatorResult.name(), NAME);
        assertEquals(indicatorResult.status(), YELLOW);
        assertEquals(indicatorResult.symptom(), "An index has been stuck on the same action longer than expected.");
        assertEquals(xContentToMap(indicatorResult.details()), Map.of());
        assertThat(indicatorResult.impacts(), hasSize(1));
        assertThat(
            indicatorResult.impacts().get(0),
            equalTo(
                new HealthIndicatorImpact(
                    NAME,
                    IlmHealthIndicatorService.INDEX_STUCK_IMPACT_ID,
                    3,
                    "Some indices have been longer than expected on the same Index Lifecycle Management action.",
                    List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                )
            )
        );
        assertThat(indicatorResult.diagnosisList(), hasSize(1));
        assertEquals(indicatorResult.diagnosisList().get(0).definition(), ACTION_STUCK_DEFINITIONS.get(action).apply(policyName));
        assertEquals(
            indicatorResult.diagnosisList().get(0).affectedResources(),
            List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of(indexName)))
        );
        verify(stuckIndicesFinder, times(1)).find();
    }

    public void testIsYellowIfThereIsOneStuckIndices() throws IOException {
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(createIlmPolicy(), RUNNING));
        var action = randomAction();
        var policyName = randomAlphaOfLength(10);
        var indexName = randomAlphaOfLength(10);
        var stuckIndicesFinder = mockedStuckIndicesFinder(List.of(indexIlmState(indexName, policyName, action)));
        var service = createIlmHealthIndicatorService(clusterState, stuckIndicesFinder);

        var indicatorResult = service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);

        assertEquals(indicatorResult.name(), NAME);
        assertEquals(indicatorResult.status(), YELLOW);
        assertEquals(indicatorResult.symptom(), "An index has been stuck on the same action longer than expected.");
        var expectedILMSummary = new HashMap<>();
        expectedILMSummary.put("downsample", 0);
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
            Map.of("policies", 1, "ilm_status", RUNNING.toString(), "stuck_indices", 1, "stuck_indices_per_action", expectedILMSummary)
        );
        assertThat(indicatorResult.impacts(), hasSize(1));
        assertThat(
            indicatorResult.impacts().get(0),
            equalTo(
                new HealthIndicatorImpact(
                    NAME,
                    IlmHealthIndicatorService.INDEX_STUCK_IMPACT_ID,
                    3,
                    "Some indices have been longer than expected on the same Index Lifecycle Management action.",
                    List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
                )
            )
        );
        assertThat(indicatorResult.diagnosisList(), hasSize(1));
        assertEquals(indicatorResult.diagnosisList().get(0).definition(), ACTION_STUCK_DEFINITIONS.get(action).apply(policyName));
        assertEquals(
            indicatorResult.diagnosisList().get(0).affectedResources(),
            List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of(indexName)))
        );
        verify(stuckIndicesFinder, times(1)).find();
    }

    private static String randomAction() {
        return randomFrom(RULES_BY_ACTION_CONFIG.keySet());
    }

    private static IlmHealthIndicatorService.IndexIlmState indexIlmState(String indexName, String policyName, String action) {
        return new IlmHealthIndicatorService.IndexIlmState(
            indexName,
            policyName,
            randomAlphaOfLength(10),
            action,
            TimeValue.parseTimeValue(randomTimeValue(), "setting.name"),
            randomAlphaOfLength(10),
            TimeValue.parseTimeValue(randomTimeValue(), "setting.name"),
            randomInt()
        );
    }

    public void testIsYellowWhenNotRunningAndPoliciesConfigured() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(createIlmPolicy(), status));
        var stuckIndicesFinder = mockedStuckIndicesFinder(List.of());
        var service = createIlmHealthIndicatorService(clusterState, stuckIndicesFinder);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    YELLOW,
                    "Index Lifecycle Management is not running",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", status, "policies", 1, "stuck_indices", 0)),
                    Collections.singletonList(
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
        verifyNoInteractions(stuckIndicesFinder);
    }

    public void testIsGreenWhenNotRunningAndNoPolicies() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new IndexLifecycleMetadata(Map.of(), status));
        var stuckIndicesFinder = mockedStuckIndicesFinder(List.of());
        var service = createIlmHealthIndicatorService(clusterState, stuckIndicesFinder);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    "No Index Lifecycle Management policies configured",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", status, "policies", 0, "stuck_indices", 0)),
                    List.of(),
                    List.of()
                )
            )
        );
        verifyNoInteractions(stuckIndicesFinder);
    }

    public void testIsGreenWhenNoMetadata() {
        var clusterState = createClusterStateWith(null);
        var stuckIndicesFinder = mockedStuckIndicesFinder(List.of());
        var service = createIlmHealthIndicatorService(clusterState, stuckIndicesFinder);

        assertThat(
            service.calculate(true, HealthInfo.EMPTY_HEALTH_INFO),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    GREEN,
                    "No Index Lifecycle Management policies configured",
                    new SimpleHealthIndicatorDetails(Map.of("ilm_status", RUNNING, "policies", 0, "stuck_indices", 0)),
                    List.of(),
                    List.of()
                )
            )
        );
        verifyNoInteractions(stuckIndicesFinder);
    }

    // We expose the indicator name and the diagnoses in the x-pack usage API. In order to index them properly in a telemetry index
    // they need to be declared in the health-api-indexer.edn in the telemetry repository.
    public void testMappedFieldsForTelemetry() {
        assertThat(IlmHealthIndicatorService.NAME, equalTo("ilm"));
        assertThat(
            IlmHealthIndicatorService.ILM_NOT_RUNNING.definition().getUniqueId(),
            equalTo("elasticsearch:health:ilm:diagnosis:ilm_disabled")
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
        IlmHealthIndicatorService.StuckIndicesFinder stuckIndicesFinder
    ) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        return new IlmHealthIndicatorService(clusterService, stuckIndicesFinder);
    }

    private static IlmHealthIndicatorService.StuckIndicesFinder mockedStuckIndicesFinder(
        List<IlmHealthIndicatorService.IndexIlmState> states
    ) {
        var stuckIndicesFinder = mock(IlmHealthIndicatorService.StuckIndicesFinder.class);
        when(stuckIndicesFinder.find()).thenReturn(states);
        return stuckIndicesFinder;
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }
}
