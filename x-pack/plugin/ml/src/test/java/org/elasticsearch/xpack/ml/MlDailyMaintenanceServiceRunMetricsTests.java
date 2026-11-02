/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlDailyMaintenanceServiceRunMetricsTests extends ESTestCase {

    private static final TimeValue RUN_INTERVAL = TimeValue.timeValueHours(24);

    private DeterministicTaskQueue taskQueue;
    private Client client;
    private ClusterService clusterService;
    private RecordingMeterRegistry meterRegistry;

    @Before
    public void setUpDeterministicService() {
        taskQueue = new DeterministicTaskQueue();
        ThreadPool threadPool = taskQueue.getThreadPool();
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState(false, false));
        meterRegistry = new RecordingMeterRegistry();
        succeedAllSteps();
    }

    public void testAllStepsSucceedShouldCountOneSuccessRun() {
        MlDailyMaintenanceService service = startedService(true);

        runNextMaintenance();

        assertThat(runCounts(), equalTo(Map.of("success", 1L)));
        service.stop();
    }

    public void testEarlierStepFailsWhileLaterStepsSucceedShouldCountFailureRun() {
        doAnswer(withException(new ElasticsearchException("get jobs failed"))).when(client)
            .execute(same(GetJobsAction.INSTANCE), any(), any());
        MlDailyMaintenanceService service = startedService(true);

        runNextMaintenance();

        assertThat(runCounts(), equalTo(Map.of("failure", 1L)));
        service.stop();
    }

    public void testLastStepFailsShouldCountFailureRun() {
        doAnswer(withException(new ElasticsearchException("delete expired data failed"))).when(client)
            .execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        MlDailyMaintenanceService service = startedService(true);

        runNextMaintenance();

        assertThat(runCounts(), equalTo(Map.of("failure", 1L)));
        service.stop();
    }

    public void testUpgradeModeShouldCountSkippedRun() {
        when(clusterService.state()).thenReturn(clusterState(true, false));
        MlDailyMaintenanceService service = startedService(true);

        runNextMaintenance();

        assertThat(runCounts(), equalTo(Map.of("skipped", 1L)));
        service.stop();
    }

    public void testResetModeShouldCountSkippedRun() {
        when(clusterService.state()).thenReturn(clusterState(false, true));
        MlDailyMaintenanceService service = startedService(true);

        runNextMaintenance();

        assertThat(runCounts(), equalTo(Map.of("skipped", 1L)));
        service.stop();
    }

    public void testAnomalyDetectionDisabledShouldCountSuccessRun() {
        MlDailyMaintenanceService service = startedService(false);

        runNextMaintenance();

        assertThat(runCounts(), equalTo(Map.of("success", 1L)));
        service.stop();
    }

    public void testRunsShouldAccumulatePerResult() {
        MlDailyMaintenanceService service = startedService(true);

        runNextMaintenance();
        runNextMaintenance();
        when(clusterService.state()).thenReturn(clusterState(true, false));
        runNextMaintenance();

        assertThat(runCounts(), equalTo(Map.of("success", 2L, "skipped", 1L)));
        service.stop();
    }

    public void testRunMeasurementShouldCarryOnlyResultAttribute() {
        MlDailyMaintenanceService service = startedService(true);

        runNextMaintenance();

        for (Measurement measurement : meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, runsMetric())) {
            assertThat(measurement.attributes().keySet(), equalTo(Set.of(MlDailyMaintenanceService.RUN_RESULT_ATTRIBUTE)));
        }
        service.stop();
    }

    public void testLastSuccessAgeShouldCountFromServiceStartUntilFirstSuccess() {
        MlDailyMaintenanceService service = startedService(true);

        advanceTo(TimeValue.timeValueHours(3).millis());

        assertThat(lastSuccessAgeSeconds(), equalTo(List.of(TimeValue.timeValueHours(3).seconds())));
        service.stop();
    }

    public void testLastSuccessAgeShouldResetOnSuccessfulRunAndGrowAfterIt() {
        MlDailyMaintenanceService service = startedService(true);

        runNextMaintenance();
        assertThat(lastSuccessAgeSeconds(), equalTo(List.of(0L)));

        advanceTo(RUN_INTERVAL.millis() + TimeValue.timeValueMinutes(90).millis());
        assertThat(lastSuccessAgeSeconds(), equalTo(List.of(TimeValue.timeValueMinutes(90).seconds())));
        service.stop();
    }

    public void testFailedRunShouldNotResetLastSuccessAge() {
        MlDailyMaintenanceService service = startedService(true);
        doAnswer(withException(new ElasticsearchException("get jobs failed"))).when(client)
            .execute(same(GetJobsAction.INSTANCE), any(), any());

        runNextMaintenance();

        assertThat(lastSuccessAgeSeconds(), equalTo(List.of(RUN_INTERVAL.seconds())));
        service.stop();
    }

    public void testSkippedRunShouldNotResetLastSuccessAge() {
        MlDailyMaintenanceService service = startedService(true);
        when(clusterService.state()).thenReturn(clusterState(true, false));

        runNextMaintenance();

        assertThat(lastSuccessAgeSeconds(), equalTo(List.of(RUN_INTERVAL.seconds())));
        service.stop();
    }

    public void testStoppedServiceShouldReportNoLastSuccessAge() {
        MlDailyMaintenanceService service = startedService(true);
        advanceTo(TimeValue.timeValueHours(1).millis());
        assertThat(lastSuccessAgeSeconds().size(), equalTo(1));

        service.stop();

        assertThat(lastSuccessAgeSeconds(), empty());
    }

    public void testRestartedServiceShouldCountAgeFromRestart() {
        MlDailyMaintenanceService service = startedService(true);
        advanceTo(TimeValue.timeValueHours(5).millis());
        service.stop();

        service.start();
        advanceTo(TimeValue.timeValueHours(5).millis() + TimeValue.timeValueMinutes(10).millis());

        assertThat(lastSuccessAgeSeconds(), equalTo(List.of(TimeValue.timeValueMinutes(10).seconds())));
        service.stop();
    }

    private MlDailyMaintenanceService startedService(boolean isAnomalyDetectionEnabled) {
        MlDailyMaintenanceService service = new MlDailyMaintenanceService(
            Settings.EMPTY,
            taskQueue.getThreadPool(),
            client,
            clusterService,
            mock(AnomalyDetectionAuditor.class),
            mock(MlAssignmentNotifier.class),
            () -> RUN_INTERVAL,
            TestIndexNameExpressionResolver.newInstance(),
            isAnomalyDetectionEnabled,
            true,
            true,
            true,
            meterRegistry
        );
        service.start();
        return service;
    }

    private void succeedAllSteps() {
        doAnswer(withResponse(new DeleteExpiredDataAction.Response(true))).when(client)
            .execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        doAnswer(withResponse(new GetJobsAction.Response(new QueryPage<>(Collections.emptyList(), 0, new ParseField(""))))).when(client)
            .execute(same(GetJobsAction.INSTANCE), any(), any());
    }

    /** Moves the deterministic clock to the next scheduled maintenance time and runs it. */
    private void runNextMaintenance() {
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
    }

    private void advanceTo(long millis) {
        taskQueue.scheduleAtAndRunUpTo(millis, () -> {});
    }

    private static String runsMetric() {
        return MlDailyMaintenanceService.RUNS_METRIC;
    }

    private Map<String, Long> runCounts() {
        return meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, runsMetric())
            .stream()
            .collect(
                Collectors.groupingBy(
                    m -> (String) m.attributes().get(MlDailyMaintenanceService.RUN_RESULT_ATTRIBUTE),
                    Collectors.summingLong(Measurement::getLong)
                )
            );
    }

    private List<Long> lastSuccessAgeSeconds() {
        meterRegistry.getRecorder().resetCalls();
        meterRegistry.getRecorder().collect();
        return meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, MlDailyMaintenanceService.LAST_SUCCESS_AGE_METRIC)
            .stream()
            .map(Measurement::getLong)
            .toList();
    }

    private static ClusterState clusterState(boolean isUpgradeMode, boolean isResetMode) {
        return ClusterState.builder(new ClusterName("MlDailyMaintenanceServiceRunMetricsTests"))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata.builder().build())
                    .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isUpgradeMode(isUpgradeMode).isResetMode(isResetMode).build())
            )
            .nodes(DiscoveryNodes.builder().build())
            .build();
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocation -> {
            ((ActionListener<Response>) invocation.getArguments()[2]).onResponse(response);
            return null;
        };
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withException(Exception e) {
        return invocation -> {
            ((ActionListener<Response>) invocation.getArguments()[2]).onFailure(e);
            return null;
        };
    }
}
