/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.DlmFrozenTransitionsHealthInfo;
import org.elasticsearch.health.node.DlmFrozenTransitionsHealthInfo.TransitionState;
import org.elasticsearch.health.node.FileSettingsHealthInfo;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.ProjectIndexName;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.health.node.DataStreamLifecycleHealthInfo.NO_DSL_ERRORS;
import static org.elasticsearch.xpack.dlm.frozen.DLMFrozenTransitionsHealthIndicatorService.ELIGIBLE_INDICES_UNMARKED_DIAGNOSIS_DEF;
import static org.elasticsearch.xpack.dlm.frozen.DLMFrozenTransitionsHealthIndicatorService.ELIGIBLE_INDICES_UNMARKED_NO_REPOSITORY_DIAGNOSIS_DEF;
import static org.elasticsearch.xpack.dlm.frozen.DLMFrozenTransitionsHealthIndicatorService.FROZEN_TRANSITION_BLOCKED_IMPACT;
import static org.elasticsearch.xpack.dlm.frozen.DLMFrozenTransitionsHealthIndicatorService.MARKED_TRANSITIONS_NOT_STARTED_DIAGNOSIS_DEF;
import static org.elasticsearch.xpack.dlm.frozen.DLMFrozenTransitionsHealthIndicatorService.MARKED_TRANSITIONS_QUEUED_DIAGNOSIS_DEF;
import static org.elasticsearch.xpack.dlm.frozen.DLMFrozenTransitionsHealthIndicatorService.SERVICE_NOT_RUNNING_DIAGNOSIS_DEF;
import static org.elasticsearch.xpack.dlm.frozen.DLMFrozenTransitionsHealthIndicatorService.STALE_AFTER_PUBLISH_INTERVALS;
import static org.elasticsearch.xpack.dlm.frozen.DLMFrozenTransitionsHealthIndicatorService.TRANSITIONS_DISABLED_DIAGNOSIS_DEF;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class DLMFrozenTransitionsHealthIndicatorServiceTests extends ESTestCase {

    private static final long PUBLISH_INTERVAL_MILLIS = TimeValue.timeValueHours(1).millis();

    private final AtomicLong now = new AtomicLong();
    private DLMFrozenTransitionsHealthIndicatorService service;

    @Before
    public void setupService() {
        now.set(System.currentTimeMillis());
        service = new DLMFrozenTransitionsHealthIndicatorService(
            TestProjectResolvers.singleProjectOnly(randomProjectIdOrDefault()),
            now::get
        );
    }

    public void testGreenWhenNoHealthDataYet() {
        HealthIndicatorResult result = service.calculate(true, 100, constructHealthInfo(null));
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.details(), is(HealthIndicatorDetails.EMPTY));
        assertThat(result.impacts(), is(List.of()));
        assertThat(result.diagnosisList(), is(List.of()));
    }

    public void testGreenWhenDisabledWithoutPendingWork() {
        HealthIndicatorResult result = service.calculate(true, 100, constructHealthInfo(healthy().transitionsEnabled(false).build()));
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.impacts(), is(List.of()));
        assertThat(result.diagnosisList(), is(List.of()));
    }

    /**
     * {@code MARKED} is the expected steady state while transitions are disabled. The data-stream lifecycle service
     * marks eligible indices regardless of the {@code dlm.frozen_transitions.enabled} setting, and the executor
     * simply will not pick them up. This is not a problem the operator needs to act on.
     */
    public void testGreenWhenDisabledWithOnlyMarkedIndices() {
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(
                healthy().transitionsEnabled(false).overdue(randomProjectIdOrDefault(), "marked-index", TransitionState.MARKED).build()
            )
        );
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.impacts(), is(List.of()));
        assertThat(result.diagnosisList(), is(List.of()));
        assertThat(result.symptom(), containsString("disabled"));
    }

    /**
     * The scheduler-running check is gated on transitions being enabled. While disabled, the transition service's
     * scheduled scan exits without submitting work; health snapshots are published by an independent scheduler.
     * A stopped transition scheduler is therefore not actionable until transitions are re-enabled.
     */
    public void testGreenWhenDisabledWithMarkedIndicesAndServiceNotRunning() {
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(
                healthy().transitionsEnabled(false)
                    .serviceRunning(false)
                    .overdue(randomProjectIdOrDefault(), "marked-index", TransitionState.MARKED)
                    .build()
            )
        );
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.impacts(), is(List.of()));
        assertThat(result.diagnosisList(), is(List.of()));
    }

    /**
     * {@code UNMARKED} indices are a problem in both modes. The data-stream lifecycle service marks eligible indices
     * independently of the enabled setting, so a persistent {@code UNMARKED} backlog is unexpected even when
     * transitions are disabled.
     */
    public void testYellowWhenDisabledWithEligibleUnmarkedIndices() {
        ProjectId projectId = randomProjectIdOrDefault();
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(
                healthy().transitionsEnabled(false)
                    .overdue(projectId, "eligible-index-1", TransitionState.UNMARKED)
                    .overdue(projectId, "eligible-index-2", TransitionState.UNMARKED)
                    .build()
            )
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(result.impacts(), is(FROZEN_TRANSITION_BLOCKED_IMPACT));
        assertThat(
            result.diagnosisList(),
            containsInAnyOrder(
                new Diagnosis(
                    ELIGIBLE_INDICES_UNMARKED_DIAGNOSIS_DEF,
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("eligible-index-1", "eligible-index-2")))
                )
            )
        );
    }

    /**
     * {@code QUEUED} and {@code RUNNING} transitions are in-flight work that disabling the feature cannot cancel.
     * Both map to the same {@code transitions_disabled} diagnosis and their index names are merged into one resource
     * list.
     */
    public void testYellowWhenDisabledWithQueuedAndRunningIndices() {
        ProjectId projectId = randomProjectIdOrDefault();
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(
                healthy().transitionsEnabled(false)
                    .overdue(projectId, "queued-index", TransitionState.QUEUED)
                    .overdue(projectId, "running-index", TransitionState.RUNNING)
                    .build()
            )
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(result.impacts(), is(FROZEN_TRANSITION_BLOCKED_IMPACT));
        assertThat(result.symptom(), containsString("An issue"));
        assertThat(result.diagnosisList().size(), is(1));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        assertThat(diagnosis.definition(), is(TRANSITIONS_DISABLED_DIAGNOSIS_DEF));
        assertThat(diagnosis.affectedResources().get(0).getValues(), containsInAnyOrder("queued-index", "running-index"));
    }

    public void testYellowWhenServiceNotRunning() {
        HealthIndicatorResult result = service.calculate(true, 100, constructHealthInfo(healthy().serviceRunning(false).build()));
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(result.impacts(), is(FROZEN_TRANSITION_BLOCKED_IMPACT));
        assertThat(result.diagnosisList(), containsInAnyOrder(new Diagnosis(SERVICE_NOT_RUNNING_DIAGNOSIS_DEF, null)));
    }

    public void testGreenWhenRunningWithNoIssues() {
        HealthIndicatorResult result = service.calculate(true, 100, constructHealthInfo(healthy().build()));
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.impacts(), is(List.of()));
        assertThat(result.diagnosisList(), is(List.of()));
    }

    /**
     * An overdue index whose transition is actually running is making progress. It is worth reporting in the details,
     * but it is not a problem the operator can act on, so it must not raise a diagnosis or turn the indicator YELLOW.
     */
    public void testGreenWhenOverdueIndicesAreOnlyRunning() {
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(healthy().overdue(randomProjectIdOrDefault(), "running-index", TransitionState.RUNNING).build())
        );
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.impacts(), is(List.of()));
        assertThat(result.diagnosisList(), is(List.of()));
        assertThat(Strings.toString(result.details()), containsString("\"transition_state\":\"running\""));
    }

    public void testYellowWithEligibleUnmarkedIndices() {
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(healthy().overdue(randomProjectIdOrDefault(), "eligible-index", TransitionState.UNMARKED).build())
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(
            result.diagnosisList(),
            containsInAnyOrder(
                new Diagnosis(
                    ELIGIBLE_INDICES_UNMARKED_DIAGNOSIS_DEF,
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("eligible-index")))
                )
            )
        );
        assertThat(Strings.toString(result.details()), containsString("\"overdue_indices_count\":1"));
    }

    public void testYellowWithEligibleUnmarkedIndicesAndNoDefaultRepository() {
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(
                healthy().defaultRepositoryConfigured(false)
                    .overdue(randomProjectIdOrDefault(), "eligible-index", TransitionState.UNMARKED)
                    .build()
            )
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(
            result.diagnosisList(),
            containsInAnyOrder(
                new Diagnosis(
                    ELIGIBLE_INDICES_UNMARKED_NO_REPOSITORY_DIAGNOSIS_DEF,
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("eligible-index")))
                )
            )
        );
    }

    public void testYellowWithMarkedButNotStartedIndices() {
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(healthy().overdue(randomProjectIdOrDefault(), "stalled-index", TransitionState.MARKED).build())
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(
            result.diagnosisList(),
            containsInAnyOrder(
                new Diagnosis(
                    MARKED_TRANSITIONS_NOT_STARTED_DIAGNOSIS_DEF,
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("stalled-index")))
                )
            )
        );
        assertThat(Strings.toString(result.details()), containsString("\"transition_state\":\"marked\""));
    }

    public void testYellowWithQueuedIndices() {
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(healthy().overdue(randomProjectIdOrDefault(), "queued-index", TransitionState.QUEUED).build())
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(
            result.diagnosisList(),
            containsInAnyOrder(
                new Diagnosis(
                    MARKED_TRANSITIONS_QUEUED_DIAGNOSIS_DEF,
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("queued-index")))
                )
            )
        );
        assertThat(Strings.toString(result.details()), containsString("\"transition_state\":\"queued\""));
    }

    /**
     * A running overdue index alongside genuinely stuck ones must not add a fourth diagnosis, so the symptom still
     * reports three issues.
     */
    public void testMultipleConditionsProduceMultipleDiagnoses() {
        ProjectId projectId = randomProjectIdOrDefault();
        DlmFrozenTransitionsHealthInfo info = healthy().overdue(projectId, "eligible-index", TransitionState.UNMARKED)
            .overdue(projectId, "stalled-index", TransitionState.MARKED)
            .overdue(projectId, "queued-index", TransitionState.QUEUED)
            .overdue(projectId, "running-index", TransitionState.RUNNING)
            .build();
        HealthIndicatorResult result = service.calculate(true, 100, constructHealthInfo(info));
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(result.symptom(), containsString("3 issues"));
        assertThat(
            result.diagnosisList(),
            containsInAnyOrder(
                new Diagnosis(
                    ELIGIBLE_INDICES_UNMARKED_DIAGNOSIS_DEF,
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("eligible-index")))
                ),
                new Diagnosis(
                    MARKED_TRANSITIONS_NOT_STARTED_DIAGNOSIS_DEF,
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("stalled-index")))
                ),
                new Diagnosis(
                    MARKED_TRANSITIONS_QUEUED_DIAGNOSIS_DEF,
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("queued-index")))
                )
            )
        );
    }

    public void testAffectedResourcesAreLimited() {
        ProjectId projectId = randomProjectIdOrDefault();
        InfoBuilder builder = healthy();
        for (int i = 0; i < 10; i++) {
            builder.overdue(projectId, "eligible-index-" + i, TransitionState.UNMARKED);
        }
        HealthIndicatorResult result = service.calculate(true, 3, constructHealthInfo(builder.build()));
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(
            result.diagnosisList().get(0).affectedResources().get(0).getValues(),
            is(List.of("eligible-index-0", "eligible-index-1", "eligible-index-2"))
        );
    }

    public void testNonVerboseProducesNoDetailsOrDiagnoses() {
        HealthIndicatorResult result = service.calculate(
            false,
            100,
            constructHealthInfo(healthy().overdue(randomProjectIdOrDefault(), "eligible-index", TransitionState.UNMARKED).build())
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(result.details(), is(HealthIndicatorDetails.EMPTY));
        assertThat(result.diagnosisList(), is(List.of()));
    }

    public void testUnknownWhenHealthInfoIsStale() {
        long generatedAt = now.get() - (STALE_AFTER_PUBLISH_INTERVALS + 1) * PUBLISH_INTERVAL_MILLIS;
        HealthIndicatorResult result = service.calculate(true, 100, constructHealthInfo(healthy().generatedAtMillis(generatedAt).build()));
        assertThat(result.status(), is(HealthStatus.UNKNOWN));
        assertThat(Strings.toString(result.details()), containsString("\"generated_at_millis\""));
    }

    public void testNotStaleWhenWithinThreshold() {
        long generatedAt = now.get() - (STALE_AFTER_PUBLISH_INTERVALS - 1) * PUBLISH_INTERVAL_MILLIS;
        HealthIndicatorResult result = service.calculate(true, 100, constructHealthInfo(healthy().generatedAtMillis(generatedAt).build()));
        assertThat(result.status(), is(HealthStatus.GREEN));
    }

    /**
     * The staleness check is a strict greater-than. A snapshot exactly
     * {@code STALE_AFTER_PUBLISH_INTERVALS} intervals old is still fresh.
     */
    public void testNotStaleAtExactThreshold() {
        long generatedAt = now.get() - STALE_AFTER_PUBLISH_INTERVALS * PUBLISH_INTERVAL_MILLIS;
        HealthIndicatorResult result = service.calculate(true, 100, constructHealthInfo(healthy().generatedAtMillis(generatedAt).build()));
        assertThat(result.status(), is(HealthStatus.GREEN));
    }

    public void testMultiProject() {
        service = new DLMFrozenTransitionsHealthIndicatorService(TestProjectResolvers.allProjects(), now::get);

        ProjectId projectId1 = randomProjectIdOrDefault();
        ProjectId projectId2 = randomUniqueProjectId();

        String expectedName1 = projectId1 + ProjectIndexName.DELIMITER + "index-one";
        String expectedName2 = projectId2 + ProjectIndexName.DELIMITER + "index-two";

        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(
                healthy().overdue(projectId1, "index-one", TransitionState.UNMARKED)
                    .overdue(projectId2, "index-two", TransitionState.UNMARKED)
                    .build()
            )
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        Diagnosis diagnosis = result.diagnosisList()
            .stream()
            .filter(d -> d.definition().equals(ELIGIBLE_INDICES_UNMARKED_DIAGNOSIS_DEF))
            .findFirst()
            .orElseThrow();
        assertThat(diagnosis.affectedResources().get(0).getValues(), containsInAnyOrder(expectedName1, expectedName2));
        assertThat(Strings.toString(result.details()), containsString(expectedName1));
        assertThat(Strings.toString(result.details()), containsString(expectedName2));
    }

    // --- helpers ---

    /**
     * Fluent builder for {@link DlmFrozenTransitionsHealthInfo} test fixtures. Starts from the all-green
     * healthy state so tests only configure the fields relevant to their scenario.
     */
    private final class InfoBuilder {
        private boolean transitionsEnabled = true;
        private boolean serviceRunning = true;
        private boolean defaultRepositoryConfigured = true;
        private final Map<ProjectId, Map<String, TransitionState>> overdueIndices = new HashMap<>();
        private int totalOverdueIndicesCount = 0;
        private long generatedAtMillis = now.get();
        private long publishIntervalMillis = PUBLISH_INTERVAL_MILLIS;

        InfoBuilder transitionsEnabled(boolean value) {
            transitionsEnabled = value;
            return this;
        }

        InfoBuilder serviceRunning(boolean value) {
            serviceRunning = value;
            return this;
        }

        InfoBuilder defaultRepositoryConfigured(boolean value) {
            defaultRepositoryConfigured = value;
            return this;
        }

        InfoBuilder overdue(ProjectId projectId, String indexName, TransitionState state) {
            overdueIndices.computeIfAbsent(projectId, ignored -> new HashMap<>()).put(indexName, state);
            totalOverdueIndicesCount++;
            return this;
        }

        InfoBuilder generatedAtMillis(long millis) {
            generatedAtMillis = millis;
            return this;
        }

        DlmFrozenTransitionsHealthInfo build() {
            return new DlmFrozenTransitionsHealthInfo(
                transitionsEnabled,
                serviceRunning,
                defaultRepositoryConfigured,
                overdueIndices,
                totalOverdueIndicesCount,
                generatedAtMillis,
                publishIntervalMillis
            );
        }
    }

    private InfoBuilder healthy() {
        return new InfoBuilder();
    }

    private HealthInfo constructHealthInfo(DlmFrozenTransitionsHealthInfo dlmFrozenTransitionsHealthInfo) {
        return new HealthInfo(Map.of(), NO_DSL_ERRORS, Map.of(), FileSettingsHealthInfo.INDETERMINATE, dlmFrozenTransitionsHealthInfo);
    }
}
