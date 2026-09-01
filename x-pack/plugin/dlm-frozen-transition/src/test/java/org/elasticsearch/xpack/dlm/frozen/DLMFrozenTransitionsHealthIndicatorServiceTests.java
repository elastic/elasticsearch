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
import org.elasticsearch.health.node.DlmFrozenTransitionIndexInfo;
import org.elasticsearch.health.node.DlmFrozenTransitionsHealthInfo;
import org.elasticsearch.health.node.FileSettingsHealthInfo;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.ProjectIndexName;
import org.elasticsearch.health.node.StalledIndices;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

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

    public void testYellowWhenDisabledWithMarkedIndices() {
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(healthy().transitionsEnabled(false).markedIndicesCount(3).build())
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(result.impacts(), is(FROZEN_TRANSITION_BLOCKED_IMPACT));
        assertThat(result.diagnosisList(), containsInAnyOrder(new Diagnosis(TRANSITIONS_DISABLED_DIAGNOSIS_DEF, null)));
    }

    public void testGreenWhenDisabledWithOnlyEligibleUnmarkedIndices() {
        ProjectId projectId = randomProjectIdOrDefault();
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(
                healthy().transitionsEnabled(false)
                    .eligibleUnmarked(
                        new StalledIndices(
                            2,
                            List.of(
                                new DlmFrozenTransitionIndexInfo(projectId, "eligible-index-1", 1L),
                                new DlmFrozenTransitionIndexInfo(projectId, "eligible-index-2", 2L)
                            )
                        )
                    )
                    .build()
            )
        );
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.impacts(), is(List.of()));
        assertThat(result.diagnosisList(), is(List.of()));
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

    public void testYellowWithEligibleUnmarkedIndices() {
        ProjectId projectId = randomProjectIdOrDefault();
        DlmFrozenTransitionIndexInfo eligible = new DlmFrozenTransitionIndexInfo(projectId, "eligible-index", 1L);
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(healthy().eligibleUnmarked(new StalledIndices(1, List.of(eligible))).build())
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
        assertThat(Strings.toString(result.details()), containsString("\"eligible_unmarked_indices_count\":1"));
    }

    public void testYellowWithEligibleUnmarkedIndicesAndNoDefaultRepository() {
        ProjectId projectId = randomProjectIdOrDefault();
        DlmFrozenTransitionIndexInfo eligible = new DlmFrozenTransitionIndexInfo(projectId, "eligible-index", 1L);
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(
                healthy().defaultRepositoryConfigured(false).eligibleUnmarked(new StalledIndices(1, List.of(eligible))).build()
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

    public void testYellowWithNotStartedMarkedIndices() {
        ProjectId projectId = randomProjectIdOrDefault();
        DlmFrozenTransitionIndexInfo stalled = new DlmFrozenTransitionIndexInfo(projectId, "stalled-index", 1L);
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(healthy().markedIndicesCount(1).notStartedMarked(new StalledIndices(1, List.of(stalled))).build())
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
        assertThat(Strings.toString(result.details()), containsString("\"not_started_marked_indices\""));
    }

    public void testYellowWithQueuedMarkedIndices() {
        ProjectId projectId = randomProjectIdOrDefault();
        DlmFrozenTransitionIndexInfo queued = new DlmFrozenTransitionIndexInfo(projectId, "queued-index", 1L);
        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(healthy().markedIndicesCount(1).queuedMarked(new StalledIndices(1, List.of(queued))).build())
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
        assertThat(Strings.toString(result.details()), containsString("\"queued_marked_indices\""));
    }

    public void testMultipleConditionsProduceMultipleDiagnoses() {
        ProjectId projectId = randomProjectIdOrDefault();
        DlmFrozenTransitionsHealthInfo info = healthy().markedIndicesCount(3)
            .eligibleUnmarked(new StalledIndices(1, List.of(new DlmFrozenTransitionIndexInfo(projectId, "eligible-index", 1L))))
            .notStartedMarked(new StalledIndices(1, List.of(new DlmFrozenTransitionIndexInfo(projectId, "stalled-index", 1L))))
            .queuedMarked(new StalledIndices(1, List.of(new DlmFrozenTransitionIndexInfo(projectId, "queued-index", 1L))))
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

    public void testNonVerboseProducesNoDetailsOrDiagnoses() {
        ProjectId projectId = randomProjectIdOrDefault();
        DlmFrozenTransitionIndexInfo eligible = new DlmFrozenTransitionIndexInfo(projectId, "eligible-index", 1L);
        HealthIndicatorResult result = service.calculate(
            false,
            100,
            constructHealthInfo(healthy().eligibleUnmarked(new StalledIndices(1, List.of(eligible))).build())
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

    public void testMultiProject() {
        service = new DLMFrozenTransitionsHealthIndicatorService(TestProjectResolvers.allProjects(), now::get);

        ProjectId projectId1 = randomProjectIdOrDefault();
        ProjectId projectId2 = randomUniqueProjectId();
        DlmFrozenTransitionIndexInfo index1 = new DlmFrozenTransitionIndexInfo(projectId1, "index-one", 1L);
        DlmFrozenTransitionIndexInfo index2 = new DlmFrozenTransitionIndexInfo(projectId2, "index-two", 2L);

        String expectedName1 = projectId1 + ProjectIndexName.DELIMITER + "index-one";
        String expectedName2 = projectId2 + ProjectIndexName.DELIMITER + "index-two";

        HealthIndicatorResult result = service.calculate(
            true,
            100,
            constructHealthInfo(healthy().eligibleUnmarked(new StalledIndices(2, List.of(index1, index2))).build())
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
        private int markedIndicesCount = 0;
        private StalledIndices eligibleUnmarked = StalledIndices.EMPTY;
        private StalledIndices notStartedMarked = StalledIndices.EMPTY;
        private StalledIndices queuedMarked = StalledIndices.EMPTY;
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

        InfoBuilder markedIndicesCount(int count) {
            markedIndicesCount = count;
            return this;
        }

        InfoBuilder eligibleUnmarked(StalledIndices value) {
            eligibleUnmarked = value;
            return this;
        }

        InfoBuilder notStartedMarked(StalledIndices value) {
            notStartedMarked = value;
            return this;
        }

        InfoBuilder queuedMarked(StalledIndices value) {
            queuedMarked = value;
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
                markedIndicesCount,
                eligibleUnmarked,
                notStartedMarked,
                queuedMarked,
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
