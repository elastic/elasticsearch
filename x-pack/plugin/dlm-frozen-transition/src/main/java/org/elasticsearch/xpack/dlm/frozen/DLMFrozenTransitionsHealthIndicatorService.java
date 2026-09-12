/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.DlmFrozenTransitionsHealthInfo;
import org.elasticsearch.health.node.DlmFrozenTransitionsHealthInfo.TransitionState;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.ProjectIndexName;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Reports health for the DLM frozen-tier transition feature.
 *
 * <p>The master publishes a snapshot of the indices it considers <em>overdue</em>: past their {@code frozen_after} age
 * by more than the configured stuck threshold, and not yet transitioned. Each overdue index carries the transition
 * state it is stuck in ({@code UNMARKED}, {@code MARKED}, {@code QUEUED} or {@code RUNNING}).
 *
 * <p>While transitions are enabled, the indicator reports YELLOW when the frozen transition service is not running on
 * the current master, or when an overdue index is stuck in {@code UNMARKED}, {@code MARKED} or {@code QUEUED}. An
 * overdue index in {@code RUNNING} is making progress, so it is reported in the details but raises no diagnosis and
 * does not affect the status.
 *
 * <p>While transitions are disabled, the indicator reports YELLOW when any overdue index is in a state other than
 * {@code MARKED}. {@code UNMARKED} indices are still a problem: the data-stream lifecycle service marks eligible
 * indices independently of the {@code dlm.frozen_transitions.enabled} setting, so a persistent {@code UNMARKED}
 * backlog is unexpected in either mode. {@code QUEUED} and {@code RUNNING} transitions are in-flight work that the
 * operator's setting change cannot cancel; they must complete before the cluster is idle. Only {@code MARKED} is
 * healthy while disabled: the index has been flagged for conversion, the executor will not pick it up, and that is
 * exactly the expected steady state for a cluster with the feature switched off.
 *
 * <p>The publisher collects non-{@code MARKED} and {@code MARKED} indices in separate internal buckets, then merges
 * them into a single sample capped at {@link DLMFrozenTransitionHealthInfoPublisher#MAX_INDICES_TO_PUBLISH} total,
 * with non-{@code MARKED} entries placed first. A flood of {@code MARKED} indices can therefore never displace a
 * non-{@code MARKED} index from the final sample. This indicator trusts that guarantee: if no non-{@code MARKED}
 * indices appear in the sample, none exist.
 *
 * <p>Indicator reports UNKNOWN when the health snapshot is older than {@link #STALE_AFTER_PUBLISH_INTERVALS} times the
 * publisher's configured interval, which indicates that publishing has stopped.
 */
public class DLMFrozenTransitionsHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "dlm_frozen_transitions";
    public static final String HELP_URL = "https://ela.st/health-dlm-frozen-transitions";

    // Number of missed publish intervals before the snapshot is considered stale.
    static final int STALE_AFTER_PUBLISH_INTERVALS = 3;

    public static final String FROZEN_TRANSITION_BLOCKED_IMPACT_ID = "frozen_transition_blocked";

    public static final List<HealthIndicatorImpact> FROZEN_TRANSITION_BLOCKED_IMPACT = List.of(
        new HealthIndicatorImpact(
            NAME,
            FROZEN_TRANSITION_BLOCKED_IMPACT_ID,
            3,
            "Data stream backing indices may be delayed or blocked from transitioning to the frozen tier. Data retention and storage "
                + "cost management could be impacted.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );

    public static final Diagnosis.Definition TRANSITIONS_DISABLED_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "transitions_disabled",
        "DLM frozen transitions are disabled, but some indices have frozen-tier transitions that are queued or in progress.",
        "Wait for queued and running frozen transitions to complete. If they remain overdue, inspect the "
            + "[dlm_frozen_transition] thread pool and the current master node's logs.",
        HELP_URL
    );

    public static final Diagnosis.Definition SERVICE_NOT_RUNNING_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "service_not_running",
        "The DLM frozen transition service is not running on the current master node.",
        "Check the current master node's logs for errors related to the DLM frozen transition service. A master "
            + "failover may resolve the issue.",
        HELP_URL
    );

    public static final Diagnosis.Definition ELIGIBLE_INDICES_UNMARKED_NO_REPOSITORY_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "eligible_indices_unmarked_no_default_repository",
        "Some indices are eligible for conversion to the frozen tier but have not been marked for conversion "
            + "because no default snapshot repository is configured.",
        "Configure a default snapshot repository using a cluster settings update on [repositories.default_repository].",
        HELP_URL
    );

    public static final Diagnosis.Definition ELIGIBLE_INDICES_UNMARKED_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "eligible_indices_unmarked",
        "Some indices are eligible for conversion to the frozen tier but have not been marked for conversion, "
            + "even though a default snapshot repository is configured.",
        "Check the current master node's logs for errors related to the data stream lifecycle and the DLM frozen transition service.",
        HELP_URL
    );

    public static final Diagnosis.Definition MARKED_TRANSITIONS_NOT_STARTED_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "marked_transitions_not_started",
        "Some indices have been marked for conversion to the frozen tier but have not been submitted to the transition "
            + "executor. The data stream lifecycle explain API reports these indices with a [frozen_transition_status] of [marked].",
        "Check the current master node's logs for errors related to the DLM frozen transition service. Check the current "
            + "status of the affected indices using the [GET /<affected_index_name>/_lifecycle/explain] API. Please replace "
            + "the <affected_index_name> in the API with the actual index name.",
        HELP_URL
    );

    public static final Diagnosis.Definition MARKED_TRANSITIONS_QUEUED_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "marked_transitions_queued",
        "Some indices have been submitted to the DLM frozen transition executor but have been waiting in its queue "
            + "without starting. The data stream lifecycle explain API reports these indices with a "
            + "[frozen_transition_status] of [queued].",
        "Inspect the [dlm_frozen_transition] thread pool for a saturated queue or rejected tasks using the "
            + "[GET /_cat/thread_pool/dlm_frozen_transition?v] API. Transitions queue when all transition threads are "
            + "busy; a persistently full queue means transitions are completing more slowly than indices are becoming "
            + "eligible.",
        HELP_URL
    );

    private final ProjectResolver projectResolver;
    private final LongSupplier nowSupplier;

    public DLMFrozenTransitionsHealthIndicatorService(ProjectResolver projectResolver, LongSupplier nowSupplier) {
        this.projectResolver = projectResolver;
        this.nowSupplier = nowSupplier;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        DlmFrozenTransitionsHealthInfo info = healthInfo.dlmFrozenTransitionsHealthInfo();
        if (info == null) {
            return createIndicator(
                HealthStatus.GREEN,
                "No DLM frozen transition health data available yet. Health information will be reported after the first run.",
                HealthIndicatorDetails.EMPTY,
                List.of(),
                List.of()
            );
        }

        long ageMillis = nowSupplier.getAsLong() - info.generatedAtMillis();
        if (ageMillis > (long) STALE_AFTER_PUBLISH_INTERVALS * info.publishIntervalMillis()) {
            return createIndicator(
                HealthStatus.UNKNOWN,
                "DLM frozen transition health information is stale; the master may have stopped reporting it",
                verbose
                    ? new SimpleHealthIndicatorDetails(Map.of("generated_at_millis", info.generatedAtMillis()))
                    : HealthIndicatorDetails.EMPTY,
                List.of(),
                List.of()
            );
        }

        boolean supportsMultipleProjects = projectResolver.supportsMultipleProjects();
        boolean transitionsEnabled = info.transitionsEnabled();
        HealthIndicatorDetails details = createDetails(verbose, info, supportsMultipleProjects);

        // The scheduler check is meaningful only while transitions are enabled. When the feature is off, the transition
        // service's scheduled scan exits without submitting work, so a stopped transition scheduler while disabled is
        // not actionable noise for the operator. Health snapshots are published by a separate scheduler.
        if (transitionsEnabled && info.serviceRunning() == false) {
            return createIndicator(
                HealthStatus.YELLOW,
                "The DLM frozen transition service is not running on the current master node",
                details,
                FROZEN_TRANSITION_BLOCKED_IMPACT,
                verbose ? List.of(new Diagnosis(SERVICE_NOT_RUNNING_DIAGNOSIS_DEF, null)) : List.of()
            );
        }

        Map<TransitionState, List<String>> overdueByState = groupOverdueIndexNamesByState(info, supportsMultipleProjects);

        // Merge the per-state groups into per-definition groups. MARKED while disabled resolves to null (healthy),
        // QUEUED and RUNNING while disabled both resolve to TRANSITIONS_DISABLED_DIAGNOSIS_DEF and are merged.
        // Walk the EnumMap in ordinal order so the resulting LinkedHashMap has a deterministic iteration order.
        Map<Diagnosis.Definition, List<String>> byDefinition = new LinkedHashMap<>();
        for (Map.Entry<TransitionState, List<String>> entry : overdueByState.entrySet()) {
            Diagnosis.Definition def = diagnosisFor(entry.getKey(), transitionsEnabled, info.defaultRepositoryConfigured());
            if (def != null) {
                byDefinition.computeIfAbsent(def, ignored -> new ArrayList<>()).addAll(entry.getValue());
            }
        }

        if (byDefinition.isEmpty()) {
            String symptom = transitionsEnabled
                ? "DLM frozen transitions are executing without issues"
                : "DLM frozen transitions are disabled";
            return createIndicator(HealthStatus.GREEN, symptom, details, List.of(), List.of());
        }

        List<Diagnosis> diagnoses = new ArrayList<>();
        byDefinition.forEach(
            (def, indexNames) -> diagnoses.add(
                new Diagnosis(
                    def,
                    List.of(
                        new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, indexNames.stream().limit(maxAffectedResourcesCount).toList())
                    )
                )
            )
        );

        String symptom;
        if (diagnoses.size() > 1) {
            symptom = diagnoses.size() + " issues affecting DLM frozen-tier transitions were detected";
        } else {
            symptom = "An issue affecting DLM frozen-tier transitions was detected";
        }
        return createIndicator(HealthStatus.YELLOW, symptom, details, FROZEN_TRANSITION_BLOCKED_IMPACT, verbose ? diagnoses : List.of());
    }

    /**
     * The diagnosis to raise for overdue indices stuck in the given state, or {@code null} if the state warrants none.
     *
     * <p>While transitions are enabled: {@code RUNNING} is making progress, so it is informational only ({@code null}).
     * {@code MARKED} means the executor has not picked the index up yet; {@code QUEUED} means it is waiting for a
     * thread.
     *
     * <p>While transitions are disabled: {@code MARKED} is the expected steady state — the data-stream lifecycle
     * service marks indices regardless of the enabled setting, and the executor simply will not pick them up. Every
     * other state is actionable: {@code UNMARKED} means the lifecycle service is not marking as expected; {@code QUEUED}
     * and {@code RUNNING} are in-flight work that cannot be cancelled by disabling the feature.
     */
    private static Diagnosis.Definition diagnosisFor(
        TransitionState state,
        boolean transitionsEnabled,
        boolean defaultRepositoryConfigured
    ) {
        return switch (state) {
            case UNMARKED -> defaultRepositoryConfigured
                ? ELIGIBLE_INDICES_UNMARKED_DIAGNOSIS_DEF
                : ELIGIBLE_INDICES_UNMARKED_NO_REPOSITORY_DIAGNOSIS_DEF;
            case MARKED -> transitionsEnabled ? MARKED_TRANSITIONS_NOT_STARTED_DIAGNOSIS_DEF : null;
            case QUEUED -> transitionsEnabled ? MARKED_TRANSITIONS_QUEUED_DIAGNOSIS_DEF : TRANSITIONS_DISABLED_DIAGNOSIS_DEF;
            case RUNNING -> transitionsEnabled ? null : TRANSITIONS_DISABLED_DIAGNOSIS_DEF;
        };
    }

    /**
     * Groups the published sample of overdue indices by the state they are stuck in, resolving each to its display
     * name. Names are sorted so that diagnosis resources and details are stable across calls.
     */
    private static Map<TransitionState, List<String>> groupOverdueIndexNamesByState(
        DlmFrozenTransitionsHealthInfo info,
        boolean supportsMultipleProjects
    ) {
        Map<TransitionState, List<String>> byState = new EnumMap<>(TransitionState.class);
        info.overdueIndices().forEach((projectId, indices) -> indices.forEach((indexName, state) -> {
            String displayName = new ProjectIndexName(projectId, indexName).toString(supportsMultipleProjects);
            byState.computeIfAbsent(state, ignored -> new ArrayList<>()).add(displayName);
        }));
        byState.values().forEach(names -> names.sort(null));
        return byState;
    }

    private static HealthIndicatorDetails createDetails(
        boolean verbose,
        DlmFrozenTransitionsHealthInfo info,
        boolean supportsMultipleProjects
    ) {
        if (verbose == false) {
            return HealthIndicatorDetails.EMPTY;
        }

        var details = new HashMap<String, Object>();
        details.put("transitions_enabled", info.transitionsEnabled());
        details.put("service_running", info.serviceRunning());
        details.put("default_repository_configured", info.defaultRepositoryConfigured());
        details.put("overdue_indices_count", info.totalOverdueIndicesCount());
        if (info.overdueIndices().isEmpty() == false) {
            details.put("overdue_indices", overdueIndexDetails(info, supportsMultipleProjects));
        }
        return new SimpleHealthIndicatorDetails(details);
    }

    private static List<Map<String, Object>> overdueIndexDetails(DlmFrozenTransitionsHealthInfo info, boolean supportsMultipleProjects) {
        List<Map<String, Object>> entries = new ArrayList<>();
        info.overdueIndices().forEach((projectId, indices) -> indices.forEach((indexName, state) -> {
            LinkedHashMap<String, Object> entry = new LinkedHashMap<>(2, 1.0f);
            entry.put("index_name", new ProjectIndexName(projectId, indexName).toString(supportsMultipleProjects));
            entry.put("transition_state", state.toString());
            entries.add(entry);
        }));
        entries.sort((left, right) -> ((String) left.get("index_name")).compareTo((String) right.get("index_name")));
        return entries;
    }
}
