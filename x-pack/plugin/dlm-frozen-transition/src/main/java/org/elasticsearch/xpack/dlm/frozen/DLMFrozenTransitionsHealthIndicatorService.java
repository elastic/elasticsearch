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
import java.util.TreeMap;
import java.util.function.LongSupplier;

/**
 * Reports health for the DLM frozen-tier transition feature.
 *
 * <p>The master publishes a snapshot containing: a complete count of overdue indices per {@link TransitionState}
 * (in {@code overdueIndicesCountByState}); and a capped informational sample of up to
 * {@code DLMFrozenTransitionHealthInfoPublisher.MAX_INDICES_TO_PUBLISH} index names per state (in {@code overdueIndices}).
 * An overdue index is one that has passed its {@code frozen_after} age by more than the configured stuck threshold and
 * has not yet completed its frozen-tier transition. Indices whose transition is already running are making progress and
 * are excluded from both the counts and the sample.
 *
 * <p>The indicator raises a {@link HealthStatus#YELLOW} diagnosis for each transition state whose count is greater than
 * zero. The sample supplies example index names for diagnosis resources; the diagnosis is raised even when the sample
 * contains fewer names than the count (as can happen with an older master during a rolling upgrade). Each diagnosis
 * reports its cluster-wide affected-index count in its cause text, so an operator can tell how many indices are in
 * that state even though the listed resources are only a capped sample.
 *
 * <p>{@code UNMARKED} means the data-stream lifecycle service is not marking eligible indices as expected; it marks
 * them independently of the {@code dlm.frozen_transitions.enabled} setting, so a persistent {@code UNMARKED} backlog
 * is unexpected in either mode. {@code MARKED} and {@code QUEUED} indices are waiting on the transition executor,
 * which will not drain them while the feature is switched off. Only the diagnosis text differs between the two modes.
 *
 * <p>The indicator also reports YELLOW when the frozen transition service is not running on the current master, but
 * only while transitions are enabled.
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
            "Data stream backing indices may be delayed or blocked from transitioning to the frozen tier. Storage "
                + "size and cost management could be impacted.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );

    public static final Diagnosis.Definition TRANSITIONS_DISABLED_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "transitions_disabled",
        "DLM frozen transitions are disabled, but some overdue indices are marked or queued for a frozen-tier transition.",
        "Re-enable transitions with the [dlm.frozen_transitions.enabled] cluster setting so that marked indices can "
            + "transition. Queued transitions complete on their own. If they remain overdue, inspect the "
            + "[dlm_frozen_transition] thread pool and the current master node's logs.",
        HELP_URL
    );

    public static final Diagnosis.Definition SERVICE_NOT_RUNNING_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "service_not_running",
        "The DLM frozen transition service is not running on the current master node.",
        "Check the current master node's logs for errors related to the DLM frozen transition service. Restarting "
            + "the master may resolve the issue.",
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
        "Some indices have been marked for conversion to the frozen tier but have not been submitted to the transition executor. ",
        "Check the current master node's logs for errors related to the DLM frozen transition service. Check the current "
            + "status of the affected indices using the [GET /<affected_index_name>/_lifecycle/explain] API. Please replace "
            + "the <affected_index_name> in the API with the actual index name.",
        HELP_URL
    );

    public static final Diagnosis.Definition MARKED_TRANSITIONS_QUEUED_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "marked_transitions_queued",
        "Some indices have been submitted to the DLM frozen transition executor but have been waiting in its queue without starting.",
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

        // Sample names are used for diagnosis resources; diagnoses themselves are driven by the per-state counts so
        // that a minority state is never crowded out of the sample by a majority state.
        Map<TransitionState, List<String>> sampleNamesByState = groupOverdueIndexNamesByState(info, supportsMultipleProjects);

        // Merge the per-state counts into per-definition groups. Walk TransitionState in ordinal order so the resulting
        // LinkedHashMap has a deterministic iteration order. MARKED and QUEUED while disabled both resolve to
        // TRANSITIONS_DISABLED_DIAGNOSIS_DEF, so their counts and sample names are summed into one group.
        Map<Diagnosis.Definition, AffectedIndices> byDefinition = new LinkedHashMap<>();
        for (TransitionState state : TransitionState.values()) {
            int stateCount = info.overdueIndicesCountByState().getOrDefault(state, 0);
            if (stateCount > 0) {
                Diagnosis.Definition def = diagnosisFor(state, transitionsEnabled, info.defaultRepositoryConfigured());
                if (def != null) {
                    AffectedIndices affected = byDefinition.computeIfAbsent(def, ignored -> new AffectedIndices());
                    affected.count += stateCount;
                    affected.sampleNames.addAll(sampleNamesByState.getOrDefault(state, List.of()));
                }
            }
        }

        if (byDefinition.isEmpty()) {
            String symptom = transitionsEnabled
                ? "DLM frozen transitions are executing without issues"
                : "DLM frozen transitions are disabled";
            return createIndicator(HealthStatus.GREEN, symptom, details, List.of(), List.of());
        }

        List<Diagnosis> diagnoses = new ArrayList<>();
        byDefinition.forEach((def, affected) -> {
            List<Diagnosis.Resource> resources = affected.sampleNames.isEmpty()
                ? null
                : List.of(
                    new Diagnosis.Resource(
                        Diagnosis.Resource.Type.INDEX,
                        affected.sampleNames.stream().limit(maxAffectedResourcesCount).toList()
                    )
                );
            diagnoses.add(new Diagnosis(withAffectedCount(def, affected.count), resources));
        });

        String symptom;
        if (diagnoses.size() > 1) {
            symptom = diagnoses.size() + " issues affecting DLM frozen-tier transitions were detected";
        } else {
            symptom = "An issue affecting DLM frozen-tier transitions was detected";
        }
        return createIndicator(HealthStatus.YELLOW, symptom, details, FROZEN_TRANSITION_BLOCKED_IMPACT, verbose ? diagnoses : List.of());
    }

    /**
     * Returns a copy of the given diagnosis definition whose cause also reports how many indices are affected across
     * the whole cluster. That count is the complete total for every transition state mapping to this definition, so it
     * can exceed the number of index names in the diagnosis's affected resources, which are drawn from a capped sample.
     *
     * <p>The definition's {@code id} and {@code helpURL} are preserved, so {@link Diagnosis.Definition#getUniqueId()}
     * stays stable for consumers that key off it.
     */
    // visible for testing
    static Diagnosis.Definition withAffectedCount(Diagnosis.Definition base, int count) {
        // Several causes end in a trailing space, so strip before appending to avoid a double space.
        String cause = base.cause().strip() + " " + (count == 1 ? "1 index is affected." : count + " indices are affected.");
        return new Diagnosis.Definition(base.indicatorName(), base.id(), cause, base.action(), base.helpURL());
    }

    /**
     * Accumulates, for a single diagnosis definition, the cluster-wide count of affected indices together with the
     * example index names drawn from the published sample. The count can exceed the number of names, because the
     * publisher caps the sample per transition state.
     */
    private static final class AffectedIndices {
        private int count;
        private final List<String> sampleNames = new ArrayList<>();
    }

    /**
     * The diagnosis to raise for overdue indices stuck in the given state, or {@code null} if the state warrants none.
     *
     * <p>{@code UNMARKED} means the data-stream lifecycle service has not marked the index for conversion, which is
     * unexpected in either mode. {@code MARKED} means the executor has not picked the index up yet, and {@code QUEUED}
     * means it is waiting for a thread; while transitions are disabled both are waiting on a feature that is switched
     * off, so they share the {@code transitions_disabled} diagnosis.
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
            case MARKED -> transitionsEnabled ? MARKED_TRANSITIONS_NOT_STARTED_DIAGNOSIS_DEF : TRANSITIONS_DISABLED_DIAGNOSIS_DEF;
            case QUEUED -> transitionsEnabled ? MARKED_TRANSITIONS_QUEUED_DIAGNOSIS_DEF : TRANSITIONS_DISABLED_DIAGNOSIS_DEF;
            // A current master never publishes running indices. One running an older version still can, and a running
            // transition is making progress, so it raises no diagnosis.
            case RUNNING -> null;
        };
    }

    /**
     * Groups the published sample of overdue indices by the state they are stuck in, resolving each to its display
     * name. Names are sorted so that diagnosis resources and details are stable across calls. The sample supplies
     * example names only; diagnoses are driven by {@link DlmFrozenTransitionsHealthInfo#overdueIndicesCountByState()}.
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
        if (info.overdueIndicesCountByState().isEmpty() == false) {
            // Use a sorted map so the JSON output has a stable key order.
            Map<String, Integer> countByStateForJson = new TreeMap<>();
            info.overdueIndicesCountByState().forEach((state, count) -> countByStateForJson.put(state.toString(), count));
            details.put("overdue_indices_count_by_state", countByStateForJson);
        }
        if (info.overdueIndices().isEmpty() == false) {
            details.put("overdue_indices_sample", overdueIndexDetails(info, supportsMultipleProjects));
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
