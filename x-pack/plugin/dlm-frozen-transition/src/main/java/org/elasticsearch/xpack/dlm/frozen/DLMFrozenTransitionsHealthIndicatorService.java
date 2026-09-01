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
import org.elasticsearch.health.node.DlmFrozenTransitionIndexInfo;
import org.elasticsearch.health.node.DlmFrozenTransitionsHealthInfo;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.ProjectIndexName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Reports health for the DLM frozen-tier transition feature.
 *
 * <p>Indicator reports YELLOW when frozen transitions are disabled but there is pending work, when the frozen transition
 * service is not running on the current master, or when indices are eligible for frozen conversion but unable to be
 * marked, marked but not yet submitted to the executor, or submitted but queued for longer than the configured stall
 * threshold.
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
            "Data streams backing indices cannot transition to the frozen tier. Data retention and storage cost "
                + "management could be impacted.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );

    public static final Diagnosis.Definition TRANSITIONS_DISABLED_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "transitions_disabled",
        "DLM frozen transitions are disabled but there are indices marked for conversion to the frozen tier.",
        "Enable frozen transitions using a cluster settings update on [dlm.frozen_transitions.enabled].",
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
        HealthIndicatorDetails details = createDetails(verbose, info, supportsMultipleProjects);

        if (info.transitionsEnabled() == false) {
            if (info.markedIndicesCount() > 0) {
                return createIndicator(
                    HealthStatus.YELLOW,
                    "DLM frozen transitions are disabled, but there are indices marked for conversion to the frozen tier",
                    details,
                    FROZEN_TRANSITION_BLOCKED_IMPACT,
                    verbose ? List.of(new Diagnosis(TRANSITIONS_DISABLED_DIAGNOSIS_DEF, null)) : List.of()
                );
            }
            return createIndicator(HealthStatus.GREEN, "DLM frozen transitions are disabled", details, List.of(), List.of());
        }

        if (info.serviceRunning() == false) {
            return createIndicator(
                HealthStatus.YELLOW,
                "The DLM frozen transition service is not running on the current master node",
                details,
                FROZEN_TRANSITION_BLOCKED_IMPACT,
                verbose ? List.of(new Diagnosis(SERVICE_NOT_RUNNING_DIAGNOSIS_DEF, null)) : List.of()
            );
        }

        List<Diagnosis> diagnoses = new ArrayList<>();
        if (info.eligibleUnmarked().isEmpty() == false) {
            addDiagnosis(
                diagnoses,
                info.defaultRepositoryConfigured()
                    ? ELIGIBLE_INDICES_UNMARKED_DIAGNOSIS_DEF
                    : ELIGIBLE_INDICES_UNMARKED_NO_REPOSITORY_DIAGNOSIS_DEF,
                toIndexNames(
                    info.eligibleUnmarked().sample(),
                    i -> new ProjectIndexName(i.projectId(), i.indexName()),
                    supportsMultipleProjects,
                    maxAffectedResourcesCount
                )
            );
        }
        if (info.notStartedMarked().isEmpty() == false) {
            addDiagnosis(
                diagnoses,
                MARKED_TRANSITIONS_NOT_STARTED_DIAGNOSIS_DEF,
                toIndexNames(
                    info.notStartedMarked().sample(),
                    i -> new ProjectIndexName(i.projectId(), i.indexName()),
                    supportsMultipleProjects,
                    maxAffectedResourcesCount
                )
            );
        }
        if (info.queuedMarked().isEmpty() == false) {
            addDiagnosis(
                diagnoses,
                MARKED_TRANSITIONS_QUEUED_DIAGNOSIS_DEF,
                toIndexNames(
                    info.queuedMarked().sample(),
                    i -> new ProjectIndexName(i.projectId(), i.indexName()),
                    supportsMultipleProjects,
                    maxAffectedResourcesCount
                )
            );
        }

        if (diagnoses.isEmpty()) {
            return createIndicator(
                HealthStatus.GREEN,
                "DLM frozen transitions are executing without issues",
                details,
                List.of(),
                List.of()
            );
        }

        String symptom;
        if (diagnoses.size() > 1) {
            symptom = diagnoses.size() + " issues affecting DLM frozen-tier transitions were detected";
        } else {
            symptom = "An issue affecting DLM frozen-tier transitions was detected";
        }
        return createIndicator(HealthStatus.YELLOW, symptom, details, FROZEN_TRANSITION_BLOCKED_IMPACT, verbose ? diagnoses : List.of());
    }

    private static <T> List<String> toIndexNames(
        List<T> items,
        Function<T, ProjectIndexName> toName,
        boolean supportsMultipleProjects,
        int limit
    ) {
        return items.stream().limit(limit).map(i -> toName.apply(i).toString(supportsMultipleProjects)).toList();
    }

    private static void addDiagnosis(List<Diagnosis> diagnoses, Diagnosis.Definition definition, List<String> indexNames) {
        diagnoses.add(new Diagnosis(definition, List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, indexNames))));
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
        details.put("marked_indices_count", info.markedIndicesCount());
        details.put("eligible_unmarked_indices_count", info.eligibleUnmarked().totalCount());
        details.put("not_started_marked_indices_count", info.notStartedMarked().totalCount());
        details.put("queued_marked_indices_count", info.queuedMarked().totalCount());
        if (info.eligibleUnmarked().isEmpty() == false) {
            details.put("eligible_unmarked_indices", indexInfoDetails(info.eligibleUnmarked().sample(), supportsMultipleProjects));
        }
        if (info.notStartedMarked().isEmpty() == false) {
            details.put("not_started_marked_indices", indexInfoDetails(info.notStartedMarked().sample(), supportsMultipleProjects));
        }
        if (info.queuedMarked().isEmpty() == false) {
            details.put("queued_marked_indices", indexInfoDetails(info.queuedMarked().sample(), supportsMultipleProjects));
        }
        return new SimpleHealthIndicatorDetails(details);
    }

    private static List<Map<String, Object>> indexInfoDetails(
        List<DlmFrozenTransitionIndexInfo> indices,
        boolean supportsMultipleProjects
    ) {
        return indices.stream().<Map<String, Object>>map(index -> {
            LinkedHashMap<String, Object> entry = new LinkedHashMap<>(2, 1.0f);
            entry.put("index_name", new ProjectIndexName(index.projectId(), index.indexName()).toString(supportsMultipleProjects));
            entry.put("stalled_since_timestamp", index.stalledSinceMillis());
            return entry;
        }).toList();
    }
}
