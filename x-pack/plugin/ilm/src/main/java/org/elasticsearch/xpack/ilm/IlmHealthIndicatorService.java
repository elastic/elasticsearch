/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteStep;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.WaitForActiveShardsStep;
import org.elasticsearch.xpack.core.ilm.WaitForDataTierStep;
import org.elasticsearch.xpack.core.ilm.WaitForIndexColorStep;
import org.elasticsearch.xpack.core.ilm.WaitForNoFollowersStep;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;

/**
 * This indicator reports health for index lifecycle management component.
 *
 * Indicator will report YELLOW status when ILM is not running and there are configured policies.
 * Constant indexing could eventually use entire disk space on hot topology in such cases.
 *
 * ILM must be running to fix warning reported by this indicator.
 */
public class IlmHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "ilm";
    public static final String HELP_URL = "https://ela.st/fix-ilm";
    public static final Diagnosis ILM_NOT_RUNNING = new Diagnosis(
        new Diagnosis.Definition(
            NAME,
            "ilm_disabled",
            "Index Lifecycle Management is stopped",
            "Start Index Lifecycle Management using [POST /_ilm/start].",
            HELP_URL
        ),
        null
    );

    public static final String AUTOMATION_DISABLED_IMPACT_ID = "automation_disabled";
    public static final String STAGNATING_INDEX_IMPACT_ID = "stagnating_index";
    public static final List<HealthIndicatorImpact> AUTOMATION_DISABLED_IMPACT = List.of(
        new HealthIndicatorImpact(
            NAME,
            AUTOMATION_DISABLED_IMPACT_ID,
            3,
            "Automatic index lifecycle and data retention management is disabled. The performance and stability of the cluster "
                + "could be impacted.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );

    public static final List<HealthIndicatorImpact> STAGNATING_INDEX_IMPACT = List.of(
        new HealthIndicatorImpact(
            NAME,
            STAGNATING_INDEX_IMPACT_ID,
            3,
            "Automatic index lifecycle and data retention management cannot make progress on one or more indices. The performance and "
                + "stability of the indices and/or the cluster could be impacted.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );

    public static final TimeValue ONE_DAY = TimeValue.timeValueDays(1);

    static final Map<String, RuleConfig> RULES_BY_ACTION_CONFIG = Stream.of(
        new RuleConfig(RolloverAction.NAME, ONE_DAY, 100, List.of(WaitForActiveShardsStep.NAME)),
        new RuleConfig(MigrateAction.NAME, ONE_DAY, 100),
        new RuleConfig(
            SearchableSnapshotAction.NAME,
            ONE_DAY,
            100,
            List.of(WaitForDataTierStep.NAME, WaitForIndexColorStep.NAME, WaitForNoFollowersStep.NAME)
        ),
        new RuleConfig(DeleteStep.NAME, ONE_DAY, 100),
        new RuleConfig(ShrinkAction.NAME, ONE_DAY, 100, List.of(WaitForNoFollowersStep.NAME)),
        new RuleConfig(AllocateAction.NAME, ONE_DAY, 100),
        new RuleConfig(ForceMergeAction.NAME, ONE_DAY, 100, List.of(WaitForIndexColorStep.NAME)),
        new RuleConfig(DownsampleAction.NAME, ONE_DAY, 100, List.of(WaitForNoFollowersStep.NAME))
    ).collect(toMap(RuleConfig::action, Function.identity()));

    public static final StagnatingIndicesRuleEvaluator ILM_RULE_EVALUATOR = new StagnatingIndicesRuleEvaluator(

        RULES_BY_ACTION_CONFIG.values().stream().map(RuleConfig::toPredicate).toList()
    );

    static final Map<String, Diagnosis.Definition> STAGNATING_ACTION_DEFINITIONS = RULES_BY_ACTION_CONFIG.entrySet()
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                entry -> new Diagnosis.Definition(
                    NAME,
                    "stagnating_action:" + entry.getKey(),
                    "Some indices have been stagnated on the action [" + entry.getKey() + "] longer than the expected time.",
                    "Check the current status of the Index Lifecycle Management service using the [/_ilm/explain] API.",
                    "https://ela.st/ilm-explain"
                )
            )
        );

    private final ClusterService clusterService;
    private final StagnatingIndicesFinder stagnatingIndicesFinder;

    public IlmHealthIndicatorService(ClusterService clusterService, StagnatingIndicesFinder stagnatingIndicesFinder) {
        this.clusterService = clusterService;
        this.stagnatingIndicesFinder = stagnatingIndicesFinder;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        final var currentState = clusterService.state();
        var ilmMetadata = currentState.metadata().custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        final var currentMode = currentILMMode(currentState);
        if (ilmMetadata.getPolicyMetadatas().isEmpty()) {
            return createIndicator(
                GREEN,
                "No Index Lifecycle Management policies configured",
                createDetails(verbose, ilmMetadata, currentMode),
                Collections.emptyList(),
                Collections.emptyList()
            );
        } else if (currentMode != OperationMode.RUNNING) {
            return createIndicator(
                YELLOW,
                "Index Lifecycle Management is not running",
                createDetails(verbose, ilmMetadata, currentMode),
                AUTOMATION_DISABLED_IMPACT,
                List.of(ILM_NOT_RUNNING)
            );
        } else {
            var stagnatingIndices = stagnatingIndicesFinder.find();

            if (stagnatingIndices.isEmpty()) {
                return createIndicator(
                    GREEN,
                    "Index Lifecycle Management is running",
                    createDetails(verbose, ilmMetadata, currentMode),
                    Collections.emptyList(),
                    Collections.emptyList()
                );
            } else {
                return createIndicator(
                    YELLOW,
                    (stagnatingIndices.size() > 1 ? stagnatingIndices.size() + " indices have" : "An index has")
                        + " stayed on the same action longer than expected.",
                    createDetails(verbose, ilmMetadata, currentMode, stagnatingIndices),
                    STAGNATING_INDEX_IMPACT,
                    createDiagnoses(stagnatingIndices, maxAffectedResourcesCount)
                );
            }
        }
    }

    private static HealthIndicatorDetails createDetails(boolean verbose, IndexLifecycleMetadata ilmMetadata, OperationMode currentMode) {
        return createDetails(verbose, ilmMetadata, currentMode, List.of());
    }

    private static List<Diagnosis> createDiagnoses(List<IndexMetadata> stagnatingIndices, int maxAffectedResourcesCount) {
        return stagnatingIndices.stream()
            .collect(groupingBy(md -> md.getLifecycleExecutionState().action()))
            .entrySet()
            .stream()
            .map(action -> {
                var affectedIndices = action.getValue()
                    .stream()
                    .map(IndexMetadata::getIndex)
                    .map(Index::getName)
                    .limit(Math.min(maxAffectedResourcesCount, action.getValue().size()))
                    .collect(Collectors.toCollection(TreeSet::new));
                var affectedPolicies = action.getValue()
                    .stream()
                    .map(IndexMetadata::getLifecyclePolicyName)
                    .limit(Math.min(maxAffectedResourcesCount, action.getValue().size()))
                    .collect(Collectors.toCollection(TreeSet::new));
                return new Diagnosis(
                    STAGNATING_ACTION_DEFINITIONS.get(action.getKey()),
                    List.of(
                        new Diagnosis.Resource(Diagnosis.Resource.Type.ILM_POLICY, affectedPolicies),
                        new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, affectedIndices)
                    )
                );
            })
            .toList();
    }

    private static HealthIndicatorDetails createDetails(
        boolean verbose,
        IndexLifecycleMetadata metadata,
        OperationMode mode,
        List<IndexMetadata> stagnatingIndices
    ) {
        if (verbose == false) {
            return HealthIndicatorDetails.EMPTY;
        }

        var details = new HashMap<String, Object>();

        details.put("ilm_status", mode);
        details.put("policies", metadata.getPolicies().size());
        details.put("stagnating_indices", stagnatingIndices.size());

        var stagnatingIndicesPerAction = stagnatingIndices.stream()
            .collect(groupingBy(md -> md.getLifecycleExecutionState().action(), counting()));

        if (stagnatingIndicesPerAction.isEmpty() == false) {
            RULES_BY_ACTION_CONFIG.forEach((action, value) -> stagnatingIndicesPerAction.putIfAbsent(action, 0L));
            details.put("stagnating_indices_per_action", stagnatingIndicesPerAction);
        }

        return new SimpleHealthIndicatorDetails(details);
    }

    /**
     * Class in charge of find all the indices that are _potentially_ stagnated at some ILM action. To find the indices, it uses a list of
     * rules evaluators (Check {@link org.elasticsearch.xpack.ilm.IlmHealthIndicatorService#RULES_BY_ACTION_CONFIG to the current rules}
     */
    static class StagnatingIndicesFinder {

        private final ClusterService clusterService;
        private final StagnatingIndicesRuleEvaluator stagnatingIndicesRuleEvaluator;
        private final LongSupplier nowSupplier;

        StagnatingIndicesFinder(
            ClusterService clusterService,
            StagnatingIndicesRuleEvaluator stagnatingIndicesRuleEvaluator,
            LongSupplier nowSupplier
        ) {
            this.clusterService = clusterService;
            this.stagnatingIndicesRuleEvaluator = stagnatingIndicesRuleEvaluator;
            this.nowSupplier = nowSupplier;
        }

        /**
         * @return A list containing info about the stagnating indices in the cluster.
         */
        public List<IndexMetadata> find() {
            var metadata = clusterService.state().metadata();
            var now = nowSupplier.getAsLong();
            return metadata.indices()
                .values()
                .stream()
                .filter(metadata::isIndexManagedByILM)
                .filter(md -> stagnatingIndicesRuleEvaluator.isStagnated(now, md))
                .toList();
        }
    }

    record StagnatingIndicesRuleEvaluator(List<BiPredicate<Long, IndexMetadata>> rules) {
        public boolean isStagnated(Long now, IndexMetadata indexMetadata) {
            return rules.stream().anyMatch(r -> r.test(now, indexMetadata));
        }
    }

    private record RuleConfig(String action, TimeValue maxTimeOn, long maxRetries, List<String> stepsToCheck) {
        RuleConfig(String action, TimeValue maxTimeOn, long maxRetries) {
            this(action, maxTimeOn, maxRetries, null);
        }

        public BiPredicate<Long, IndexMetadata> toPredicate() {
            return stepsToCheck == null || stepsToCheck.isEmpty() ? this::onlyCheckAction : this::checkActionAndStep;
        }

        private boolean checkActionAndStep(Long now, IndexMetadata indexMetadata) {
            var currentAction = indexMetadata.getLifecycleExecutionState().action();
            var currentStep = indexMetadata.getLifecycleExecutionState().step();
            var currentStepRetries = indexMetadata.getLifecycleExecutionState().failedStepRetryCount();
            var currentTimeOnAction = safelyGetTimeValue(now, indexMetadata.getLifecycleExecutionState().actionTime());
            return action.equals(currentAction)
                && stepsToCheck.contains(currentStep)
                && (maxTimeOn.compareTo(currentTimeOnAction) < 0 || currentStepRetries > maxRetries);
        }

        private boolean onlyCheckAction(Long now, IndexMetadata indexMetadata) {
            var currentAction = indexMetadata.getLifecycleExecutionState().action();
            var currentTimeOnAction = safelyGetTimeValue(now, indexMetadata.getLifecycleExecutionState().actionTime());
            return action.equals(currentAction) && maxTimeOn.compareTo(currentTimeOnAction) < 0;
        }

        private static TimeValue safelyGetTimeValue(Long now, Long currentTime) {
            return currentTime == null ? TimeValue.ZERO : TimeValue.timeValueMillis(now - currentTime);
        }
    }
}
