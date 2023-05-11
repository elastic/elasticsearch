/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
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

    private static final Logger logger = LogManager.getLogger(IlmHealthIndicatorService.class);

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
    public static final String INDEX_STUCK_IMPACT_ID = "index_stuck";
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

    public static final List<HealthIndicatorImpact> INDEX_STUCK_IMPACT = List.of(
        new HealthIndicatorImpact(
            NAME,
            INDEX_STUCK_IMPACT_ID,
            3,
            "Some indices have been longer than expected on the same Index Lifecycle Management action. The performance and stability of "
                + "the cluster could be impacted.",
            List.of(ImpactArea.INGEST, ImpactArea.SEARCH)
        )
    );

    // TODO fix this value
    public static final TimeValue ONE_DAY = TimeValue.timeValueSeconds(2);

    private static final Map<String, RuleConfig> RULES_BY_ACTION_CONFIG = Map.of(
        RolloverAction.NAME,
        new RuleConfig(ONE_DAY, 100),
        MigrateAction.NAME,
        new RuleConfig(ONE_DAY, 100),
        SearchableSnapshotAction.NAME,
        new RuleConfig(ONE_DAY, 100),
        DeleteStep.NAME,
        new RuleConfig(ONE_DAY, 100),
        ShrinkAction.NAME,
        new RuleConfig(ONE_DAY, 100),
        AllocateAction.NAME,
        new RuleConfig(ONE_DAY, 100),
        ForceMergeAction.NAME,
        new RuleConfig(ONE_DAY, 100),
        DownsampleAction.NAME,
        new RuleConfig(ONE_DAY, 100)
    );

    private static final Map<String, Function<String, Diagnosis.Definition>> ACTION_STUCK_DEFINITIONS = RULES_BY_ACTION_CONFIG.entrySet()
        .stream()
        .collect(
            Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                entry -> ilmPolicyName -> new Diagnosis.Definition(
                    NAME,
                    "stuck_action:" + entry.getKey(),
                    "Some indices managed by the policy ["
                        + ilmPolicyName
                        + "] have been stuck on the action ["
                        + entry.getKey()
                        + "] longer than the expected time [time spent in action: "
                        + entry.getValue().maxTimeOn()
                        + "].",
                    "Check the current status of the Index Lifecycle Management service using the [/_ilm/explain] API.",
                    "https://ela.st/ilm-explain"
                )
            )
        );

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final LongSupplier nowSupplier;

    public IlmHealthIndicatorService(
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        LongSupplier nowSupplier
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.nowSupplier = nowSupplier;
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
            return calculateIndicator(verbose, ilmMetadata, currentMode, maxAffectedResourcesCount);
        }
    }

    private HealthIndicatorDetails createDetails(boolean verbose, IndexLifecycleMetadata ilmMetadata, OperationMode currentMode) {
        return createDetails(verbose, ilmMetadata, currentMode, List.of());
    }

    private HealthIndicatorResult calculateIndicator(
        boolean verbose,
        IndexLifecycleMetadata ilmMetadata,
        OperationMode currentMode,
        int maxAffectedResourcesCount
    ) {
        var stuckIndices = findIndicesManagedByIlm().filter(IlmRuleEvaluator.ILM_RULE_EVALUATOR::isStuck).toList();

        if (stuckIndices.isEmpty()) {
            return createIndicator(
                GREEN,
                "Index Lifecycle Management is running",
                createDetails(verbose, ilmMetadata, currentMode, stuckIndices),
                Collections.emptyList(),
                Collections.emptyList()
            );
        }

        return createIndicator(
            YELLOW,
            (stuckIndices.size() > 1 ? "Some indices have" : "An index has") + " been stuck on the same action longer than expected.",
            createDetails(verbose, ilmMetadata, currentMode, stuckIndices),
            INDEX_STUCK_IMPACT,
            createDiagnoses(stuckIndices, maxAffectedResourcesCount)
        );
    }

    private List<Diagnosis> createDiagnoses(List<CurrentState> stuckIndices, int maxAffectedResourcesCount) {
        return stuckIndices.stream()
            .collect(groupingBy(cs -> Tuple.tuple(cs.action, cs.policyName)))
            .entrySet()
            .stream()
            .map(actionPolicyTuple -> {
                var affectedResources = actionPolicyTuple.getValue()
                    .stream()
                    .map(c -> c.indexName)
                    .limit(Math.min(maxAffectedResourcesCount, actionPolicyTuple.getValue().size()))
                    .toList();
                return new Diagnosis(
                    ACTION_STUCK_DEFINITIONS.get(actionPolicyTuple.getKey().v1()).apply(actionPolicyTuple.getKey().v2()),
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, affectedResources))
                );
            })
            .toList();
    }

    private Stream<CurrentState> findIndicesManagedByIlm() {
        var concreteIndices = indexNameExpressionResolver.concreteIndices(
            clusterService.state(),
            IndicesOptions.STRICT_EXPAND_OPEN,
            true,
            "*"
        );
        var metadata = clusterService.state().metadata();

        return Arrays.stream(concreteIndices).map(metadata::index).filter(metadata::isIndexManagedByILM).map(indexMetadata -> {
            var ilmExecutionState = indexMetadata.getLifecycleExecutionState();
            var now = nowSupplier.getAsLong();
            return new CurrentState(
                indexMetadata.getIndex().getName(),
                indexMetadata.getLifecyclePolicyName(),
                ilmExecutionState.phase(),
                ilmExecutionState.action(),
                ilmExecutionState.actionTime() != null ? TimeValue.timeValueMillis(now - ilmExecutionState.actionTime()) : TimeValue.ZERO,
                ilmExecutionState.step(),
                ilmExecutionState.stepTime() != null ? TimeValue.timeValueMillis(now - ilmExecutionState.stepTime()) : TimeValue.ZERO,
                ilmExecutionState.failedStepRetryCount()
            );
        });
    }

    private static HealthIndicatorDetails createDetails(
        boolean verbose,
        IndexLifecycleMetadata metadata,
        OperationMode mode,
        List<CurrentState> stuckIndices
    ) {
        if (verbose == false) {
            return HealthIndicatorDetails.EMPTY;
        }

        var details = new HashMap<String, Object>();

        details.put("ilm_status", mode);
        details.put("policies", metadata.getPolicies().size());
        details.put("stuck_indices", stuckIndices.size());

        var indicesStuckPerAction = stuckIndices.stream().collect(groupingBy(CurrentState::action, counting()));

        if (indicesStuckPerAction.isEmpty() == false) {
            RULES_BY_ACTION_CONFIG.forEach((action, value) -> indicesStuckPerAction.putIfAbsent(action, 0L));
            details.put("stuck_indices_per_action", indicesStuckPerAction);
        }

        return new SimpleHealthIndicatorDetails(details);
    }

    static class IlmRuleEvaluator {

        private static Predicate<CurrentState> actionAndMaxTimeOn(String action) {
            assert RULES_BY_ACTION_CONFIG.get(action) != null;
            return cs -> action.equals(cs.action) && RULES_BY_ACTION_CONFIG.get(action).maxTimeOn.compareTo(cs.timeOnAction) < 0;
        }

        private static final IlmRuleEvaluator ILM_RULE_EVALUATOR = new IlmRuleEvaluator(
            List.of(
                actionAndMaxTimeOn(RolloverAction.NAME),
                actionAndMaxTimeOn(MigrateAction.NAME),
                actionAndMaxTimeOn(SearchableSnapshotAction.NAME),
                actionAndMaxTimeOn(DeleteStep.NAME),
                actionAndMaxTimeOn(ShrinkAction.NAME),
                actionAndMaxTimeOn(AllocateAction.NAME),
                actionAndMaxTimeOn(ForceMergeAction.NAME),
                actionAndMaxTimeOn(DownsampleAction.NAME)
            )
        );
        private final List<Predicate<CurrentState>> rules;

        IlmRuleEvaluator(List<Predicate<CurrentState>> rules) {
            this.rules = rules;
        }

        public boolean isStuck(CurrentState currentState) {
            return rules.stream().anyMatch(r -> r.test(currentState));
        }
    }

    private record RuleConfig(TimeValue maxTimeOn, long maxRetries) {}

    private record CurrentState(
        String indexName,
        String policyName,
        String phase,
        String action,
        TimeValue timeOnAction,
        String step,
        TimeValue timeOnStep,
        Integer stepRetries
    ) {}

}
