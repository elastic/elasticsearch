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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Stream;

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
        final ClusterState currentState = clusterService.state();
        var ilmMetadata = currentState.metadata().custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        final OperationMode currentMode = currentILMMode(currentState);
        if (ilmMetadata.getPolicyMetadatas().isEmpty()) {
            return createIndicator(
                GREEN,
                "No Index Lifecycle Management policies configured",
                createDetails(verbose, ilmMetadata, currentMode),
                Collections.emptyList(),
                Collections.emptyList()
            );
        } else if (currentMode != OperationMode.RUNNING) {
            List<HealthIndicatorImpact> impacts = Collections.singletonList(
                new HealthIndicatorImpact(
                    NAME,
                    AUTOMATION_DISABLED_IMPACT_ID,
                    3,
                    "Automatic index lifecycle and data retention management is disabled. The performance and stability of the cluster "
                        + "could be impacted.",
                    List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                )
            );
            return createIndicator(
                YELLOW,
                "Index Lifecycle Management is not running",
                createDetails(verbose, ilmMetadata, currentMode),
                impacts,
                List.of(ILM_NOT_RUNNING)
            );
        } else {
            return calculateIndicator(verbose, ilmMetadata, currentMode);
        }
    }

    private HealthIndicatorResult calculateIndicator(boolean verbose, IndexLifecycleMetadata ilmMetadata, OperationMode currentMode) {
        findIndices().filter(IlmRuleEvaluator.ILM_RULE_EVALUATOR::evaluate);

        return createIndicator(
            GREEN,
            "Index Lifecycle Management is running",
            createDetails(verbose, ilmMetadata, currentMode),
            Collections.emptyList(),
            Collections.emptyList()
        );
    }

    private Stream<CurrentState> findIndices() {
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
                Duration.ofMillis(now - ilmExecutionState.actionTime()),
                ilmExecutionState.step(),
                Duration.ofMillis(now - ilmExecutionState.stepTime()),
                ilmExecutionState.failedStepRetryCount()
            );
        });
    }

    private static HealthIndicatorDetails createDetails(boolean verbose, IndexLifecycleMetadata metadata, OperationMode mode) {
        if (verbose) {
            return new SimpleHealthIndicatorDetails(Map.of("ilm_status", mode, "policies", metadata.getPolicies().size()));
        } else {
            return HealthIndicatorDetails.EMPTY;
        }
    }

    static class IlmRuleEvaluator {

        public static final Duration ONE_DAY = Duration.ofDays(1);

        private static final IlmRuleEvaluator ILM_RULE_EVALUATOR = new IlmRuleEvaluator(List.of(
        ));
        private final List<Predicate<CurrentState>> rules;

        IlmRuleEvaluator(List<Predicate<CurrentState>> rules) {
            this.rules = rules;
        }

        public boolean evaluate(CurrentState currentState) {
            for (var rule : rules) {
                if (rule.test(currentState)) return true;
            }
            return false;
        }
    }

    static class Predicates {}

    private record CurrentState(
        String indexName,
        String policyName,
        String phase,
        String action,
        Duration timeOnAction,
        String step,
        Duration timeOnStep,
        Integer stepRetries
    ) {}

}
