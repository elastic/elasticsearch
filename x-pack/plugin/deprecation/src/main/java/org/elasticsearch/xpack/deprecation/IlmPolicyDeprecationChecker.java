/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.Phase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.deprecation.DeprecationInfoAction.filterChecks;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_COMMON_DETAIL;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_HELP_URL;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.DEPRECATION_MESSAGE;
import static org.elasticsearch.xpack.deprecation.LegacyTiersDetection.containsDeprecatedFilteredAllocationConfig;

/**
 * Checks the ILM policies for deprecation warnings.
 */
public class IlmPolicyDeprecationChecker implements ResourceDeprecationChecker {

    public static final String NAME = "ilm_policies";
    private static final List<Function<LifecyclePolicy, DeprecationIssue>> CHECKS = List.of(
        IlmPolicyDeprecationChecker::checkLegacyTiers,
        IlmPolicyDeprecationChecker::checkFrozenAction
    );

    /**
     * @param clusterState The cluster state provided for the checker
     * @return the name of the data streams that have violated the checks with their respective warnings.
     */
    @Override
    public Map<String, List<DeprecationIssue>> check(ClusterState clusterState, DeprecationInfoAction.Request request) {
        IndexLifecycleMetadata lifecycleMetadata = clusterState.metadata().custom(IndexLifecycleMetadata.TYPE);
        if (lifecycleMetadata == null || lifecycleMetadata.getPolicyMetadatas().isEmpty()) {
            return Map.of();
        }
        Map<String, List<DeprecationIssue>> issues = new HashMap<>();
        for (Map.Entry<String, LifecyclePolicyMetadata> entry : lifecycleMetadata.getPolicyMetadatas().entrySet()) {
            String name = entry.getKey();
            LifecyclePolicyMetadata policyMetadata = entry.getValue();

            List<DeprecationIssue> issuesForSinglePolicy = filterChecks(CHECKS, c -> c.apply(policyMetadata.getPolicy()));
            if (issuesForSinglePolicy.isEmpty() == false) {
                issues.put(name, issuesForSinglePolicy);
            }
        }
        return issues.isEmpty() ? Map.of() : issues;
    }

    static DeprecationIssue checkLegacyTiers(LifecyclePolicy policy) {
        for (Phase phase : policy.getPhases().values()) {
            AllocateAction allocateAction = (AllocateAction) phase.getActions().get(AllocateAction.NAME);
            if (allocateAction != null) {
                if (containsDeprecatedFilteredAllocationConfig(allocateAction.getExclude())
                    || containsDeprecatedFilteredAllocationConfig(allocateAction.getInclude())
                    || containsDeprecatedFilteredAllocationConfig(allocateAction.getRequire())) {
                    return new DeprecationIssue(
                        DeprecationIssue.Level.WARNING,
                        DEPRECATION_MESSAGE,
                        DEPRECATION_HELP_URL,
                        "One or more of your ILM policies is configuring tiers via the 'data' node attribute. " + DEPRECATION_COMMON_DETAIL,
                        false,
                        null
                    );
                }
            }
        }
        return null;
    }

    static DeprecationIssue checkFrozenAction(LifecyclePolicy policy) {
        for (Phase phase : policy.getPhases().values()) {
            if (phase.getActions().containsKey(FreezeAction.NAME)) {
                return new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "ILM policy ["
                        + policy.getName()
                        + "] contains the action 'freeze' that is deprecated and will be removed in a future version.",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/frozen-indices.html",
                    "This action is already a noop so it can be safely removed, because frozen indices no longer offer any advantages."
                        + " Consider cold or frozen tiers in place of frozen indices.",
                    false,
                    null
                );
            }
        }
        return null;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
