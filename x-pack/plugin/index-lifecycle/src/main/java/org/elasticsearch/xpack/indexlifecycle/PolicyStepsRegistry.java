/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class PolicyStepsRegistry {


    // keeps track of existing policies in the cluster state
    SortedMap<String, LifecyclePolicy> lifecyclePolicyMap;
    // keeps track of what the first step in a policy is
    Map<String, Step> firstStepMap;
    // keeps track of a mapping from policy/step-name to respective Step
    Map<String, Map<Step.StepKey, Step>> stepMap;

    public PolicyStepsRegistry() {
        this.lifecyclePolicyMap = new TreeMap<>();
        this.firstStepMap = new HashMap<>();
        this.stepMap = new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    public void update(ClusterState currentState) {
        IndexLifecycleMetadata meta = currentState.metaData().custom(IndexLifecycleMetadata.TYPE);
        Diff<Map<String, LifecyclePolicy>> diff = DiffableUtils.diff(lifecyclePolicyMap, meta.getPolicies(), DiffableUtils.getStringKeySerializer());
        DiffableUtils.MapDiff<String, LifecyclePolicy, DiffableUtils.KeySerializer<String>> mapDiff = (DiffableUtils.MapDiff) diff;
        if (mapDiff.getUpserts().isEmpty() == false) {
            for (LifecyclePolicy policy : mapDiff.getUpserts().values()) {
                lifecyclePolicyMap.put(policy.getName(), policy);
                List<Step> policyAsSteps = policy.toSteps();
                if (policyAsSteps.isEmpty() == false) {
                    firstStepMap.put(policy.getName(), policyAsSteps.get(0));
                    Map<Step.StepKey, Step> stepMapForPolicy = stepMap.put(policy.getName(), new HashMap<>());
                    for (Step step : policyAsSteps) {
                        stepMapForPolicy.put(new Step.StepKey(step.getPhase(), step.getAction(), step.getName()), step);
                    }
                }
            }
        }

        for (String deletedPolicyName : mapDiff.getDeletes()) {
            lifecyclePolicyMap.remove(deletedPolicyName);
            firstStepMap.remove(deletedPolicyName);
            stepMap.remove(deletedPolicyName);
        }
    }

    /**
     * returns the {@link Step} that matches the name and
     * policy specified. This is used by {@link ClusterState}
     * readers that know the current policy and step by name
     * as String values in the cluster state.
     * @param policy the policy from which to fetch the associated steps from
     * @param stepKey the key to the requested {@link Step}
     * @return
     */
    public Step getStep(String policy, Step.StepKey stepKey) {
        Step step = stepMap.getOrDefault(policy, Collections.emptyMap()).get(stepKey);
        if (step == null) {
            step = firstStepMap.get(policy);
        }
        return step;
    }

    public Step getFirstStep(String policy) {
        return firstStepMap.get(policy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lifecyclePolicyMap, firstStepMap, stepMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PolicyStepsRegistry other = (PolicyStepsRegistry) obj;
        return Objects.equals(lifecyclePolicyMap, other.lifecyclePolicyMap)
            && Objects.equals(firstStepMap, other.firstStepMap) && Objects.equals(stepMap, other.stepMap);
    }

}
