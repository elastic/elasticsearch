/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.LongSupplier;

public class PolicyStepsRegistry {
    // keeps track of existing policies in the cluster state
    private SortedMap<String, LifecyclePolicy> lifecyclePolicyMap;
    // keeps track of what the first step in a policy is
    private Map<String, Step> firstStepMap;
    // keeps track of a mapping from policy/step-name to respective Step
    private Map<String, Map<Step.StepKey, Step>> stepMap;

    public PolicyStepsRegistry() {
        this.lifecyclePolicyMap = new TreeMap<>();
        this.firstStepMap = new HashMap<>();
        this.stepMap = new HashMap<>();
    }

    PolicyStepsRegistry(SortedMap<String, LifecyclePolicy> lifecyclePolicyMap,
                        Map<String, Step> firstStepMap, Map<String, Map<Step.StepKey, Step>> stepMap) {
        this.lifecyclePolicyMap = lifecyclePolicyMap;
        this.firstStepMap = firstStepMap;
        this.stepMap = stepMap;
    }

    SortedMap<String, LifecyclePolicy> getLifecyclePolicyMap() {
        return lifecyclePolicyMap;
    }

    Map<String, Step> getFirstStepMap() {
        return firstStepMap;
    }

    Map<String, Map<Step.StepKey, Step>> getStepMap() {
        return stepMap;
    }


    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void update(ClusterState currentState, Client client, LongSupplier nowSupplier) {
        IndexLifecycleMetadata meta = currentState.metaData().custom(IndexLifecycleMetadata.TYPE);
        Diff<Map<String, LifecyclePolicy>> diff = DiffableUtils.diff(lifecyclePolicyMap, meta.getPolicies(),
            DiffableUtils.getStringKeySerializer());
        DiffableUtils.MapDiff<String, LifecyclePolicy, DiffableUtils.KeySerializer<String>> mapDiff = (DiffableUtils.MapDiff) diff;
        if (mapDiff.getUpserts().isEmpty() == false) {
            for (LifecyclePolicy policy : mapDiff.getUpserts().values()) {
                lifecyclePolicyMap.put(policy.getName(), policy);
                List<Step> policyAsSteps = policy.toSteps(client, nowSupplier);
                if (policyAsSteps.isEmpty() == false) {
                    firstStepMap.put(policy.getName(), policyAsSteps.get(0));
                    stepMap.put(policy.getName(), new HashMap<>());
                    Map<Step.StepKey, Step> stepMapForPolicy = stepMap.get(policy.getName());
                    for (Step step : policyAsSteps) {
                        assert ErrorStep.NAME.equals(step.getKey().getName()) == false;
                        stepMapForPolicy.put(step.getKey(), step);
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
     * @return step
     */
    public Step getStep(String policy, Step.StepKey stepKey) {
        if (ErrorStep.NAME.equals(stepKey.getName())) {
            return new ErrorStep(new Step.StepKey(stepKey.getPhase(), stepKey.getAction(), ErrorStep.NAME));
        }
        Map<Step.StepKey, Step> steps = stepMap.get(policy);
        if (steps == null) {
            throw new IllegalStateException("policy [" + policy + "] does not exist");
        }
        Step step = steps.get(stepKey);
        if (step == null) {
            throw new IllegalStateException("step [" + stepKey + "] does not exist");
        }
        return step;
    }

    public Step getFirstStep(String policy) {
        return firstStepMap.get(policy);
    }

}
