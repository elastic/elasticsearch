/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class PolicyStepsRegistry {


    // keeps track of existing policies in the cluster state
    SortedMap<String, LifecyclePolicy> lifecyclePolicyMap;
    // keeps track of what the first step in a policy is
    Map<String, Step> firstStepMap;
    // keeps track of a mapping from step-name to respective Step
    Map<StepKey, Step> stepMap;

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
                    for (Step step : policyAsSteps) {
                        stepMap.put(new StepKey(step.getPhase(), step.getAction(), step.getName()), step);
                    }
                }
            }
        }

        for (String deletedPolicyName : mapDiff.getDeletes()) {
            LifecyclePolicy policy = lifecyclePolicyMap.remove(deletedPolicyName);
            Step next = firstStepMap.remove(deletedPolicyName);
            while (next.hasNextStep()) {
                next = stepMap.remove(next.getNextStep());
            }
        }
    }

    /**
     * returns the {@link Step} that matches the name and
     * policy specified. This is used by {@link ClusterState}
     * readers that know the current policy and step by name
     * as String values in the cluster state.
     * @param policy the policy from which to fetch the associated steps from
     * @param phase the phase the requested step is run in
     * @param action the action the requested step is run in
     * @param name the name of the requested step
     * @return
     */
    public Step getStep(String policy, @Nullable String phase, @Nullable String action, @Nullable String name) {
        Step step = stepMap.get(new StepKey(phase, action, name));
        if (step == null) {
            step = firstStepMap.get(policy);
        }
        return step;
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

    public class StepKey {
        private final String phase;

        private final String action;
        private final String name;

        public StepKey(String phase, String action, String name) {
            this.phase = phase;
            this.action = action;
            this.name = name;
        }

        public String getPhase() {
            return phase;
        }

        public String getAction() {
            return action;
        }

        public String getName() {
            return name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(phase, action, name);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            StepKey other = (StepKey) obj;
            return Objects.equals(phase, other.phase) && Objects.equals(action, other.action) && Objects.equals(name, other.name);
        }
    }
}
