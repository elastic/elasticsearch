/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class PolicyStepsRegistry {
    private static final Logger logger = ESLoggerFactory.getLogger(PolicyStepsRegistry.class);

    // keeps track of existing policies in the cluster state
    private final SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap;
    // keeps track of what the first step in a policy is, the key is policy name
    private final Map<String, Step> firstStepMap;
    // keeps track of a mapping from policy/step-name to respective Step, the key is policy name
    private final Map<String, Map<Step.StepKey, Step>> stepMap;
    // A map of index name to a list of compiled steps for the current phase
    private final Map<String, List<Step>> indexPhaseSteps;

    public PolicyStepsRegistry() {
        this.lifecyclePolicyMap = new TreeMap<>();
        this.firstStepMap = new HashMap<>();
        this.stepMap = new HashMap<>();
        this.indexPhaseSteps = new HashMap<>();
    }

    PolicyStepsRegistry(SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap,
                        Map<String, Step> firstStepMap, Map<String, Map<Step.StepKey, Step>> stepMap,
                        Map<String, List<Step>> indexPhaseSteps) {
        this.lifecyclePolicyMap = lifecyclePolicyMap;
        this.firstStepMap = firstStepMap;
        this.stepMap = stepMap;
        this.indexPhaseSteps = indexPhaseSteps;
    }

    SortedMap<String, LifecyclePolicyMetadata> getLifecyclePolicyMap() {
        return lifecyclePolicyMap;
    }

    Map<String, Step> getFirstStepMap() {
        return firstStepMap;
    }

    Map<String, Map<Step.StepKey, Step>> getStepMap() {
        return stepMap;
    }


    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void update(ClusterState clusterState, IndexLifecycleMetadata meta, Client client, LongSupplier nowSupplier) {
        assert meta != null : "IndexLifecycleMetadata cannot be null when updating the policy steps registry";

        Diff<Map<String, LifecyclePolicyMetadata>> diff = DiffableUtils.diff(lifecyclePolicyMap, meta.getPolicyMetadatas(),
            DiffableUtils.getStringKeySerializer(),
            // Use a non-diffable value serializer. Otherwise actions in the same
            // action and phase that are changed show up as diffs instead of upserts.
            // We want to treat any change in the policy as an upsert so the map is
            // correctly rebuilt
            new DiffableUtils.NonDiffableValueSerializer<String, LifecyclePolicyMetadata>() {
                @Override
                public void write(LifecyclePolicyMetadata value, StreamOutput out) {
                    // This is never called
                    throw new UnsupportedOperationException("should never be called");
                }

                @Override
                public LifecyclePolicyMetadata read(StreamInput in, String key) {
                    // This is never called
                    throw new UnsupportedOperationException("should never be called");
                }
            });
        DiffableUtils.MapDiff<String, LifecyclePolicyMetadata, DiffableUtils.KeySerializer<String>> mapDiff = (DiffableUtils.MapDiff) diff;

        for (String deletedPolicyName : mapDiff.getDeletes()) {
            lifecyclePolicyMap.remove(deletedPolicyName);
            firstStepMap.remove(deletedPolicyName);
            stepMap.remove(deletedPolicyName);
        }

        if (mapDiff.getUpserts().isEmpty() == false) {
            for (LifecyclePolicyMetadata policyMetadata : mapDiff.getUpserts().values()) {
                LifecyclePolicySecurityClient policyClient = new LifecyclePolicySecurityClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN,
                        policyMetadata.getHeaders());
                lifecyclePolicyMap.put(policyMetadata.getName(), policyMetadata);
                List<Step> policyAsSteps = policyMetadata.getPolicy().toSteps(policyClient, nowSupplier);
                if (policyAsSteps.isEmpty() == false) {
                    firstStepMap.put(policyMetadata.getName(), policyAsSteps.get(0));
                    final Map<Step.StepKey, Step> stepMapForPolicy = new HashMap<>();
                    for (Step step : policyAsSteps) {
                        assert ErrorStep.NAME.equals(step.getKey().getName()) == false : "unexpected error step in policy";
                        stepMapForPolicy.put(step.getKey(), step);
                    }
                    stepMap.put(policyMetadata.getName(), stepMapForPolicy);
                }
            }
        }

        for (ObjectCursor<IndexMetaData> imd : clusterState.metaData().getIndices().values()) {
            final String indexName = imd.value.getIndex().getName();
            final String policy = imd.value.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
            if (policy == null) {
                indexPhaseSteps.remove(indexName);
            } else {
                final List<Step> currentSteps = indexPhaseSteps.get(indexName);
                // Get the current steps' phase, if there are steps stored
                final String existingPhase = (currentSteps == null || currentSteps.size() == 0) ?
                    "_none_" : currentSteps.get(0).getKey().getPhase();
                // Retrieve the current phase, defaulting to "new" if no phase is set
                final String currentPhase = imd.value.getSettings().get(LifecycleSettings.LIFECYCLE_PHASE, "new");
                assert currentPhase != null : "expected a current phase but it was null";

                if (existingPhase.equals(currentPhase) == false) {
                    // Only rebuild the index's steps if the phase of the existing steps does not match our index's current phase
                    final Map<Step.StepKey, Step> steps = stepMap.get(policy);

                    // Build a list of steps that correspond with the phase the index is currently in
                    final List<Step> phaseSteps;
                    if (steps == null) {
                        phaseSteps = new ArrayList<>();
                    } else {
                        phaseSteps = steps.entrySet().stream()
                            .filter(e -> e.getKey().getPhase().equals(currentPhase))
                            .map(Map.Entry::getValue)
                            .collect(Collectors.toList());
                    }
                    indexPhaseSteps.put(indexName, phaseSteps);
                }
            }
        }
    }

    /**
     * returns the {@link Step} that matches the index name and
     * stepkey specified. This is used by {@link ClusterState}
     * readers that know the current policy and step by name
     * as String values in the cluster state.
     * @param indexName the name of the index to get the step for
     * @param stepKey the key to the requested {@link Step}
     * @return the step for the given stepkey or null if the step was not found
     */
    @Nullable
    public Step getStep(final String indexName, final Step.StepKey stepKey) {
        if (ErrorStep.NAME.equals(stepKey.getName())) {
            return new ErrorStep(new Step.StepKey(stepKey.getPhase(), stepKey.getAction(), ErrorStep.NAME));
        }

        if (indexPhaseSteps.get(indexName) == null) {
            return null;
        }

        assert indexPhaseSteps.get(indexName).stream().allMatch(step -> step.getKey().getPhase().equals(stepKey.getPhase())) :
            "expected all steps for [" + indexName + "] to be in phase [" + stepKey.getPhase() +
                "] but they were not, steps: " + indexPhaseSteps.get(indexName);
        return indexPhaseSteps.get(indexName).stream().filter(step -> step.getKey().equals(stepKey)).findFirst().orElse(null);
    }

    /**
     * Given a policy and stepkey, return true if a step exists, false otherwise
     */
    public boolean stepExists(final String policy, final Step.StepKey stepKey) {
        Map<Step.StepKey, Step> steps = stepMap.get(policy);
        if (steps == null) {
            return false;
        } else {
            return steps.containsKey(stepKey);
        }
    }

    public Step getFirstStep(String policy) {
        return firstStepMap.get(policy);
    }

}
