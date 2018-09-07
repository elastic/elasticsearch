/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.TerminalPolicyStep;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PolicyStepsRegistry {
    private static final Logger logger = LogManager.getLogger(PolicyStepsRegistry.class);

    private final Client client;
    // keeps track of existing policies in the cluster state
    private final SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap;
    // keeps track of what the first step in a policy is, the key is policy name
    private final Map<String, Step> firstStepMap;
    // keeps track of a mapping from policy/step-name to respective Step, the key is policy name
    private final Map<String, Map<Step.StepKey, Step>> stepMap;
    // A map of index to a list of compiled steps for the current phase
    private final Map<Index, List<Step>> indexPhaseSteps;
    private final NamedXContentRegistry xContentRegistry;

    public PolicyStepsRegistry(NamedXContentRegistry xContentRegistry, Client client) {
        this.lifecyclePolicyMap = new TreeMap<>();
        this.firstStepMap = new HashMap<>();
        this.stepMap = new HashMap<>();
        this.indexPhaseSteps = new HashMap<>();
        this.xContentRegistry = xContentRegistry;
        this.client = client;
    }

    PolicyStepsRegistry(SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap,
                        Map<String, Step> firstStepMap, Map<String, Map<Step.StepKey, Step>> stepMap,
                        Map<Index, List<Step>> indexPhaseSteps, NamedXContentRegistry xContentRegistry, Client client) {
        this.lifecyclePolicyMap = lifecyclePolicyMap;
        this.firstStepMap = firstStepMap;
        this.stepMap = stepMap;
        this.indexPhaseSteps = indexPhaseSteps;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
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

    /**
     * Remove phase step lists for indices that have been deleted
     * @param indices a list of indices that have been deleted
     */
    public void removeIndices(List<Index> indices) {
        indices.forEach(index -> {
            logger.trace("removing cached phase steps for deleted index [{}]", index.getName());
            indexPhaseSteps.remove(index);
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void update(ClusterState clusterState) {
        final IndexLifecycleMetadata meta = clusterState.metaData().custom(IndexLifecycleMetadata.TYPE);

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
                List<Step> policyAsSteps = policyMetadata.getPolicy().toSteps(policyClient);
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
            final Index index = imd.value.getIndex();
            final String policy = imd.value.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
            if (policy == null || lifecyclePolicyMap.containsKey(policy) == false) {
                indexPhaseSteps.remove(index);
            } else {
                final List<Step> currentSteps = indexPhaseSteps.get(index);
                // Get the current steps' phase, if there are steps stored
                final String existingPhase = (currentSteps == null || currentSteps.size() == 0) ?
                    "_none_" : currentSteps.get(0).getKey().getPhase();
                // Retrieve the current phase, defaulting to "new" if no phase is set
                final String currentPhase = imd.value.getSettings().get(LifecycleSettings.LIFECYCLE_PHASE,
                    InitializePolicyContextStep.INITIALIZATION_PHASE);

                if (existingPhase.equals(currentPhase) == false) {
                    logger.debug("index [{}] has transitioned phases [{} -> {}], rebuilding step list",
                        index, existingPhase, currentPhase);
                    // parse existing phase steps from the phase definition in the index settings
                    String phaseDef = imd.value.getSettings().get(LifecycleSettings.LIFECYCLE_PHASE_DEFINITION,
                        InitializePolicyContextStep.INITIALIZATION_PHASE);
                    final Phase phase;
                    LifecyclePolicy currentPolicy = lifecyclePolicyMap.get(policy).getPolicy();
                    final LifecyclePolicy policyToExecute;
                    if (InitializePolicyContextStep.INITIALIZATION_PHASE.equals(phaseDef)
                            || TerminalPolicyStep.COMPLETED_PHASE.equals(phaseDef)) {
                        // It is ok to re-use potentially modified policy here since we are in an initialization or completed phase
                        policyToExecute = currentPolicy;
                    } else {
                        // if the current phase definition describes an internal step/phase, do not parse
                        try (XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, phaseDef)) {
                            phase = Phase.parse(parser, currentPhase);
                        } catch (IOException e) {
                            logger.error("failed to configure phase [" + currentPhase + "] for index [" + index.getName() + "]", e);
                            indexPhaseSteps.remove(index);
                            continue;
                        }
                        Map<String, Phase> phaseMap = new HashMap<>(currentPolicy.getPhases());
                        if (phase != null) {
                            phaseMap.put(currentPhase, phase);
                        }
                        policyToExecute = new LifecyclePolicy(currentPolicy.getType(), currentPolicy.getName(), phaseMap);
                    }
                    LifecyclePolicySecurityClient policyClient = new LifecyclePolicySecurityClient(client,
                        ClientHelper.INDEX_LIFECYCLE_ORIGIN, lifecyclePolicyMap.get(policy).getHeaders());
                    final List<Step> steps = policyToExecute.toSteps(policyClient);
                    // Build a list of steps that correspond with the phase the index is currently in
                    final List<Step> phaseSteps;
                    if (steps == null) {
                        phaseSteps = new ArrayList<>();
                    } else {
                        phaseSteps = steps.stream()
                            .filter(e -> e.getKey().getPhase().equals(currentPhase))
                            .collect(Collectors.toList());
                    }
                    indexPhaseSteps.put(index, phaseSteps);
                }
            }
        }
    }

    /**
     * returns the {@link Step} that matches the index name and
     * stepkey specified. This is used by {@link ClusterState}
     * readers that know the current policy and step by name
     * as String values in the cluster state.
     * @param index the index to get the step for
     * @param stepKey the key to the requested {@link Step}
     * @return the step for the given stepkey or null if the step was not found
     */
    @Nullable
    public Step getStep(final Index index, final Step.StepKey stepKey) {
        if (ErrorStep.NAME.equals(stepKey.getName())) {
            return new ErrorStep(new Step.StepKey(stepKey.getPhase(), stepKey.getAction(), ErrorStep.NAME));
        }

        if (indexPhaseSteps.get(index) == null) {
            return null;
        }

        if (logger.isTraceEnabled()) {
            logger.trace("[{}]: retrieving step [{}], found: [{}]\nall steps for this phase: [{}]", index, stepKey,
                indexPhaseSteps.get(index).stream().filter(step -> step.getKey().equals(stepKey)).findFirst().orElse(null),
                indexPhaseSteps.get(index));
        } else if (logger.isDebugEnabled()) {
            logger.debug("[{}]: retrieving step [{}], found: [{}]", index, stepKey,
                indexPhaseSteps.get(index).stream().filter(step -> step.getKey().equals(stepKey)).findFirst().orElse(null));
        }
        assert indexPhaseSteps.get(index).stream().allMatch(step -> step.getKey().getPhase().equals(stepKey.getPhase())) :
            "expected all steps for [" + index + "] to be in phase [" + stepKey.getPhase() +
                "] but they were not, steps: " + indexPhaseSteps.get(index);
        return indexPhaseSteps.get(index).stream().filter(step -> step.getKey().equals(stepKey)).findFirst().orElse(null);
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

    public TimeValue getIndexAgeForPhase(final String policy, final String phase) {
        // These built in phases should never wait
        if (InitializePolicyContextStep.INITIALIZATION_PHASE.equals(phase) || TerminalPolicyStep.COMPLETED_PHASE.equals(phase)) {
            return TimeValue.ZERO;
        }
        final LifecyclePolicyMetadata meta = lifecyclePolicyMap.get(policy);
        if (meta == null) {
            throw new IllegalArgumentException("no policy found with name \"" + policy + "\"");
        } else {
            final Phase retrievedPhase = meta.getPolicy().getPhases().get(phase);
            if (retrievedPhase == null) {
                // We don't have that phase registered, proceed right through it
                return TimeValue.ZERO;
            } else {
                return retrievedPhase.getAfter();
            }
        }
    }
}
