/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final NamedXContentRegistry xContentRegistry;

    public PolicyStepsRegistry(NamedXContentRegistry xContentRegistry, Client client) {
        this(new TreeMap<>(), new HashMap<>(), new HashMap<>(), xContentRegistry, client);
    }

    PolicyStepsRegistry(SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap,
                        Map<String, Step> firstStepMap, Map<String, Map<Step.StepKey, Step>> stepMap,
                        NamedXContentRegistry xContentRegistry, Client client) {
        this.lifecyclePolicyMap = lifecyclePolicyMap;
        this.firstStepMap = firstStepMap;
        this.stepMap = stepMap;
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void update(ClusterState clusterState) {
        final IndexLifecycleMetadata meta = clusterState.metadata().custom(IndexLifecycleMetadata.TYPE);

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
                    final Map<Step.StepKey, Step> stepMapForPolicy = new LinkedHashMap<>();
                    for (Step step : policyAsSteps) {
                        assert ErrorStep.NAME.equals(step.getKey().getName()) == false : "unexpected error step in policy";
                        stepMapForPolicy.put(step.getKey(), step);
                    }
                    logger.trace("updating cached steps for [{}] policy, new steps: {}",
                        policyMetadata.getName(), stepMapForPolicy.keySet());
                    stepMap.put(policyMetadata.getName(), stepMapForPolicy);
                }
            }
        }
    }

    private List<Step> parseStepsFromPhase(String policy, String currentPhase, String phaseDef) throws IOException {
        final PhaseExecutionInfo phaseExecutionInfo;
        LifecyclePolicyMetadata policyMetadata = lifecyclePolicyMap.get(policy);
        if (policyMetadata == null) {
            throw new IllegalStateException("unable to parse steps for policy [" + policy + "] as it doesn't exist");
        }
        LifecyclePolicy currentPolicy = policyMetadata.getPolicy();
        final LifecyclePolicy policyToExecute;
        if (InitializePolicyContextStep.INITIALIZATION_PHASE.equals(phaseDef)
            || TerminalPolicyStep.COMPLETED_PHASE.equals(phaseDef)) {
            // It is ok to re-use potentially modified policy here since we are in an initialization or completed phase
            policyToExecute = currentPolicy;
        } else {
            // if the current phase definition describes an internal step/phase, do not parse
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, phaseDef)) {
                phaseExecutionInfo = PhaseExecutionInfo.parse(parser, currentPhase);
            }
            Map<String, Phase> phaseMap = new HashMap<>(currentPolicy.getPhases());
            if (phaseExecutionInfo.getPhase() != null) {
                phaseMap.put(currentPhase, phaseExecutionInfo.getPhase());
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
        logger.trace("parsed steps for policy [{}] in phase [{}], definition: [{}], steps: [{}]",
            policy, currentPhase, phaseDef, phaseSteps);
        return phaseSteps;
    }

    @Nullable
    public Step getStep(final IndexMetadata indexMetadata, final Step.StepKey stepKey) {
        if (ErrorStep.NAME.equals(stepKey.getName())) {
            return new ErrorStep(new Step.StepKey(stepKey.getPhase(), stepKey.getAction(), ErrorStep.NAME));
        }

        final String phase = stepKey.getPhase();
        final String policyName = indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
        final Index index = indexMetadata.getIndex();

        if (policyName == null) {
            throw new IllegalArgumentException("failed to retrieve step " + stepKey + " as index [" + index.getName() + "] has no policy");
        }

        // parse phase steps from the phase definition in the index settings
        final String phaseJson = Optional.ofNullable(LifecycleExecutionState.fromIndexMetadata(indexMetadata).getPhaseDefinition())
            .orElse(InitializePolicyContextStep.INITIALIZATION_PHASE);

        final List<Step> phaseSteps;
        try {
            phaseSteps = parseStepsFromPhase(policyName, phase, phaseJson);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to load cached steps for " + stepKey, e);
        } catch (XContentParseException parseErr) {
            throw new XContentParseException(parseErr.getLocation(),
                "failed to load steps for " + stepKey + " from [" + phaseJson + "]", parseErr);
        }

        assert phaseSteps.stream().allMatch(step -> step.getKey().getPhase().equals(phase)) :
            "expected phase steps loaded from phase definition for [" + index.getName() + "] to be in phase [" + phase +
                "] but they were not, steps: " + phaseSteps;

        // Return the step that matches the given stepKey or else null if we couldn't find it
        return phaseSteps.stream().filter(step -> step.getKey().equals(stepKey)).findFirst().orElse(null);
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

    public boolean policyExists(final String policy) {
        return lifecyclePolicyMap.containsKey(policy);
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
                return retrievedPhase.getMinimumAge();
            }
        }
    }
}
