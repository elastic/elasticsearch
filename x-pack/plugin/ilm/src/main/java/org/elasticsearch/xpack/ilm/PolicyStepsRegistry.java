/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicySecurityClient;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PolicyStepsRegistry {
    private static final Logger logger = LogManager.getLogger(PolicyStepsRegistry.class);

    private final NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final XPackLicenseState licenseState;

    // keeps track of existing policies in the cluster state
    private final SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap;
    // keeps track of what the first step in a policy is, the key is policy name
    private final Map<String, Step> firstStepMap;
    // keeps track of a mapping from policy/step-name to respective Step, the key is policy name
    private final Map<String, Map<Step.StepKey, Step>> stepMap;

    // tracks an index->step cache, where the indexmetadata is also tracked for cache invalidation/eviction purposes.
    // for a given index, the step can be cached as long as the indexmetadata (and the policy!) hasn't changed. since
    // policies change infrequently, the entire cache is cleared on policy change.
    private final Map<Index, Tuple<IndexMetadata, Step>> cachedSteps = new ConcurrentHashMap<>();

    public PolicyStepsRegistry(NamedXContentRegistry xContentRegistry, Client client, XPackLicenseState licenseState) {
        this(new TreeMap<>(), new HashMap<>(), new HashMap<>(), xContentRegistry, client, licenseState);
    }

    PolicyStepsRegistry(
        SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap,
        Map<String, Step> firstStepMap,
        Map<String, Map<Step.StepKey, Step>> stepMap,
        NamedXContentRegistry xContentRegistry,
        Client client,
        XPackLicenseState licenseState
    ) {
        this.lifecyclePolicyMap = lifecyclePolicyMap;
        this.firstStepMap = firstStepMap;
        this.stepMap = stepMap;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.licenseState = licenseState;
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

    public void update(IndexLifecycleMetadata meta) {
        assert meta != null : "IndexLifecycleMetadata cannot be null when updating the policy steps registry";

        DiffableUtils.MapDiff<String, LifecyclePolicyMetadata, Map<String, LifecyclePolicyMetadata>> mapDiff = DiffableUtils.diff(
            lifecyclePolicyMap,
            meta.getPolicyMetadatas(),
            DiffableUtils.getStringKeySerializer(),
            // Use a non-diffable value serializer. Otherwise actions in the same
            // action and phase that are changed show up as diffs instead of upserts.
            // We want to treat any change in the policy as an upsert so the map is
            // correctly rebuilt
            new DiffableUtils.NonDiffableValueSerializer<>() {
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
            }
        );

        for (String deletedPolicyName : mapDiff.getDeletes()) {
            lifecyclePolicyMap.remove(deletedPolicyName);
            firstStepMap.remove(deletedPolicyName);
            stepMap.remove(deletedPolicyName);
        }

        if (mapDiff.getUpserts().isEmpty() == false) {
            for (var entry : mapDiff.getUpserts()) {
                LifecyclePolicyMetadata policyMetadata = entry.getValue();
                LifecyclePolicySecurityClient policyClient = new LifecyclePolicySecurityClient(
                    client,
                    ClientHelper.INDEX_LIFECYCLE_ORIGIN,
                    policyMetadata.getHeaders()
                );
                lifecyclePolicyMap.put(policyMetadata.getName(), policyMetadata);
                List<Step> policyAsSteps = policyMetadata.getPolicy().toSteps(policyClient, licenseState);
                if (policyAsSteps.isEmpty() == false) {
                    firstStepMap.put(policyMetadata.getName(), policyAsSteps.get(0));
                    final Map<Step.StepKey, Step> stepMapForPolicy = new LinkedHashMap<>();
                    for (Step step : policyAsSteps) {
                        assert ErrorStep.NAME.equals(step.getKey().name()) == false : "unexpected error step in policy";
                        stepMapForPolicy.put(step.getKey(), step);
                    }
                    logger.trace(
                        "updating cached steps for [{}] policy, new steps: {}",
                        policyMetadata.getName(),
                        stepMapForPolicy.keySet()
                    );
                    stepMap.put(policyMetadata.getName(), stepMapForPolicy);
                }
            }
        }

        // Since the policies (may have) changed, the whole steps cache needs to be thrown out.
        // We do this after we update `lifecyclePolicyMap` to ensure `cachedSteps` does not contain outdated data.
        // This means we may clear up-to-date data, but that's a lot better than the cache containing outdated entries indefinitely.
        cachedSteps.clear();
    }

    /**
     * Remove the entry for an index from the index->step cache.
     *
     * We clear the map entirely when the master of the cluster changes, and when any
     * policy changes, but in a long-lived cluster that doesn't happen to experience
     * either of those events (and where indices are removed regularly) we still want
     * the cache to trim deleted indices.
     *
     * n.b. even with this, there's still a pretty small chance that a given index
     * could leak, if we're right in the middle of populating the cache for that
     * index (in getStep) when we process the delete here, then we'll end up with an
     * entry that doesn't get deleted until the master changes or a policy changes
     * -- it's harmless enough
     */
    public void delete(Index deleted) {
        cachedSteps.remove(deleted);
    }

    /**
     * Clear internal maps that were populated by update (and others).
     */
    public void clear() {
        // this is potentially large, so it's important to clear it
        cachedSteps.clear();
        // these are relatively small, but there's no harm in clearing them
        lifecyclePolicyMap.clear();
        firstStepMap.clear();
        stepMap.clear();
    }

    /**
     * Return all ordered steps for the current policy for the index. Does not
     * resolve steps using the phase caching, but only for the currently existing policy.
     */
    private List<Step> getAllStepsForIndex(ClusterState state, Index index) {
        final Metadata metadata = state.metadata();
        if (metadata.getProject().hasIndex(index) == false) {
            throw new IllegalArgumentException("index " + index + " does not exist in the current cluster state");
        }
        final IndexMetadata indexMetadata = metadata.getProject().index(index);
        final String policyName = indexMetadata.getLifecyclePolicyName();
        final LifecyclePolicyMetadata policyMetadata = lifecyclePolicyMap.get(policyName);
        if (policyMetadata == null) {
            throw new IllegalArgumentException("the policy [" + policyName + "] for index" + index + " does not exist");
        }
        final LifecyclePolicySecurityClient policyClient = new LifecyclePolicySecurityClient(
            client,
            ClientHelper.INDEX_LIFECYCLE_ORIGIN,
            policyMetadata.getHeaders()
        );
        return policyMetadata.getPolicy().toSteps(policyClient, licenseState);
    }

    /**
     * Given an index and a phase name, return the {@link Step.StepKey} for the
     * first step in that phase, if it exists, or null otherwise.
     */
    @Nullable
    public Step.StepKey getFirstStepForPhase(ClusterState state, Index index, String phase) {
        return getAllStepsForIndex(state, index).stream()
            .map(Step::getKey)
            .filter(stepKey -> phase.equals(stepKey.phase()))
            .findFirst()
            .orElse(null);
    }

    /**
     * Given an index, phase name, and action name, return the {@link Step.StepKey}
     * for the first step in that phase, if it exists, or null otherwise.
     */
    @Nullable
    public Step.StepKey getFirstStepForPhaseAndAction(ClusterState state, Index index, String phase, String action) {
        return getAllStepsForIndex(state, index).stream()
            .map(Step::getKey)
            .filter(stepKey -> phase.equals(stepKey.phase()))
            .filter(stepKey -> action.equals(stepKey.action()))
            .findFirst()
            .orElse(null);
    }

    /*
     * Parses the step keys from the {@code phaseDef} for the given phase.
     * ILM makes use of some implicit steps that belong to actions that we automatically inject
     * (eg. unfollow and migrate) or special purpose steps like the phase `complete` step.
     *
     * The {@code phaseDef} is *mostly* a valid json we store in the lifecycle execution state. However,
     * we have a few of exceptional cases:
     * - null is treated as the `new` phase (see {@code InitializePolicyContextStep})
     * - the `new` phase is not stored as json but ... "new"
     * - there's a legacy step, the {@code TerminalPolicyStep} which is also not stored as json but as "completed"
     * (note: this step exists only for BWC reasons as these days we move to the {@code PhaseCompleteStep} when reaching
     * the end of the phase)
     *
     * This method returns **all** the steps that are part of the phase definition including the implicit steps.
     *
     * Returns null if there's a parsing error.
     */
    @Nullable
    public Set<Step.StepKey> parseStepKeysFromPhase(String policy, String currentPhase, String phaseDef) {
        try {
            String phaseDefNonNull = Objects.requireNonNullElse(phaseDef, InitializePolicyContextStep.INITIALIZATION_PHASE);
            return parseStepsFromPhase(policy, currentPhase, phaseDefNonNull).stream().map(Step::getKey).collect(Collectors.toSet());
        } catch (IOException e) {
            logger.trace(
                () -> Strings.format(
                    "unable to parse steps for policy [%s], phase [%s], and phase definition [%s]",
                    policy,
                    currentPhase,
                    phaseDef
                ),
                e
            );
            return null;
        }
    }

    /**
     * The {@code phaseDef} is *mostly* a valid json we store in the lifecycle execution state. However,
     * we have a few of exceptional cases:
     * - null is treated as the `new` phase (see {@code InitializePolicyContextStep})
     * - the `new` phase is not stored as json but ... "new"
     * - there's a legacy step, the {@code TerminalPolicyStep} which is also not stored as json but as "completed"
     * (note: this step exists only for BWC reasons as these days we move to the {@code PhaseCompleteStep} when reaching
     * the end of the phase)
     */
    private List<Step> parseStepsFromPhase(String policy, String currentPhase, String phaseDef) throws IOException {
        final PhaseExecutionInfo phaseExecutionInfo;
        LifecyclePolicyMetadata policyMetadata = lifecyclePolicyMap.get(policy);
        if (policyMetadata == null) {
            throw new IllegalStateException("unable to parse steps for policy [" + policy + "] as it doesn't exist");
        }
        LifecyclePolicy currentPolicy = policyMetadata.getPolicy();
        final LifecyclePolicy policyToExecute;
        if (InitializePolicyContextStep.INITIALIZATION_PHASE.equals(phaseDef) || TerminalPolicyStep.COMPLETED_PHASE.equals(phaseDef)) {
            // It is ok to re-use potentially modified policy here since we are in an initialization or completed phase
            policyToExecute = currentPolicy;
        } else {
            // if the current phase definition describes an internal step/phase, do not parse
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry),
                    phaseDef
                )
            ) {
                phaseExecutionInfo = PhaseExecutionInfo.parse(parser, currentPhase);
            }
            Map<String, Phase> phaseMap = new HashMap<>(currentPolicy.getPhases());
            if (phaseExecutionInfo.getPhase() != null) {
                phaseMap.put(currentPhase, phaseExecutionInfo.getPhase());
            }
            policyToExecute = new LifecyclePolicy(currentPolicy.getType(), currentPolicy.getName(), phaseMap, currentPolicy.getMetadata());
        }
        LifecyclePolicySecurityClient policyClient = new LifecyclePolicySecurityClient(
            client,
            ClientHelper.INDEX_LIFECYCLE_ORIGIN,
            lifecyclePolicyMap.get(policy).getHeaders()
        );
        final List<Step> steps = policyToExecute.toSteps(policyClient, licenseState);
        // Build a list of steps that correspond with the phase the index is currently in
        final List<Step> phaseSteps;
        if (steps == null) {
            phaseSteps = List.of();
        } else {
            phaseSteps = steps.stream().filter(e -> e.getKey().phase().equals(currentPhase)).toList();
        }
        logger.trace(
            "parsed steps for policy [{}] in phase [{}], definition: [{}], steps: [{}]",
            policy,
            currentPhase,
            phaseDef,
            phaseSteps
        );
        return phaseSteps;
    }

    /**
     * Read-only internal helper for getStep that returns a non-null step if one is cached for the provided
     * IndexMetadata and StepKey, and null otherwise.
     */
    @Nullable
    private Step getCachedStep(final IndexMetadata indexMetadata, final Step.StepKey stepKey) {
        final Tuple<IndexMetadata, Step> cachedStep = cachedSteps.get(indexMetadata.getIndex());
        // n.b. we're using instance equality here for the IndexMetadata rather than object equality because it's fast,
        // this means that we're erring on the side of cache misses (if the IndexMetadata changed in any way, it'll be
        // a new instance, so we'll miss-and-repopulate the cache for the index in question)
        if (cachedStep != null && cachedStep.v1() == indexMetadata) {
            assert cachedStep.v2() != null : "null steps should never be cached in the policy step registry";
            if (cachedStep.v2() != null && cachedStep.v2().getKey().equals(stepKey)) {
                return cachedStep.v2();
            }
        }
        return null;
    }

    @Nullable
    public Step getStep(final IndexMetadata indexMetadata, final Step.StepKey stepKey) {
        final Step cachedStep = getCachedStep(indexMetadata, stepKey);
        if (cachedStep != null) {
            return cachedStep;
        }

        if (ErrorStep.NAME.equals(stepKey.name())) {
            return new ErrorStep(new Step.StepKey(stepKey.phase(), stepKey.action(), ErrorStep.NAME));
        }

        final String phase = stepKey.phase();
        final String policyName = indexMetadata.getLifecyclePolicyName();
        final Index index = indexMetadata.getIndex();

        if (policyName == null) {
            throw new IllegalArgumentException("failed to retrieve step " + stepKey + " as index [" + index.getName() + "] has no policy");
        }

        // parse phase steps from the phase definition in the index settings
        final String phaseJson = Objects.requireNonNullElse(
            indexMetadata.getLifecycleExecutionState().phaseDefinition(),
            InitializePolicyContextStep.INITIALIZATION_PHASE
        );

        final List<Step> phaseSteps;
        try {
            phaseSteps = parseStepsFromPhase(policyName, phase, phaseJson);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to load cached steps for " + stepKey, e);
        } catch (XContentParseException parseErr) {
            throw new XContentParseException(
                parseErr.getLocation(),
                "failed to load steps for " + stepKey + " from [" + phaseJson + "]",
                parseErr
            );
        }

        assert phaseSteps.stream().allMatch(step -> step.getKey().phase().equals(phase))
            : "expected phase steps loaded from phase definition for ["
                + index.getName()
                + "] to be in phase ["
                + phase
                + "] but they were not, steps: "
                + phaseSteps;

        // Return the step that matches the given stepKey or else null if we couldn't find it
        final Step s = phaseSteps.stream().filter(step -> step.getKey().equals(stepKey)).findFirst().orElse(null);
        if (s != null) {
            cachedSteps.put(indexMetadata.getIndex(), Tuple.tuple(indexMetadata, s));
        }
        return s;
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
