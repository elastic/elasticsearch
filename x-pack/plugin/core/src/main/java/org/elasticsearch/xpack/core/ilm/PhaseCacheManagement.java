/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.core.Strings.format;

/**
 * We cache the currently executing ILM phase in the index metadata so the ILM execution for managed indices is not irrecoverably
 * interrupted by a concurrent update policy that, say, would remove the current execution phase altogether.
 * <p>
 * This contains class contains a series of methods that help manage the cached ILM phase.
 */
public final class PhaseCacheManagement {

    private static final Logger logger = LogManager.getLogger(PhaseCacheManagement.class);

    private PhaseCacheManagement() {}

    /**
     * Rereads the phase JSON for the given index, returning a new cluster state.
     */
    public static ProjectMetadata refreshPhaseDefinition(
        final ProjectMetadata projectMetadata,
        final String index,
        final LifecyclePolicyMetadata updatedPolicy
    ) {
        IndexMetadata idxMeta = projectMetadata.index(index);
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectMetadata);
        refreshPhaseDefinition(projectMetadataBuilder, idxMeta, updatedPolicy);
        return projectMetadataBuilder.build();
    }

    /**
     * Rereads the phase JSON for the given index, and updates the provided metadata.
     */
    public static void refreshPhaseDefinition(
        final ProjectMetadata.Builder projectMetadataBuilder,
        final IndexMetadata idxMeta,
        final LifecyclePolicyMetadata updatedPolicy
    ) {
        String index = idxMeta.getIndex().getName();
        assert eligibleToCheckForRefresh(idxMeta) : "index " + index + " is missing crucial information needed to refresh phase definition";

        logger.trace("[{}] updating cached phase definition for policy [{}]", index, updatedPolicy.getName());
        LifecycleExecutionState currentExState = idxMeta.getLifecycleExecutionState();

        String currentPhase = currentExState.phase();
        PhaseExecutionInfo pei = new PhaseExecutionInfo(
            updatedPolicy.getName(),
            updatedPolicy.getPolicy().getPhases().get(currentPhase),
            updatedPolicy.getVersion(),
            updatedPolicy.getModifiedDate()
        );

        LifecycleExecutionState newExState = LifecycleExecutionState.builder(currentExState)
            .setPhaseDefinition(Strings.toString(pei, false, false))
            .build();

        projectMetadataBuilder.put(IndexMetadata.builder(idxMeta).putCustom(ILM_CUSTOM_METADATA_KEY, newExState.asMap()));
    }

    /**
     * Ensure that we have the minimum amount of metadata necessary to check for cache phase
     * refresh. This includes:
     * - An execution state
     * - Existing phase definition JSON
     * - A current step key
     * - A current phase in the step key
     * - Not currently in the ERROR step
     */
    public static boolean eligibleToCheckForRefresh(final IndexMetadata metadata) {
        LifecycleExecutionState executionState = metadata.getLifecycleExecutionState();
        if (executionState == null || executionState.phaseDefinition() == null) {
            return false;
        }

        Step.StepKey currentStepKey = Step.getCurrentStepKey(executionState);
        if (currentStepKey == null || currentStepKey.phase() == null) {
            return false;
        }

        return ErrorStep.NAME.equals(currentStepKey.name()) == false;
    }

    /**
     * For the given new policy, returns a new cluster with all updateable indices' phase JSON refreshed.
     */
    public static ProjectMetadata updateIndicesForPolicy(
        final ProjectMetadata projectMetadata,
        final NamedXContentRegistry xContentRegistry,
        final Client client,
        final LifecyclePolicy oldPolicy,
        final LifecyclePolicyMetadata newPolicy,
        XPackLicenseState licenseState
    ) {
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(projectMetadata);
        if (updateIndicesForPolicy(projectMetadataBuilder, projectMetadata, xContentRegistry, client, oldPolicy, newPolicy, licenseState)) {
            return projectMetadataBuilder.build();
        }
        return projectMetadata;
    }

    /**
     * For the given new policy, update the provided metadata to reflect the refreshed phase JSON for all updateable indices.
     * Returns true if any indices were updated and false otherwise.
     * Users of this API should consider the returned value and only create a new {@link ClusterState} if `true` is returned.
     */
    public static boolean updateIndicesForPolicy(
        final ProjectMetadata.Builder projectMetadataBuilder,
        final ProjectMetadata projectMetadata,
        final NamedXContentRegistry xContentRegistry,
        final Client client,
        final LifecyclePolicy oldPolicy,
        final LifecyclePolicyMetadata newPolicy,
        final XPackLicenseState licenseState
    ) {
        assert oldPolicy.getName().equals(newPolicy.getName())
            : "expected both policies to have the same id but they were: [" + oldPolicy.getName() + "] vs. [" + newPolicy.getName() + "]";

        // No need to update anything if the policies are identical in contents
        if (oldPolicy.equals(newPolicy.getPolicy())) {
            logger.debug("policy [{}] is unchanged and no phase definition refresh is needed", oldPolicy.getName());
            return false;
        }

        final List<IndexMetadata> indicesThatCanBeUpdated = projectMetadata.indices()
            .values()
            .stream()
            .filter(meta -> newPolicy.getName().equals(meta.getLifecyclePolicyName()))
            .filter(meta -> isIndexPhaseDefinitionUpdatable(xContentRegistry, client, meta, newPolicy.getPolicy(), licenseState))
            .toList();

        final List<String> refreshedIndices = new ArrayList<>(indicesThatCanBeUpdated.size());
        for (IndexMetadata index : indicesThatCanBeUpdated) {
            try {
                refreshPhaseDefinition(projectMetadataBuilder, index, newPolicy);
                refreshedIndices.add(index.getIndex().getName());
            } catch (Exception e) {
                logger.warn(() -> format("[%s] unable to refresh phase definition for updated policy [%s]", index, newPolicy.getName()), e);
            }
        }
        logger.debug("refreshed policy [{}] phase definition for [{}] indices", newPolicy.getName(), refreshedIndices.size());
        return refreshedIndices.size() > 0;
    }

    /**
     * Returns 'true' if the index's cached phase JSON can be safely reread, 'false' otherwise.
     */
    public static boolean isIndexPhaseDefinitionUpdatable(
        final NamedXContentRegistry xContentRegistry,
        final Client client,
        final IndexMetadata metadata,
        final LifecyclePolicy newPolicy,
        final XPackLicenseState licenseState
    ) {
        final String index = metadata.getIndex().getName();
        if (eligibleToCheckForRefresh(metadata) == false) {
            logger.debug("[{}] does not contain enough information to check for eligibility of refreshing phase", index);
            return false;
        }
        final String policyId = newPolicy.getName();

        final LifecycleExecutionState executionState = metadata.getLifecycleExecutionState();
        final Step.StepKey currentStepKey = Step.getCurrentStepKey(executionState);
        final String currentPhase = currentStepKey.phase();

        final Set<Step.StepKey> newStepKeys = newPolicy.toSteps(client, licenseState)
            .stream()
            .map(Step::getKey)
            .collect(Collectors.toCollection(LinkedHashSet::new));

        if (newStepKeys.contains(currentStepKey) == false) {
            // The index is on a step that doesn't exist in the new policy, we
            // can't safely re-read the JSON
            logger.debug(
                "[{}] updated policy [{}] does not contain the current step key [{}], so the policy phase will not be refreshed",
                index,
                policyId,
                currentStepKey
            );
            return false;
        }

        final String phaseDef = executionState.phaseDefinition();
        final Set<Step.StepKey> oldStepKeys = readStepKeys(xContentRegistry, client, phaseDef, currentPhase, licenseState);
        if (oldStepKeys == null) {
            logger.debug(
                "[{}] unable to parse phase definition for cached policy [{}], policy phase will not be refreshed",
                index,
                policyId
            );
            return false;
        }

        final Set<Step.StepKey> oldPhaseStepKeys = oldStepKeys.stream()
            .filter(sk -> currentPhase.equals(sk.phase()))
            .collect(Collectors.toCollection(LinkedHashSet::new));

        final PhaseExecutionInfo phaseExecutionInfo = new PhaseExecutionInfo(policyId, newPolicy.getPhases().get(currentPhase), 1L, 1L);
        final String peiJson = Strings.toString(phaseExecutionInfo);

        final Set<Step.StepKey> newPhaseStepKeys = readStepKeys(xContentRegistry, client, peiJson, currentPhase, licenseState);
        if (newPhaseStepKeys == null) {
            logger.debug(
                () -> format("[%s] unable to parse phase definition for policy [%s] to determine if it could be refreshed", index, policyId)
            );
            return false;
        }

        if (newPhaseStepKeys.equals(oldPhaseStepKeys)) {
            // The new and old phase have the same stepkeys for this current phase, so we can
            // refresh the definition because we know it won't change the execution flow.
            logger.debug("[{}] updated policy [{}] contains the same phase step keys and can be refreshed", index, policyId);
            return true;
        } else {
            logger.debug(
                "[{}] updated policy [{}] has different phase step keys and will NOT refresh phase "
                    + "definition as it differs too greatly. old: {}, new: {}",
                index,
                policyId,
                oldPhaseStepKeys,
                newPhaseStepKeys
            );
            return false;
        }
    }

    /**
     * Parse the {@code phaseDef} phase definition to get the stepkeys for the given phase.
     * If there is an error parsing or if the phase definition is missing the required
     * information, returns null.
     */
    @Nullable
    static Set<Step.StepKey> readStepKeys(
        final NamedXContentRegistry xContentRegistry,
        final Client client,
        final String phaseDef,
        final String currentPhase,
        final XPackLicenseState licenseState
    ) {
        if (phaseDef == null) {
            return null;
        }

        final PhaseExecutionInfo phaseExecutionInfo;
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry),
                phaseDef
            )
        ) {
            phaseExecutionInfo = PhaseExecutionInfo.parse(parser, currentPhase);
        } catch (Exception e) {
            logger.trace(() -> format("exception reading step keys checking for refreshability, phase definition: %s", phaseDef), e);
            return null;
        }

        if (phaseExecutionInfo == null || phaseExecutionInfo.getPhase() == null) {
            return null;
        }

        return phaseExecutionInfo.getPhase()
            .getActions()
            .values()
            .stream()
            .flatMap(a -> a.toSteps(client, phaseExecutionInfo.getPhase().getName(), null, licenseState).stream())
            .map(Step::getKey)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
