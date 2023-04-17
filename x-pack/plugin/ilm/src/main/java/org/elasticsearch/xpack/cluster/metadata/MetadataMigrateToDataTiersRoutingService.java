/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.Step;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.cluster.routing.allocation.DataTier.ENFORCE_DEFAULT_TIER_PREFERENCE;
import static org.elasticsearch.cluster.routing.allocation.DataTier.TIER_PREFERENCE;
import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPED;
import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagement.updateIndicesForPolicy;
import static org.elasticsearch.xpack.ilm.IndexLifecycleTransition.moveStateToNextActionAndUpdateCachedPhase;

/**
 * Exposes the necessary methods to migrate a system's elasticsearch abstractions to use data tiers for index allocation routing.
 */
public final class MetadataMigrateToDataTiersRoutingService {

    public static final String DEFAULT_NODE_ATTRIBUTE_NAME = "data";
    private static final Logger logger = LogManager.getLogger(MetadataMigrateToDataTiersRoutingService.class);

    private MetadataMigrateToDataTiersRoutingService() {}

    /**
     * Migrates the elasticsearch abstractions to use data tiers for allocation routing.
     * This will:
     * - remove the given V1 index template if it exists.
     *
     * - loop through the existing ILM policies and look at the configured {@link AllocateAction}s. If they define *any* routing rules
     * based on the provided node attribute name (we look at include, exclude, and require rules) *ALL* the rules in the allocate action
     * will be removed. All the rules are removed in order to allow for ILM to inject the {@link MigrateAction}.
     * So for eg. this action:
     *      allocate {
     *          number_of_replicas: 0,
     *          require: {data: warm},
     *          include: {rack: one}
     *      }
     *  will become
     *      allocate {
     *          number_of_replicas: 0
     *      }
     *  Note that if the `allocate` action doesn't define any `number_of_replicas` it will be removed completely from the migrated policy.
     *  As part of migrating the ILM policies we also update the cached phase definition for the managed indices to reflect the migrated
     *  policy phase.
     *
     *  - loop through all the indices convert the index.routing.allocation.require.{nodeAttrName} or
     *  index.routing.allocation.include.{nodeAttrName} setting (if present) to the corresponding data tier `_tier_preference` routing.
     *  We are only able to convert the `frozen`, `cold`, `warm`, or `hot` setting values to the `_tier_preference`. If other
     *  configuration values are present eg ("the_warm_nodes") the index will not be migrated.
     *  If the require or include setting is successfully migrated to _tier_preference, all the **other** routing settings for the
     *  provided attribute are also removed (if present).
     *  Eg. if we manage to migrate the `index.routing.allocation.require.data` setting, but the index also has configured
     *  `index.routing.allocation.include.data` and `index.routing.allocation.exclude.data`, the
     *  migrated settings will contain `index.routing.allocation.include._tier_preference` configured to the corresponding
     *  `index.routing.allocation.require.data` value, with `index.routing.allocation.include.data` and
     *  `index.routing.allocation.exclude.data` being removed.
     *  Settings:
     *    {
     *      index.routing.allocation.require.data: "warm",
     *      index.routing.allocation.include.data: "rack1",
     *      index.routing.allocation.exclude.data: "rack2,rack3"
     *    }
     *  will be migrated to:
     *    {
     *        index.routing.allocation.include._tier_preference: "data_warm,data_hot"
     *    }
     *
     * If both the `index.routing.allocation.require.data` and `index.routing.allocation.include.data` settings are configured to
     * recognized values the coldest one will be converted to the corresponding `_tier_preference` configuration.
     * Eg. the following configuration:
     *    {
     *      index.routing.allocation.require.data: "warm",
     *      index.routing.allocation.include.data: "cold",
     *      index.routing.allocation.exclude.data: "rack2,rack3"
     *    }
     *  will be migrated to:
     *    {
     *        index.routing.allocation.include._tier_preference: "data_cold,data_warm,data_hot"
     *    }
     *
     *  - loop through the existing legacy, composable, and component templates and remove all the custom attribute routing settings for
     *  the configured @param nodeAttrName, if any of the index.routing.allocation.require.{nodeAttrName} or index.routing.allocation
     *  .include.{nodeAttrName} settings are presents in the template (irrespective of what they are configured to, we do not inspect the
     *  values in this case).
     *  Eg. this legacy template:
     *  {
     *    "order": 0,
     *    "index_patterns": [
     *      "*"
     *    ],
     *    "settings": {
     *      "index": {
     *        "routing": {
     *          "allocation": {
     *            "require": {
     *              "data": "hot"
     *            },
     *            "include": {
     *               "data": "rack1"
     *            },
     *            "exclude": {
     *               "data": "bad_rack"
     *            }
     *          }
     *        }
     *      }
     *    },
     *    "mappings": {},
     *    "aliases": {}
     *  }
     *  will be migrated to
     *  {
     *    "order": 0,
     *    "index_patterns": [
     *      "*"
     *    ],
     *    "settings": {},
     *    "mappings": {},
     *    "aliases": {}
     *  }
     *
     * Same pattern applies to composable and component templates.
     *
     * If no @param nodeAttrName is provided "data" will be used.
     * If no @param indexTemplateToDelete is provided, no index templates will be deleted.
     *
     * This returns a new {@link ClusterState} representing the migrated state that is ready to use data tiers for index and
     * ILM routing allocations. It also returns a summary of the affected abstractions encapsulated in {@link MigratedEntities}
     */
    public static Tuple<ClusterState, MigratedEntities> migrateToDataTiersRouting(
        ClusterState currentState,
        @Nullable String nodeAttrName,
        @Nullable String indexTemplateToDelete,
        NamedXContentRegistry xContentRegistry,
        Client client,
        XPackLicenseState licenseState,
        boolean dryRun
    ) {
        if (dryRun == false) {
            IndexLifecycleMetadata currentMetadata = currentState.metadata().custom(IndexLifecycleMetadata.TYPE);
            if (currentMetadata != null && currentILMMode(currentState) != STOPPED) {
                throw new IllegalStateException(
                    "stop ILM before migrating to data tiers, current state is [" + currentILMMode(currentState) + "]"
                );
            }
        }

        Metadata.Builder mb = Metadata.builder(currentState.metadata());

        // remove ENFORCE_DEFAULT_TIER_PREFERENCE from the persistent settings
        Settings.Builder persistentSettingsBuilder = Settings.builder().put(mb.persistentSettings());
        persistentSettingsBuilder.remove(ENFORCE_DEFAULT_TIER_PREFERENCE);
        mb.persistentSettings(persistentSettingsBuilder.build());

        // and remove it from the transient settings, just in case it was there
        Settings.Builder transientSettingsBuilder = Settings.builder().put(mb.transientSettings());
        transientSettingsBuilder.remove(ENFORCE_DEFAULT_TIER_PREFERENCE);
        mb.transientSettings(transientSettingsBuilder.build());

        String removedIndexTemplateName = null;
        if (Strings.hasText(indexTemplateToDelete)) {
            if (currentState.metadata().getTemplates().containsKey(indexTemplateToDelete)) {
                mb.removeTemplate(indexTemplateToDelete);
                logger.debug("removing legacy template [{}]", indexTemplateToDelete);
                removedIndexTemplateName = indexTemplateToDelete;
            } else {
                logger.debug("legacy template [{}] does not exist", indexTemplateToDelete);
            }
        }

        String attribute = nodeAttrName;
        if (Strings.isNullOrEmpty(nodeAttrName)) {
            attribute = DEFAULT_NODE_ATTRIBUTE_NAME;
        }
        List<String> migratedPolicies = migrateIlmPolicies(mb, currentState, attribute, xContentRegistry, client, licenseState);
        // Creating an intermediary cluster state view as when migrating policy we also update the cached phase definition stored in the
        // index metadata so the metadata.builder will probably contain an already updated view over the indices metadata which we don't
        // want to lose when migrating the indices settings
        ClusterState intermediateState = ClusterState.builder(currentState).metadata(mb).build();
        mb = Metadata.builder(intermediateState.metadata());
        List<String> migratedIndices = migrateIndices(mb, intermediateState, attribute);
        MigratedTemplates migratedTemplates = migrateIndexAndComponentTemplates(mb, intermediateState, attribute);
        return Tuple.tuple(
            ClusterState.builder(currentState).metadata(mb).build(),
            new MigratedEntities(removedIndexTemplateName, migratedIndices, migratedPolicies, migratedTemplates)
        );
    }

    /**
     * Iterate through the existing ILM policies and look at the configured {@link AllocateAction}s. If they define *any* routing rules
     * based on the provided node attribute name (we look at include, exclude, and require rules) *ALL* the rules in the allocate
     * action will be removed. All the rules are removed in order to allow for ILM to inject the {@link MigrateAction}.
     * This also iterates through all the indices that are executing a given *migrated* policy and refreshes the cached phase definition
     * for each of these managed indices.
     */
    static List<String> migrateIlmPolicies(
        Metadata.Builder mb,
        ClusterState currentState,
        String nodeAttrName,
        NamedXContentRegistry xContentRegistry,
        Client client,
        XPackLicenseState licenseState
    ) {
        IndexLifecycleMetadata currentLifecycleMetadata = currentState.metadata().custom(IndexLifecycleMetadata.TYPE);
        if (currentLifecycleMetadata == null) {
            return Collections.emptyList();
        }

        List<String> migratedPolicies = new ArrayList<>();
        Map<String, LifecyclePolicyMetadata> currentPolicies = currentLifecycleMetadata.getPolicyMetadatas();
        SortedMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(currentPolicies);
        for (Map.Entry<String, LifecyclePolicyMetadata> policyMetadataEntry : currentPolicies.entrySet()) {
            LifecyclePolicy newLifecyclePolicy = migrateSingleILMPolicy(nodeAttrName, policyMetadataEntry.getValue().getPolicy());
            if (newLifecyclePolicy != null) {
                // we updated at least one phase
                long nextVersion = policyMetadataEntry.getValue().getVersion() + 1L;
                LifecyclePolicyMetadata newPolicyMetadata = new LifecyclePolicyMetadata(
                    newLifecyclePolicy,
                    policyMetadataEntry.getValue().getHeaders(),
                    nextVersion,
                    Instant.now().toEpochMilli()
                );
                LifecyclePolicyMetadata oldPolicyMetadata = newPolicies.put(policyMetadataEntry.getKey(), newPolicyMetadata);
                assert oldPolicyMetadata != null
                    : "we must only update policies, not create new ones, but " + policyMetadataEntry.getKey() + " didn't exist";

                refreshCachedPhases(mb, currentState, oldPolicyMetadata, newPolicyMetadata, xContentRegistry, client, licenseState);
                migratedPolicies.add(policyMetadataEntry.getKey());
            }
        }

        if (migratedPolicies.size() > 0) {
            IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentILMMode(currentState));
            mb.putCustom(IndexLifecycleMetadata.TYPE, newMetadata);
        }
        return migratedPolicies;
    }

    /**
     * Refreshed the cached ILM phase definition for the indices managed by the migrated policy.
     */
    static void refreshCachedPhases(
        Metadata.Builder mb,
        ClusterState currentState,
        LifecyclePolicyMetadata oldPolicyMetadata,
        LifecyclePolicyMetadata newPolicyMetadata,
        NamedXContentRegistry xContentRegistry,
        Client client,
        XPackLicenseState licenseState
    ) {
        // this performs a walk through the managed indices and safely updates the cached phase (ie. for the phases we did not
        // remove the allocate action)
        updateIndicesForPolicy(mb, currentState, xContentRegistry, client, oldPolicyMetadata.getPolicy(), newPolicyMetadata, licenseState);

        LifecyclePolicy newLifecyclePolicy = newPolicyMetadata.getPolicy();
        List<String> migratedPhasesWithoutAllocateAction = getMigratedPhasesWithoutAllocateAction(
            oldPolicyMetadata.getPolicy(),
            newLifecyclePolicy
        );

        if (migratedPhasesWithoutAllocateAction.size() > 0) {
            logger.debug(
                "the updated policy [{}] does not contain the allocate action in phases [{}] anymore",
                newLifecyclePolicy.getName(),
                migratedPhasesWithoutAllocateAction
            );
            // if we removed the allocate action in any phase we won't be able to perform a safe update of the ilm cached phase (as
            // defined by {@link PhaseCacheManagement#isIndexPhaseDefinitionUpdatable} because the number of steps in the new phase is
            // not the same as in the cached phase) so let's forcefully (and still safely :) ) refresh the cached phase for the managed
            // indices in these phases.
            refreshCachedPhaseForPhasesWithoutAllocateAction(
                mb,
                currentState,
                oldPolicyMetadata.getPolicy(),
                newPolicyMetadata,
                migratedPhasesWithoutAllocateAction,
                client,
                licenseState
            );
        }
    }

    /**
     * Refresh the cached phase definition for those indices currently in one of the phases we migrated by removing the allocate action.
     * This refresh can be executed in two ways, depending where exactly within such a migrated phase is currently the managed index.
     * 1) if the index is in the allocate action, we'll move the ILM execution state for this index into the first step of the next
     * action of the phase (note that even if the allocate action was the only action defined in a phase we have a complete action we
     * inject at the end of every phase)
     * 2) if the index is anywhere else in the phase, we simply update the cached phase definition to reflect the migrated phase
     */
    private static void refreshCachedPhaseForPhasesWithoutAllocateAction(
        Metadata.Builder mb,
        ClusterState currentState,
        LifecyclePolicy oldPolicy,
        LifecyclePolicyMetadata newPolicyMetadata,
        List<String> phasesWithoutAllocateAction,
        Client client,
        XPackLicenseState licenseState
    ) {
        String policyName = oldPolicy.getName();
        final List<IndexMetadata> managedIndices = currentState.metadata()
            .indices()
            .values()
            .stream()
            .filter(meta -> policyName.equals(meta.getLifecyclePolicyName()))
            .toList();

        for (IndexMetadata indexMetadata : managedIndices) {
            LifecycleExecutionState currentExState = indexMetadata.getLifecycleExecutionState();

            if (currentExState != null) {
                Step.StepKey currentStepKey = Step.getCurrentStepKey(currentExState);
                if (currentStepKey != null && phasesWithoutAllocateAction.contains(currentStepKey.phase())) {
                    // the index is in a phase that doesn't contain the allocate action anymore
                    if (currentStepKey.action().equals(AllocateAction.NAME)) {
                        // this index is in the middle of executing the allocate action - which doesn't exist in the updated policy
                        // anymore so let's try to move the index to the next action

                        LifecycleExecutionState newLifecycleState = moveStateToNextActionAndUpdateCachedPhase(
                            indexMetadata,
                            currentExState,
                            System::currentTimeMillis,
                            oldPolicy,
                            newPolicyMetadata,
                            client,
                            licenseState
                        );
                        if (currentExState.equals(newLifecycleState) == false) {
                            mb.put(IndexMetadata.builder(indexMetadata).putCustom(ILM_CUSTOM_METADATA_KEY, newLifecycleState.asMap()));
                        }
                    } else {
                        // if the index is not in the allocate action, we're going to perform a cached phase update (which is "unsafe" by
                        // the rules defined in {@link PhaseCacheManagement#isIndexPhaseDefinitionUpdatable} but in our case it is safe
                        // as the migration would've only removed the allocate action and the current index is not in the middle of
                        // executing the allocate action, we made sure of that)

                        LifecycleExecutionState.Builder updatedState = LifecycleExecutionState.builder(currentExState);
                        PhaseExecutionInfo phaseExecutionInfo = new PhaseExecutionInfo(
                            newPolicyMetadata.getPolicy().getName(),
                            newPolicyMetadata.getPolicy().getPhases().get(currentStepKey.phase()),
                            newPolicyMetadata.getVersion(),
                            newPolicyMetadata.getModifiedDate()
                        );
                        String newPhaseDefinition = Strings.toString(phaseExecutionInfo, false, false);
                        updatedState.setPhaseDefinition(newPhaseDefinition);

                        logger.debug(
                            "updating the cached phase definition for index [{}], current step [{}] in policy " + "[{}] to [{}]",
                            indexMetadata.getIndex().getName(),
                            currentStepKey,
                            policyName,
                            newPhaseDefinition
                        );
                        mb.put(IndexMetadata.builder(indexMetadata).putCustom(ILM_CUSTOM_METADATA_KEY, updatedState.build().asMap()));
                    }
                }
            }
        }
    }

    /**
     * Returns a list of phases that had an allocate action defined in the old policy, but don't have it anymore in the new policy
     * (ie. they were allocate actions that only specified attribute based routing, without any number of replicas configuration and we
     * removed them as part of the migration of ILM policies to data tiers in order to allow ILM to inject the migrate action)
     */
    private static List<String> getMigratedPhasesWithoutAllocateAction(LifecyclePolicy oldPolicy, LifecyclePolicy newLifecyclePolicy) {
        List<String> oldPhasesWithAllocateAction = new ArrayList<>(oldPolicy.getPhases().size());
        for (Map.Entry<String, Phase> phaseEntry : oldPolicy.getPhases().entrySet()) {
            if (phaseEntry.getValue().getActions().containsKey(AllocateAction.NAME)) {
                oldPhasesWithAllocateAction.add(phaseEntry.getKey());
            }
        }

        List<String> migratedPhasesWithoutAllocateAction = new ArrayList<>(oldPhasesWithAllocateAction.size());
        for (String phaseWithAllocateAction : oldPhasesWithAllocateAction) {
            Phase phase = newLifecyclePolicy.getPhases().get(phaseWithAllocateAction);
            assert phase != null : "the migration service should not remove an entire phase altogether";
            if (phase.getActions().containsKey(AllocateAction.NAME) == false) {
                // the updated policy doesn't have the allocate action defined in this phase anymore
                migratedPhasesWithoutAllocateAction.add(phaseWithAllocateAction);
            }
        }
        return migratedPhasesWithoutAllocateAction;
    }

    /**
     * Migrates a single ILM policy from defining {@link AllocateAction}s in order to configure shard allocation routing based on the
     * provided node attribute name towards allowing ILM to inject the {@link MigrateAction}.
     *
     * Returns the migrated ILM policy.
     */
    @Nullable
    private static LifecyclePolicy migrateSingleILMPolicy(String nodeAttrName, LifecyclePolicy lifecyclePolicy) {
        LifecyclePolicy newLifecyclePolicy = null;
        for (Map.Entry<String, Phase> phaseEntry : lifecyclePolicy.getPhases().entrySet()) {
            Phase phase = phaseEntry.getValue();
            AllocateAction allocateAction = (AllocateAction) phase.getActions().get(AllocateAction.NAME);
            if (allocateActionDefinesRoutingRules(nodeAttrName, allocateAction)) {
                Map<String, LifecycleAction> actionMap = new HashMap<>(phase.getActions());
                // this phase contains an allocate action that defines a require rule for the attribute name so we'll remove all the
                // rules to allow for the migrate action to be injected
                if (allocateAction.getNumberOfReplicas() != null || allocateAction.getTotalShardsPerNode() != null) {
                    // keep the number of replicas configuration and/or the total shards per node configuration
                    AllocateAction updatedAllocateAction = new AllocateAction(
                        allocateAction.getNumberOfReplicas(),
                        allocateAction.getTotalShardsPerNode(),
                        null,
                        null,
                        null
                    );
                    actionMap.put(allocateAction.getWriteableName(), updatedAllocateAction);
                    logger.debug(
                        "ILM policy [{}], phase [{}]: updated the allocate action to [{}]",
                        lifecyclePolicy.getName(),
                        phase.getName(),
                        allocateAction
                    );
                } else {
                    // remove the action altogether
                    actionMap.remove(allocateAction.getWriteableName());
                    logger.debug("ILM policy [{}], phase [{}]: removed the allocate action", lifecyclePolicy.getName(), phase.getName());
                }

                // we removed the allocate action allocation rules (or the action completely) so let's check if there is an
                // explicit migrate action that's disabled, and remove it so ILM can inject an enabled one
                if (actionMap.containsKey(MigrateAction.NAME)) {
                    MigrateAction migrateAction = (MigrateAction) actionMap.get(MigrateAction.NAME);
                    if (migrateAction.isEnabled() == false) {
                        actionMap.remove(MigrateAction.NAME);
                        logger.debug(
                            "ILM policy [{}], phase [{}]: removed the deactivated migrate action",
                            lifecyclePolicy.getName(),
                            phase.getName()
                        );
                    }
                }

                Phase updatedPhase = new Phase(phase.getName(), phase.getMinimumAge(), actionMap);
                Map<String, Phase> updatedPhases = new HashMap<>(
                    newLifecyclePolicy == null ? lifecyclePolicy.getPhases() : newLifecyclePolicy.getPhases()
                );
                updatedPhases.put(phaseEntry.getKey(), updatedPhase);
                newLifecyclePolicy = new LifecyclePolicy(lifecyclePolicy.getName(), updatedPhases);
            }
        }
        return newLifecyclePolicy;
    }

    /**
     * Returns true of the provided {@link AllocateAction} defines any index allocation rules.
     */
    static boolean allocateActionDefinesRoutingRules(String nodeAttrName, @Nullable AllocateAction allocateAction) {
        return allocateAction != null
            && (allocateAction.getRequire().get(nodeAttrName) != null
                || allocateAction.getInclude().get(nodeAttrName) != null
                || allocateAction.getExclude().get(nodeAttrName) != null);
    }

    /**
     * Iterates through the existing indices and migrates them away from using attribute based routing using the provided node
     * attribute name towards the tier preference routing.
     * Returns a list of the migrated indices.
     */
    static List<String> migrateIndices(Metadata.Builder mb, ClusterState currentState, String nodeAttrName) {
        List<String> migratedIndices = new ArrayList<>();
        String nodeAttrIndexRequireRoutingSetting = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + nodeAttrName;
        String nodeAttrIndexIncludeRoutingSetting = INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + nodeAttrName;
        String nodeAttrIndexExcludeRoutingSetting = INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + nodeAttrName;
        for (var indexMetadata : currentState.metadata().indices().values()) {
            String indexName = indexMetadata.getIndex().getName();
            Settings currentSettings = indexMetadata.getSettings();

            boolean removeNodeAttrIndexRoutingSettings = true;

            // migrate using the `require` setting
            Settings newSettings = maybeMigrateRoutingSettingToTierPreference(
                nodeAttrIndexRequireRoutingSetting,
                currentSettings,
                indexName
            );
            // we possibly migrated the `require` setting, but maybe that attribute was not the coldest configured.
            // let's try to migrate the `include` setting as well
            newSettings = maybeMigrateRoutingSettingToTierPreference(nodeAttrIndexIncludeRoutingSetting, newSettings, indexName);

            if (newSettings.equals(currentSettings)) {
                removeNodeAttrIndexRoutingSettings = false;
                // migrating based on the `include` setting was not successful,
                // so, last stop, we just inject a tier preference regardless of anything else
                newSettings = migrateToDefaultTierPreference(currentState, indexMetadata);
            }

            if (newSettings.equals(currentSettings) == false) {
                Settings.Builder finalSettings = Settings.builder().put(newSettings);

                if (removeNodeAttrIndexRoutingSettings) {
                    // we converted either the `require` or the `include` routing setting to tier preference
                    // so let's clear all the routing settings for the given attribute
                    finalSettings.remove(nodeAttrIndexExcludeRoutingSetting);
                    finalSettings.remove(nodeAttrIndexRequireRoutingSetting);
                    finalSettings.remove(nodeAttrIndexIncludeRoutingSetting);
                }

                mb.put(
                    IndexMetadata.builder(indexMetadata).settings(finalSettings).settingsVersion(indexMetadata.getSettingsVersion() + 1)
                );
                migratedIndices.add(indexMetadata.getIndex().getName());
            }
        }
        return migratedIndices;
    }

    /**
     * Attempts to migrate the value of the given attribute routing setting to the _tier_preference equivalent. The provided setting
     * needs to be configured and have one of the supported values (hot, warm, cold, or frozen) in order for the migration to be performed.
     * If the migration is successful the provided setting will be removed.
     *
     * If the migration is **not** executed the current index settings is returned, otherwise the updated settings are returned
     */
    private static Settings maybeMigrateRoutingSettingToTierPreference(
        String attributeBasedRoutingSettingName,
        Settings currentIndexSettings,
        String indexName
    ) {
        if (currentIndexSettings.keySet().contains(attributeBasedRoutingSettingName) == false) {
            return currentIndexSettings;
        }

        Settings.Builder newSettingsBuilder = Settings.builder().put(currentIndexSettings);

        // look at the value, get the correct tiers config and update the settings
        if (currentIndexSettings.keySet().contains(TIER_PREFERENCE)) {
            String tierPreferenceConfiguration = currentIndexSettings.get(TIER_PREFERENCE);
            List<String> tiersConfiguration = DataTier.parseTierList(tierPreferenceConfiguration);
            if (tiersConfiguration.isEmpty() == false) {
                String coldestConfiguredTier = tiersConfiguration.get(0);
                String attributeValue = currentIndexSettings.get(attributeBasedRoutingSettingName);
                String attributeTierEquivalent = "data_" + attributeValue;
                if (DataTier.validTierName(attributeTierEquivalent)) {
                    // if the attribute's tier equivalent would be colder than what is currently the coldest tier configured
                    // in the _tier_preference setting, the configured attribute routing is more accurate so we'll update the
                    // tier_preference to reflect this before removing the attribute routing setting.
                    if (DataTier.compare(attributeTierEquivalent, coldestConfiguredTier) < 0) {
                        String newTierPreferenceConfiguration = convertAttributeValueToTierPreference(attributeValue);
                        if (newTierPreferenceConfiguration != null) {
                            logger.debug(
                                "index [{}]: updated the [{}] setting to [{}] as the attribute based routing setting [{}] had "
                                    + "the value [{}]",
                                indexName,
                                TIER_PREFERENCE,
                                newTierPreferenceConfiguration,
                                attributeBasedRoutingSettingName,
                                attributeValue
                            );
                            newSettingsBuilder.put(TIER_PREFERENCE, newTierPreferenceConfiguration);
                        }
                    }
                }
            }
            newSettingsBuilder.remove(attributeBasedRoutingSettingName);
            logger.debug("index [{}]: removed setting [{}]", indexName, attributeBasedRoutingSettingName);
        } else {
            // parse the custom attribute routing into the corresponding tier preference and configure it
            String attributeValue = currentIndexSettings.get(attributeBasedRoutingSettingName);
            String convertedTierPreference = convertAttributeValueToTierPreference(attributeValue);
            if (convertedTierPreference != null) {
                newSettingsBuilder.put(TIER_PREFERENCE, convertedTierPreference);
                newSettingsBuilder.remove(attributeBasedRoutingSettingName);
                logger.debug("index [{}]: removed setting [{}]", indexName, attributeBasedRoutingSettingName);
                logger.debug("index [{}]: configured setting [{}] to [{}]", indexName, TIER_PREFERENCE, convertedTierPreference);
            } else {
                // log warning and do *not* remove setting, return the settings unchanged
                logger.warn(
                    "index [{}]: could not convert attribute based setting [{}] value of [{}] to a tier preference "
                        + "configuration. the only known values are: {}",
                    indexName,
                    attributeBasedRoutingSettingName,
                    attributeValue,
                    "hot,warm,cold, and frozen"
                );
                return currentIndexSettings;
            }
        }
        return newSettingsBuilder.build();
    }

    static MigratedTemplates migrateIndexAndComponentTemplates(Metadata.Builder mb, ClusterState clusterState, String nodeAttrName) {
        List<String> migratedLegacyTemplates = migrateLegacyTemplates(mb, clusterState, nodeAttrName);
        List<String> migratedComposableTemplates = migrateComposableTemplates(mb, clusterState, nodeAttrName);
        List<String> migratedComponentTemplates = migrateComponentTemplates(mb, clusterState, nodeAttrName);
        return new MigratedTemplates(migratedLegacyTemplates, migratedComposableTemplates, migratedComponentTemplates);
    }

    static List<String> migrateLegacyTemplates(Metadata.Builder mb, ClusterState clusterState, String nodeAttrName) {
        String requireRoutingSetting = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + nodeAttrName;
        String includeRoutingSetting = INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + nodeAttrName;
        String excludeRoutingSetting = INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + nodeAttrName;

        List<String> migratedLegacyTemplates = new ArrayList<>();

        for (var template : clusterState.metadata().templates().entrySet()) {
            IndexTemplateMetadata templateMetadata = template.getValue();
            if (templateMetadata.settings().keySet().contains(requireRoutingSetting)
                || templateMetadata.settings().keySet().contains(includeRoutingSetting)) {
                IndexTemplateMetadata.Builder templateMetadataBuilder = new IndexTemplateMetadata.Builder(templateMetadata);
                Settings.Builder settingsBuilder = Settings.builder().put(templateMetadata.settings());
                settingsBuilder.remove(requireRoutingSetting);
                settingsBuilder.remove(includeRoutingSetting);
                settingsBuilder.remove(excludeRoutingSetting);
                templateMetadataBuilder.settings(settingsBuilder);

                mb.put(templateMetadataBuilder);
                migratedLegacyTemplates.add(template.getKey());
            }
        }
        return migratedLegacyTemplates;
    }

    static List<String> migrateComposableTemplates(Metadata.Builder mb, ClusterState clusterState, String nodeAttrName) {
        String requireRoutingSetting = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + nodeAttrName;
        String includeRoutingSetting = INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + nodeAttrName;
        String excludeRoutingSetting = INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + nodeAttrName;

        List<String> migratedComposableTemplates = new ArrayList<>();

        for (Map.Entry<String, ComposableIndexTemplate> templateEntry : clusterState.metadata().templatesV2().entrySet()) {
            ComposableIndexTemplate composableTemplate = templateEntry.getValue();
            if (composableTemplate.template() != null && composableTemplate.template().settings() != null) {
                Settings settings = composableTemplate.template().settings();

                if (settings.keySet().contains(requireRoutingSetting) || settings.keySet().contains(includeRoutingSetting)) {
                    Template currentInnerTemplate = composableTemplate.template();
                    ComposableIndexTemplate.Builder migratedComposableTemplateBuilder = new ComposableIndexTemplate.Builder();
                    Settings.Builder settingsBuilder = Settings.builder().put(settings);
                    settingsBuilder.remove(requireRoutingSetting);
                    settingsBuilder.remove(includeRoutingSetting);
                    settingsBuilder.remove(excludeRoutingSetting);
                    Template migratedInnerTemplate = new Template(
                        settingsBuilder.build(),
                        currentInnerTemplate.mappings(),
                        currentInnerTemplate.aliases(),
                        currentInnerTemplate.lifecycle()
                    );

                    migratedComposableTemplateBuilder.indexPatterns(composableTemplate.indexPatterns());
                    migratedComposableTemplateBuilder.template(migratedInnerTemplate);
                    migratedComposableTemplateBuilder.componentTemplates(composableTemplate.composedOf());
                    migratedComposableTemplateBuilder.priority(composableTemplate.priority());
                    migratedComposableTemplateBuilder.version(composableTemplate.version());
                    migratedComposableTemplateBuilder.metadata(composableTemplate.metadata());
                    migratedComposableTemplateBuilder.dataStreamTemplate(composableTemplate.getDataStreamTemplate());
                    migratedComposableTemplateBuilder.allowAutoCreate(composableTemplate.getAllowAutoCreate());
                    migratedComposableTemplateBuilder.ignoreMissingComponentTemplates(
                        composableTemplate.getIgnoreMissingComponentTemplates()
                    );

                    mb.put(templateEntry.getKey(), migratedComposableTemplateBuilder.build());
                    migratedComposableTemplates.add(templateEntry.getKey());
                }
            }
        }

        return migratedComposableTemplates;
    }

    static List<String> migrateComponentTemplates(Metadata.Builder mb, ClusterState clusterState, String nodeAttrName) {
        String requireRoutingSetting = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + nodeAttrName;
        String includeRoutingSetting = INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + nodeAttrName;
        String excludeRoutingSetting = INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + nodeAttrName;

        List<String> migratedComponentTemplates = new ArrayList<>();

        for (Map.Entry<String, ComponentTemplate> componentEntry : clusterState.metadata().componentTemplates().entrySet()) {
            ComponentTemplate componentTemplate = componentEntry.getValue();
            if (componentTemplate.template() != null && componentTemplate.template().settings() != null) {
                Settings settings = componentTemplate.template().settings();

                if (settings.keySet().contains(requireRoutingSetting) || settings.keySet().contains(includeRoutingSetting)) {
                    Template currentInnerTemplate = componentTemplate.template();
                    Settings.Builder settingsBuilder = Settings.builder().put(settings);
                    settingsBuilder.remove(requireRoutingSetting);
                    settingsBuilder.remove(includeRoutingSetting);
                    settingsBuilder.remove(excludeRoutingSetting);
                    Template migratedInnerTemplate = new Template(
                        settingsBuilder.build(),
                        currentInnerTemplate.mappings(),
                        currentInnerTemplate.aliases(),
                        currentInnerTemplate.lifecycle()
                    );

                    ComponentTemplate migratedComponentTemplate = new ComponentTemplate(
                        migratedInnerTemplate,
                        componentTemplate.version(),
                        componentTemplate.metadata()
                    );

                    mb.put(componentEntry.getKey(), migratedComponentTemplate);
                    migratedComponentTemplates.add(componentEntry.getKey());
                }
            }
        }

        return migratedComponentTemplates;
    }

    private static Settings migrateToDefaultTierPreference(ClusterState currentState, IndexMetadata indexMetadata) {
        Settings currentIndexSettings = indexMetadata.getSettings();
        List<String> tierPreference = DataTier.parseTierList(currentIndexSettings.get(DataTier.TIER_PREFERENCE));
        if (tierPreference.isEmpty() == false) {
            return currentIndexSettings;
        }

        Settings.Builder newSettingsBuilder = Settings.builder().put(currentIndexSettings);
        String indexName = indexMetadata.getIndex().getName();

        boolean isDataStream = currentState.metadata().findDataStreams(indexName).isEmpty() == false;
        String convertedTierPreference = isDataStream ? DataTier.DATA_HOT : DataTier.DATA_CONTENT;
        newSettingsBuilder.put(TIER_PREFERENCE, convertedTierPreference);
        logger.debug("index [{}]: configured setting [{}] to [{}]", indexName, TIER_PREFERENCE, convertedTierPreference);
        return newSettingsBuilder.build();
    }

    /**
     * Converts the provided node attribute value to the corresponding `_tier_preference` configuration.
     * Known (and convertible) attribute values are:
     * * hot
     * * warm
     * * cold
     * * frozen
     * and the corresponding tier preference setting values are, respectively:
     * * data_hot
     * * data_warm,data_hot
     * * data_cold,data_warm,data_hot
     * * data_frozen,data_cold,data_warm,data_hot
     * <p>
     * This returns `null` if an unknown attribute value is received.
     */
    @Nullable
    static String convertAttributeValueToTierPreference(String nodeAttributeValue) {
        String targetTier = "data_" + nodeAttributeValue;
        // handle the `content` accidental node attribute value which would match a data tier but doesn't fall into the hot/warm/cold
        // (given we're _migrating_ to data tiers we won't catch this accidental tier which didn't exist as a concept before the
        // formalisation of data tiers)
        if (DataTier.validTierName(targetTier) == false || targetTier.equals(DataTier.DATA_CONTENT)) {
            return null;
        }
        return DataTier.getPreferredTiersConfiguration(targetTier);
    }

    /**
     * Represents the elasticsearch abstractions that were, in some way, migrated such that the system is managing indices lifecycles and
     * allocations using data tiers.
     */
    public static final class MigratedEntities {
        @Nullable
        public final String removedIndexTemplateName;
        public final List<String> migratedIndices;
        public final List<String> migratedPolicies;
        public final MigratedTemplates migratedTemplates;

        public MigratedEntities(
            @Nullable String removedIndexTemplateName,
            List<String> migratedIndices,
            List<String> migratedPolicies,
            MigratedTemplates migratedTemplates
        ) {
            this.removedIndexTemplateName = removedIndexTemplateName;
            this.migratedIndices = Collections.unmodifiableList(migratedIndices);
            this.migratedPolicies = Collections.unmodifiableList(migratedPolicies);
            this.migratedTemplates = migratedTemplates;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MigratedEntities that = (MigratedEntities) o;
            return Objects.equals(removedIndexTemplateName, that.removedIndexTemplateName)
                && Objects.equals(migratedIndices, that.migratedIndices)
                && Objects.equals(migratedPolicies, that.migratedPolicies)
                && Objects.equals(migratedTemplates, that.migratedTemplates);
        }

        @Override
        public int hashCode() {
            return Objects.hash(removedIndexTemplateName, migratedIndices, migratedPolicies, migratedTemplates);
        }
    }

    /**
     * Represents the legacy, composable, and component templates that were migrated away from shard allocation settings based on custom
     * node attributes.
     */
    public static final class MigratedTemplates {
        public final List<String> migratedLegacyTemplates;
        public final List<String> migratedComposableTemplates;
        public final List<String> migratedComponentTemplates;

        public MigratedTemplates(
            List<String> migratedLegacyTemplates,
            List<String> migratedComposableTemplates,
            List<String> migratedComponentTemplates
        ) {
            this.migratedLegacyTemplates = Collections.unmodifiableList(migratedLegacyTemplates);
            this.migratedComposableTemplates = Collections.unmodifiableList(migratedComposableTemplates);
            this.migratedComponentTemplates = Collections.unmodifiableList(migratedComponentTemplates);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MigratedTemplates that = (MigratedTemplates) o;
            return Objects.equals(migratedLegacyTemplates, that.migratedLegacyTemplates)
                && Objects.equals(migratedComposableTemplates, that.migratedComposableTemplates)
                && Objects.equals(migratedComponentTemplates, that.migratedComponentTemplates);
        }

        @Override
        public int hashCode() {
            return Objects.hash(migratedLegacyTemplates, migratedComposableTemplates, migratedComponentTemplates);
        }
    }
}
