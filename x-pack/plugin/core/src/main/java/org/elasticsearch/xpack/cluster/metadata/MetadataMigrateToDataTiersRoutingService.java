/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.Phase;

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
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_PREFER;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPED;
import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagement.updateIndicesForPolicy;

/**
 * Exposes the necessary methods to migrate a system's elasticsearch abstractions to use data tiers for index allocation routing.
 */
public final class MetadataMigrateToDataTiersRoutingService {

    public static final String DEFAULT_NODE_ATTRIBUTE_NAME = "data";
    private static final Logger logger = LogManager.getLogger(MetadataMigrateToDataTiersRoutingService.class);

    private MetadataMigrateToDataTiersRoutingService() {
    }

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
     *  If the require or include setting is successfully migrated to _tier_preference, the **other** routing settings for the
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
     * If no @param nodeAttrName is provided "data" will be used.
     * If no @param indexTemplateToDelete is provided, no index templates will be deleted.
     *
     * This returns a new {@link ClusterState} representing the migrated state that is ready to use data tiers for index and
     * ILM routing allocations. It also returns a summary of the affected abstractions encapsulated in {@link MigratedEntities}
     */
    public static Tuple<ClusterState, MigratedEntities> migrateToDataTiersRouting(ClusterState currentState,
                                                                                  @Nullable String nodeAttrName,
                                                                                  @Nullable String indexTemplateToDelete,
                                                                                  NamedXContentRegistry xContentRegistry, Client client) {
        IndexLifecycleMetadata currentMetadata = currentState.metadata().custom(IndexLifecycleMetadata.TYPE);
        if (currentMetadata != null && currentMetadata.getOperationMode() != STOPPED) {
            throw new IllegalStateException("stop ILM before migrating to data tiers, current state is [" +
                currentMetadata.getOperationMode() + "]");
        }

        Metadata.Builder mb = Metadata.builder(currentState.metadata());
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
        List<String> migratedPolicies = migrateIlmPolicies(mb, currentState, attribute, xContentRegistry, client);
        List<String> migratedIndices = migrateIndices(mb, currentState, attribute);
        return Tuple.tuple(ClusterState.builder(currentState).metadata(mb).build(),
            new MigratedEntities(removedIndexTemplateName, migratedIndices, migratedPolicies));
    }

    /**
     * Iterate through the existing ILM policies and look at the configured {@link AllocateAction}s. If they define *any* routing rules
     * based on the provided node attribute name (we look at include, exclude, and require rules) *ALL* the rules in the allocate
     * action will be removed. All the rules are removed in order to allow for ILM to inject the {@link MigrateAction}.
     * This also iterates through all the indices that are executing a given *migrated* policy and refreshes the cached phase definition
     * for each of these managed indices.
     */
    static List<String> migrateIlmPolicies(Metadata.Builder mb, ClusterState currentState, String nodeAttrName,
                                           NamedXContentRegistry xContentRegistry, Client client) {
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
                LifecyclePolicyMetadata newPolicyMetadata = new LifecyclePolicyMetadata(newLifecyclePolicy,
                    policyMetadataEntry.getValue().getHeaders(), nextVersion, Instant.now().toEpochMilli());
                LifecyclePolicyMetadata oldPolicyMetadata = newPolicies.put(policyMetadataEntry.getKey(), newPolicyMetadata);
                assert oldPolicyMetadata != null :
                    "we must only update policies, not create new ones, but " + policyMetadataEntry.getKey() + " didn't exist";

                updateIndicesForPolicy(mb, currentState, xContentRegistry, client, oldPolicyMetadata.getPolicy(), newPolicyMetadata);
                migratedPolicies.add(policyMetadataEntry.getKey());
            }
        }

        IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentLifecycleMetadata.getOperationMode());
        mb.putCustom(IndexLifecycleMetadata.TYPE, newMetadata);
        return migratedPolicies;
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
                if (allocateAction.getNumberOfReplicas() != null) {
                    // keep the number of replicas configuration
                    AllocateAction updatedAllocateAction =
                        new AllocateAction(allocateAction.getNumberOfReplicas(), null, null, null);
                    actionMap.put(allocateAction.getWriteableName(), updatedAllocateAction);
                    logger.debug("ILM policy [{}], phase [{}]: updated the allocate action to [{}]", lifecyclePolicy.getName(),
                        phase.getName(), allocateAction);
                } else {
                    // remove the action altogether
                    actionMap.remove(allocateAction.getWriteableName());
                    logger.debug("ILM policy [{}], phase [{}]: removed the allocate action", lifecyclePolicy.getName(),
                        phase.getName());
                }

                Phase updatedPhase = new Phase(phase.getName(), phase.getMinimumAge(), actionMap);
                Map<String, Phase> updatedPhases =
                    new HashMap<>(newLifecyclePolicy == null ? lifecyclePolicy.getPhases() : newLifecyclePolicy.getPhases());
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
        return allocateAction != null && (allocateAction.getRequire().get(nodeAttrName) != null ||
            allocateAction.getInclude().get(nodeAttrName) != null ||
            allocateAction.getExclude().get(nodeAttrName) != null);
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
        for (ObjectObjectCursor<String, IndexMetadata> index : currentState.metadata().indices()) {
            IndexMetadata indexMetadata = index.value;
            Settings currentSettings = indexMetadata.getSettings();
            Settings newSettings = maybeMigrateRoutingSettingToTierPreference(nodeAttrIndexRequireRoutingSetting, indexMetadata);
            if (newSettings.equals(currentSettings)) {
                // migrating based on the `require` setting was not successful so let's check if the index used the `include` routing
                // setting to configure the allocations and try to migrate it
                newSettings = maybeMigrateRoutingSettingToTierPreference(nodeAttrIndexIncludeRoutingSetting, indexMetadata);
            }

            if (newSettings.equals(currentSettings) == false) {
                // we converted either the require or the include routing setting to tier preference
                // so let's clear all the routing settings for the given attribute
                Settings.Builder finalSettings = Settings.builder().put(newSettings);
                finalSettings.remove(nodeAttrIndexExcludeRoutingSetting);
                finalSettings.remove(nodeAttrIndexRequireRoutingSetting);
                finalSettings.remove(nodeAttrIndexIncludeRoutingSetting);

                mb.put(IndexMetadata.builder(indexMetadata)
                    .settings(finalSettings)
                    .settingsVersion(indexMetadata.getSettingsVersion() + 1));
                migratedIndices.add(indexMetadata.getIndex().getName());
            }
        }
        return migratedIndices;
    }

    /**
     * Attempts to migrate the value of the given attribute routing setting to the _tier_preference equivalent. The provided setting
     * needs to be configured and have one of the supported values (hot, warm, cold, or frozen) in order for the migration to be preformed.
     * If the migration is successful the provided setting will be removed.
     *
     * If the migration is **not** executed the current index settings is returned, otherwise the updated settings are returned
     */
    private static Settings maybeMigrateRoutingSettingToTierPreference(String attributeBasedRoutingSettingName,
                                                                       IndexMetadata indexMetadata) {
        Settings currentIndexSettings = indexMetadata.getSettings();
        if (currentIndexSettings.keySet().contains(attributeBasedRoutingSettingName) == false) {
            return currentIndexSettings;
        }
        // look at the value, get the correct tiers config and update the settings and index metadata
        Settings.Builder newSettingsBuilder = Settings.builder().put(currentIndexSettings);
        String indexName = indexMetadata.getIndex().getName();
        if (currentIndexSettings.keySet().contains(INDEX_ROUTING_PREFER)) {
            newSettingsBuilder.remove(attributeBasedRoutingSettingName);
            logger.debug("index [{}]: removed setting [{}]", indexName, attributeBasedRoutingSettingName);
        } else {
            // parse the custom attribute routing into the corresponding tier preference and configure it
            String attributeValue = currentIndexSettings.get(attributeBasedRoutingSettingName);
            String convertedTierPreference = convertAttributeValueToTierPreference(attributeValue);
            if (convertedTierPreference != null) {
                newSettingsBuilder.put(INDEX_ROUTING_PREFER, convertedTierPreference);
                newSettingsBuilder.remove(attributeBasedRoutingSettingName);
                logger.debug("index [{}]: removed setting [{}]", indexName, attributeBasedRoutingSettingName);
                logger.debug("index [{}]: configured setting [{}] to [{}]", indexName,
                    INDEX_ROUTING_PREFER, convertedTierPreference);
            } else {
                // log warning and do *not* remove setting, return the settings unchanged
                logger.warn("index [{}]: could not convert attribute based setting [{}] value of [{}] to a tier preference " +
                        "configuration. the only known values are: {}", indexName,
                    attributeBasedRoutingSettingName, attributeValue, "hot,warm,cold, and frozen");
                return currentIndexSettings;
            }
        }
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

        public MigratedEntities(@Nullable String removedIndexTemplateName, List<String> migratedIndices, List<String> migratedPolicies) {
            this.removedIndexTemplateName = removedIndexTemplateName;
            this.migratedIndices = Collections.unmodifiableList(migratedIndices);
            this.migratedPolicies = Collections.unmodifiableList(migratedPolicies);
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
            return Objects.equals(removedIndexTemplateName, that.removedIndexTemplateName) &&
                Objects.equals(migratedIndices, that.migratedIndices) &&
                Objects.equals(migratedPolicies, that.migratedPolicies);
        }

        @Override
        public int hashCode() {
            return Objects.hash(removedIndexTemplateName, migratedIndices, migratedPolicies);
        }
    }
}
