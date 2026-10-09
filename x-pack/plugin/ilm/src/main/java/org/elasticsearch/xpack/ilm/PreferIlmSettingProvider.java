/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Defaults {@link IndexSettings#PREFER_ILM} to {@code false} on new data stream backing indices whose ILM policy is the
 * pre-installed metrics policy and the user has not customised it, as determined by {@link #isMetricPolicyUnchanged(ProjectMetadata)}.
 * With prefer_ilm set to false, all affected indices will be managed by the data stream lifecycle configured on the data stream.
 * <p>
 * This provider only supplies a default. It deliberately does not
 * {@link IndexSettingProvider#overrulesTemplateAndRequestSettings() overrule} the template and request settings, so a
 * user who sets {@code index.lifecycle.prefer_ilm} explicitly keeps that value; the merging in
 * {@code MetadataCreateIndexService} applies template and request settings on top of what we provide here.
 * <p>
 * Note that the decision is taken once per index, at creation time, from the state of the policy at that moment.
 * Customising the policy later does not move indices that were already created back under ILM, it only changes what
 * subsequent backing indices get. That is by design: {@code prefer_ilm} is a per-index setting and
 * {@link org.elasticsearch.cluster.metadata.DataStream#lifecycleManagedBy} evaluates it per index.
 * <p>
 * The provider can be turned on or off at runtime with {@link #ENABLED_SETTING}. Turning it off only affects indices created
 * afterwards.
 */
public class PreferIlmSettingProvider implements IndexSettingProvider {

    private static final Logger logger = LogManager.getLogger(PreferIlmSettingProvider.class);
    private static final String METRICS_POLICY_NAME = "metrics";

    /**
     * Whether new backing indices of data streams managed by the unmodified pre-installed metrics policy default
     * {@link IndexSettings#PREFER_ILM} to {@code false}. When disabled, this provider supplies no settings.
     */
    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "data_streams.lifecycle.prefer_by_default.metrics_enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean enabled;

    private PreferIlmSettingProvider(boolean enabled) {
        this.enabled = enabled;
    }

    public static PreferIlmSettingProvider create(ClusterSettings clusterSettings) {
        PreferIlmSettingProvider preferIlmSettingProvider = new PreferIlmSettingProvider(clusterSettings.get(ENABLED_SETTING));
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, preferIlmSettingProvider::setEnabled);
        return preferIlmSettingProvider;
    }

    @Override
    public void provideAdditionalSettings(
        String indexName,
        @Nullable String dataStreamName,
        @Nullable IndexMode templateIndexMode,
        boolean registryInstalledTemplate,
        ProjectMetadata projectMetadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings,
        IndexVersion indexVersion,
        Settings.Builder additionalSettings
    ) {
        if (enabled == false) {
            return;
        }
        if (dataStreamName == null) {
            return;
        }
        if (templateIndexMode != IndexMode.TIME_SERIES) {
            return;
        }
        if (indexTemplateAndCreateRequestSettings.hasValue(IndexSettings.PREFER_ILM)) {
            return;
        }
        String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexTemplateAndCreateRequestSettings);
        if (METRICS_POLICY_NAME.equals(policyName) == false || isMetricPolicyUnchanged(projectMetadata) == false) {
            // if settings do not resolve to the unchanged metrics policy, then we do not set prefer_ilm
            return;
        }
        logger.debug(
            "index [{}] of data stream [{}] uses the unmodified managed policy [{}], defaulting [{}] to false",
            indexName,
            dataStreamName,
            policyName,
            IndexSettings.PREFER_ILM
        );
        additionalSettings.put(IndexSettings.PREFER_ILM, false);
    }

    /**
     * Determines whether the metrics policy is the pre-installed one, and it hasn't been customised, namely:
     * <ul>
     *     <li>it is still on its first version, so it has not been updated since it was installed;</li>
     *     <li>it consists of nothing but a hot phase whose only action is a rollover.</li>
     * </ul>
     *
     * @param projectMetadata the project holding the {@link IndexLifecycleMetadata}
     * @return {@code true} only when all of the above hold; a missing policy or missing ILM metadata yields
     *         {@code false}, since we cannot establish that it is safe to treat the policy as ours
     */
    public boolean isMetricPolicyUnchanged(ProjectMetadata projectMetadata) {
        String policyName = "metrics";
        IndexLifecycleMetadata metadata = projectMetadata.custom(IndexLifecycleMetadata.TYPE);
        if (metadata == null) {
            return false;
        }
        LifecyclePolicyMetadata lifecyclePolicyMetadata = metadata.getPolicyMetadatas().get(policyName);
        if (lifecyclePolicyMetadata == null) {
            return false;
        }
        if (lifecyclePolicyMetadata.getVersion() > 1) {
            // The policy has been written at least once since it was installed. We cannot tell a registry upgrade apart
            // from a user edit here, so we err on the side of leaving the policy in charge.
            return false;
        }
        Map<String, Phase> phases = lifecyclePolicyMetadata.getPolicy().getPhases();
        if (phases.size() > 1) {
            return false;
        }
        Phase hotPhase = phases.get(TimeseriesLifecycleType.HOT_PHASE);
        return hotPhase != null && hotPhase.getActions().size() == 1 && hotPhase.getActions().containsKey(RolloverAction.NAME);
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
