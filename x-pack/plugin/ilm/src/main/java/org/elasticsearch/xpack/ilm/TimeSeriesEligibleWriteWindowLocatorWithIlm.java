/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.dlm.TimeSeriesEligibleWriteWindowLocator;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

/**
 * Extends {@link TimeSeriesEligibleWriteWindowLocator} to calculate the eligible write window including
 * ILM policies as well.
 */
public class TimeSeriesEligibleWriteWindowLocatorWithIlm extends TimeSeriesEligibleWriteWindowLocator {

    /**
     * Returns the effective ILM policy name for the provided data stream based on the index template
     * that is associated with this data stream. If the data stream has a data stream lifecycle configured,
     * it will take it into account and return null if the data stream lifecycle is effective. This choice
     * was made for performance reasons, since we need to resolve the settings for both the policy and the
     * `prefer_ilm` setting.
     * @param dataStream the requested data stream
     * @param projectMetadata the project metadata based on which we will resolve the data stream configuration
     * @return the effective ILM policy name or null
     */
    @Override
    protected String getEffectiveIlmPolicy(DataStream dataStream, ProjectMetadata projectMetadata) {
        String indexTemplateName = MetadataIndexTemplateService.findV2Template(projectMetadata, dataStream.getName(), false);
        if (indexTemplateName == null) {
            return null;
        }
        ComposableIndexTemplate indexTemplate = projectMetadata.templatesV2().get(indexTemplateName);
        Settings settings = MetadataIndexTemplateService.resolveSettings(indexTemplate, projectMetadata.componentTemplates());
        final var policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(settings);
        if (Strings.hasText(policyName) == false) {
            return null;
        }
        // If there is only one of the lifecycle features configured, we return the policy name if available otherwise null.
        if (dataStream.getDataLifecycle() == null) {
            return policyName;
        }
        // If both are configured, ILM is in effect only if prefer_ilm is true.
        return IndexSettings.PREFER_ILM_SETTING.get(settings) ? policyName : null;
    }

    /**
     * Retrieves the ILM policy from the project metadata and returns the `min_age` of the first phase
     * with a read-only action. The read-only actions are defined in
     * {@link TimeseriesLifecycleType#READ_ONLY_ACTIONS}.
     * @return the min age of the first phase that makes an index read-only, otherwise -1
     */
    @Override
    protected long getEligibleWriteWindowFromPolicy(String policy, ProjectMetadata projectMetadata) {
        IndexLifecycleMetadata metadata = projectMetadata.custom(IndexLifecycleMetadata.TYPE);
        if (metadata == null) {
            return -1;
        }
        LifecyclePolicyMetadata lifecyclePolicyMetadata = metadata.getPolicyMetadatas().get(policy);
        if (lifecyclePolicyMetadata == null) {
            return -1;
        }
        for (String phaseName : TimeseriesLifecycleType.ORDERED_VALID_PHASES) {
            Phase phase = lifecyclePolicyMetadata.getPolicy().getPhases().get(phaseName);
            if (isReadOnlyPhase(phase)) {
                return phase.getMinimumAge().millis();
            }
        }
        return -1;
    }

    private boolean isReadOnlyPhase(Phase phase) {
        if (phase == null) {
            return false;
        }
        for (String readOnlyAction : TimeseriesLifecycleType.READ_ONLY_ACTIONS) {
            if (phase.getActions().containsKey(readOnlyAction)) {
                return true;
            }
        }
        return false;
    }
}
