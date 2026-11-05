/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsAction;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class SecureSettingsFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        if (UpdateSecureSettingsAction.SECURE_SETTINGS_API_FEATURE_FLAG.isEnabled()) {
            return Set.of(UpdateSecureSettingsAction.SECURE_SETTINGS_API);
        }
        return Set.of();
    }
}
