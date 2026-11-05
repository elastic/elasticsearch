/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.features.NodeFeature;

/**
 * Sets or removes secure settings through {@code PUT _cluster/settings}. The REST handler sends a request that names a secure setting
 * to this action instead of {@link ClusterUpdateSettingsAction}, so it is authorized as a security action rather than under
 * {@code manage}. The transport action is registered by the secure-settings plugin, which stores the values encrypted in cluster state.
 */
public class UpdateSecureSettingsAction extends ActionType<AcknowledgedResponse> {

    public static final String NAME = "cluster:admin/xpack/security/secure_settings/update";
    public static final UpdateSecureSettingsAction INSTANCE = new UpdateSecureSettingsAction();

    public static final FeatureFlag SECURE_SETTINGS_API_FEATURE_FLAG = new FeatureFlag("secure_settings_api");

    /**
     * Published by the secure-settings plugin. The API is only used once every node in the cluster can read the stored values.
     */
    public static final NodeFeature SECURE_SETTINGS_API = new NodeFeature("secure_settings.api");

    private UpdateSecureSettingsAction() {
        super(NAME);
    }
}
