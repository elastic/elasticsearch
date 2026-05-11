/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialsExtension;
import org.elasticsearch.xpack.core.security.cloud.InternalCloudApiKeyService;
import org.elasticsearch.xpack.ml.autoscaling.AbstractNodeAvailabilityZoneMapper;

public interface MachineLearningExtension {

    default void configure(Settings settings) {}

    boolean includeNodeInfo();

    default boolean disableInferenceProcessCache() {
        return false;
    }

    String[] getAnalyticsDestIndexAllowedSettings();

    AbstractNodeAvailabilityZoneMapper getNodeAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings);

    /**
     * Cloud credential manager used by datafeeds for cross-project search authentication.
     */
    default CloudCredentialManager cloudCredentialManager() {
        return CloudCredentialsExtension.getInstance().credentialManager();
    }

    /**
     * Internal cloud API key service used by datafeeds to mint per-datafeed cloud-managed credentials.
     */
    default InternalCloudApiKeyService internalCloudApiKeyService() {
        return CloudCredentialsExtension.getInstance().internalCloudApiKeyService();
    }
}
