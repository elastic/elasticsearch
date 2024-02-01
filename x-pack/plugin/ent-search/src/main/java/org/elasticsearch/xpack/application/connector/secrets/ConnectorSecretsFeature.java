/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets;

import org.elasticsearch.common.util.FeatureFlag;

/**
 * Connector Secrets feature flag. When the feature is complete, this flag will be removed.
 */
public class ConnectorSecretsFeature {

    private static final FeatureFlag SECRETS_FEATURE_FLAG = new FeatureFlag("connector_secrets");

    /**
     * Enables the Connectors Secrets feature by default for the tech preview phase.
     * As documented, the Connectors Secrets is currently a tech preview feature,
     * and customers should be aware that no SLAs or support are guaranteed during
     * its pre-General Availability (GA) stage.
     *
     * Instead of removing the feature flag from the code, we enable it by default.
     * This approach allows for the complete deactivation of the feature during the QA phase,
     * should any critical bugs be discovered, with a single, trackable code change.
     */
    public static boolean isEnabled() {
        return true;
    }
}
