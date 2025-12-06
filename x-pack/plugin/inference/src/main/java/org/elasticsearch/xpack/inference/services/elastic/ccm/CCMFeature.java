/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;

public class CCMFeature {
    public static final ElasticsearchStatusException CCM_FORBIDDEN_EXCEPTION = new ElasticsearchStatusException(
        "Cloud Connected Mode configuration is not permitted for this environment",
        RestStatus.FORBIDDEN
    );

    public static final ElasticsearchStatusException CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION = new ElasticsearchStatusException(
        "Cloud Connected Mode functionality is not supported until the cluster is fully upgraded, please try again later",
        RestStatus.BAD_REQUEST
    );

    private final boolean isCcmSupportedEnvironment;

    public CCMFeature(Settings settings) {
        isCcmSupportedEnvironment = CCMSettings.CCM_SUPPORTED_ENVIRONMENT.get(settings);
    }

    public boolean isCcmSupportedEnvironment() {
        return isCcmSupportedEnvironment && CCMFeatureFlag.FEATURE_FLAG.isEnabled();
    }
}
