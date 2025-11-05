/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.common.settings.Settings;

public class CCMFeature {
    private final boolean allowConfiguringCcm;

    public CCMFeature(Settings settings) {
        allowConfiguringCcm = CCMSettings.ALLOW_CONFIGURING_CCM.get(settings);
    }

    public boolean allowConfiguringCcm() {
        return allowConfiguringCcm && CCMFeatureFlag.FEATURE_FLAG.isEnabled();
    }
}
