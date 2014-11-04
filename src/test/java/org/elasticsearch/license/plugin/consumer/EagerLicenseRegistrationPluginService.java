/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.consumer;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicensesClientService;

@Singleton
public class EagerLicenseRegistrationPluginService extends TestPluginServiceBase {

    public static String FEATURE_NAME = "feature1";

    @Inject
    public EagerLicenseRegistrationPluginService(Settings settings, LicensesClientService licensesClientService) {
        super(true, settings, licensesClientService, null);
    }

    @Override
    public String featureName() {
        return FEATURE_NAME;
    }

    @Override
    public String settingPrefix() {
        return EagerLicenseRegistrationConsumerPlugin.NAME;
    }
}
