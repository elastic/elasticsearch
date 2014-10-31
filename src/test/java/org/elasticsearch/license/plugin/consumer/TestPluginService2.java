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
public class TestPluginService2 extends TestPluginServiceBase {


    public static String FEATURE_NAME = "feature2";

    @Inject
    public TestPluginService2(Settings settings, LicensesClientService licensesClientService) {
        super(settings, licensesClientService);
    }

    @Override
    public String featureName() {
        return FEATURE_NAME;
    }

    @Override
    public String settingPrefix() {
        return TestConsumerPlugin2.NAME;
    }
}
