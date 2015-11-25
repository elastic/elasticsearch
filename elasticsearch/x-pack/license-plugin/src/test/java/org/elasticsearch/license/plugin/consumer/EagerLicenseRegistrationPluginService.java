/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.consumer;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicensesService;

@Singleton
public class EagerLicenseRegistrationPluginService extends TestPluginServiceBase {

    public static String ID = "id1";

    @Inject
    public EagerLicenseRegistrationPluginService(Settings settings, LicensesService licensesClientService) {
        super(true, settings, licensesClientService, null);
    }

    @Override
    public String id() {
        return ID;
    }
}
