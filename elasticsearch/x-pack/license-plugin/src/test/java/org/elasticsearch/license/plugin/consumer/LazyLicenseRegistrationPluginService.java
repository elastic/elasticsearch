/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.consumer;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicensesService;

@Singleton
public class LazyLicenseRegistrationPluginService extends TestPluginServiceBase {

    public static String ID = "id2";

    @Inject
    public LazyLicenseRegistrationPluginService(Settings settings, LicensesService licensesClientService, ClusterService clusterService) {
        super(false, settings, licensesClientService, clusterService);
    }

    @Override
    public String id() {
        return ID;
    }
}
