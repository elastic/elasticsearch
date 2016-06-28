/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.consumer;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

/**
 * Registers licenses only after cluster has recovered
 * see {@link LazyLicenseRegistrationPluginService}
 * <p>
 * License registration happens after clusterservice start()
 */
public class LazyLicenseRegistrationConsumerPlugin extends TestConsumerPluginBase {

    @Inject
    public LazyLicenseRegistrationConsumerPlugin(Settings settings) {
        super(settings);
    }

    @Override
    public Class<? extends TestPluginServiceBase> service() {
        return LazyLicenseRegistrationPluginService.class;
    }

    @Override
    public String id() {
        return LazyLicenseRegistrationPluginService.ID;
    }
}
