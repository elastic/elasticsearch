/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.license.plugin.consumer.EagerLicenseRegistrationConsumerPlugin;

public class LicensesEagerConsumerPluginIntegrationTests extends AbstractLicensesConsumerPluginIntegrationTests {

    public LicensesEagerConsumerPluginIntegrationTests() {
        super(new EagerLicenseRegistrationConsumerPlugin(ImmutableSettings.EMPTY));
    }
}
