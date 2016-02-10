/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.license.core.LicenseVerifier;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.license.plugin.core.LicensesService;

public class LicensingModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(LicenseVerifier.class).asEagerSingleton();
        bind(LicensesService.class).asEagerSingleton();
        bind(LicenseeRegistry.class).to(LicensesService.class);
        bind(LicensesManagerService.class).to(LicensesService.class);
    }

}