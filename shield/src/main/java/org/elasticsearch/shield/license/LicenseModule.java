/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.license;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class LicenseModule extends AbstractShieldModule.Node {

    public LicenseModule(Settings settings) {
        super(settings);
        verifyLicensePlugin();
    }

    @Override
    protected void configureNode() {
        bind(ShieldLicensee.class).asEagerSingleton();
        bind(ShieldLicenseState.class).asEagerSingleton();
    }

    private void verifyLicensePlugin() {
        try {
            getClass().getClassLoader().loadClass("org.elasticsearch.license.plugin.LicensePlugin");
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException("shield plugin requires the license plugin to be installed");
        }
    }

}
