/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.license;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.LicenseVersion;
import org.elasticsearch.shield.ShieldVersion;
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
        bind(LicenseService.class).asEagerSingleton();
        bind(LicenseEventsNotifier.class).asEagerSingleton();
    }

    private void verifyLicensePlugin() {
        try {
            getClass().getClassLoader().loadClass("org.elasticsearch.license.plugin.LicensePlugin");
        } catch (ClassNotFoundException cnfe) {
            throw new ElasticsearchIllegalStateException("Shield plugin requires the elasticsearch-license plugin to be installed");
        }

        if (LicenseVersion.CURRENT.before(ShieldVersion.CURRENT.minLicenseCompatibilityVersion)) {
            throw new ElasticsearchIllegalStateException("Shield [" + ShieldVersion.CURRENT +
                    "] requires minumum License plugin version [" + ShieldVersion.CURRENT.minLicenseCompatibilityVersion +
                    "], but installed License plugin version is [" + LicenseVersion.CURRENT + "]");
        }
    }

}