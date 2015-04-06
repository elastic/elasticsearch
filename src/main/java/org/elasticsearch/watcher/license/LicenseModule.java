/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.license;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.license.plugin.LicenseVersion;
import org.elasticsearch.watcher.WatcherVersion;

/**
 *
 */
public class LicenseModule extends AbstractModule {

    public LicenseModule() {
        verifyLicensePlugin();
    }

    @Override
    protected void configure() {
        bind(LicenseService.class).asEagerSingleton();
    }

    private void verifyLicensePlugin() {
        try {
            getClass().getClassLoader().loadClass("org.elasticsearch.license.plugin.LicensePlugin");
        } catch (ClassNotFoundException cnfe) {
            throw new ElasticsearchIllegalStateException("watcher plugin requires the license plugin to be installed");
        }

        if (LicenseVersion.CURRENT.before(WatcherVersion.CURRENT.minLicenseCompatibilityVersion)) {
            throw new ElasticsearchIllegalStateException("watcher [" + WatcherVersion.CURRENT +
                    "] requires minimum license plugin version [" + WatcherVersion.CURRENT.minLicenseCompatibilityVersion +
                    "], but installed license plugin version is [" + LicenseVersion.CURRENT + "]");
        }
    }
}
