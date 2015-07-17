/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.license.plugin.LicenseVersion;
import org.elasticsearch.marvel.MarvelVersion;

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
            throw new IllegalStateException("marvel plugin requires the license plugin to be installed");
        }

        if (LicenseVersion.CURRENT.before(MarvelVersion.CURRENT.minLicenseCompatibilityVersion)) {
            throw new ElasticsearchException("marvel [" + MarvelVersion.CURRENT +
                    "] requires minimum license plugin version [" + MarvelVersion.CURRENT.minLicenseCompatibilityVersion +
                    "], but installed license plugin version is [" + LicenseVersion.CURRENT + "]");
        }
    }
}