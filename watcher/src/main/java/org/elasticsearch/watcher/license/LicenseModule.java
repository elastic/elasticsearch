/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.license;

import org.elasticsearch.common.inject.AbstractModule;

/**
 *
 */
public class LicenseModule extends AbstractModule {

    public LicenseModule() {
        verifyLicensePlugin();
    }

    @Override
    protected void configure() {
        bind(WatcherLicensee.class).asEagerSingleton();
    }

    private void verifyLicensePlugin() {
        try {
            getClass().getClassLoader().loadClass("org.elasticsearch.license.plugin.LicensePlugin");
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException("watcher plugin requires the license plugin to be installed");
        }
    }
}
