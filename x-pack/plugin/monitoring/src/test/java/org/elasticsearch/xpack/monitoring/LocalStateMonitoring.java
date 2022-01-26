/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.watcher.Watcher;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

public class LocalStateMonitoring extends LocalStateCompositeXPackPlugin {

    final Monitoring monitoring;

    public LocalStateMonitoring(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        LocalStateMonitoring thisVar = this;

        monitoring = new Monitoring(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected LicenseService getLicenseService() {
                return thisVar.getLicenseService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        };
        plugins.add(monitoring);
        plugins.add(new Watcher(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }

            @Override
            public Collection<Module> createGuiceModules() {
                return XPackPlugin.transportClientMode(settings) ? Collections.emptyList() : super.createGuiceModules();
            }
        });
        plugins.add(new IndexLifecycle(settings));
    }

    public Monitoring getMonitoring() {
        return monitoring;
    }

}
