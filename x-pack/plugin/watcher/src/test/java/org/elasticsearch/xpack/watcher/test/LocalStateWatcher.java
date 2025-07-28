/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.watcher.Watcher;

import java.nio.file.Path;

public class LocalStateWatcher extends LocalStateCompositeXPackPlugin implements ReloadablePlugin {

    private final Watcher watcher;

    public LocalStateWatcher(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        LocalStateWatcher thisVar = this;
        this.watcher = new Watcher(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }

        };
        plugins.add(watcher);
    }

    @Override
    public void reload(Settings settings) throws Exception {
        this.watcher.reload(settings);
    }
}
