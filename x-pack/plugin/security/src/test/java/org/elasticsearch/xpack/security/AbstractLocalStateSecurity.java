/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.nio.file.Path;
import java.util.List;

public abstract class AbstractLocalStateSecurity extends LocalStateCompositeXPackPlugin implements ReloadablePlugin {
    @SuppressWarnings("this-escape")
    public AbstractLocalStateSecurity(Settings settings, Path configPath) {
        super(settings, configPath);

        plugins.add(new Security(settings, AbstractLocalStateSecurity.this.securityExtensions()) {
            @Override
            protected SSLService getSslService() {
                return AbstractLocalStateSecurity.this.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return AbstractLocalStateSecurity.this.getLicenseState();
            }
        });
    }

    @Override
    public void reload(Settings settings) throws Exception {
        plugins.stream().filter(p -> p instanceof ReloadablePlugin).forEach(p -> {
            try {
                ((ReloadablePlugin) p).reload(settings);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected List<SecurityExtension> securityExtensions() {
        return List.of();
    }
}
