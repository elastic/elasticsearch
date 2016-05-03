/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
import org.elasticsearch.watcher.ResourceWatcherService;

public class ServerSSLService extends AbstractSSLService {

    @Inject
    public ServerSSLService(Settings settings, Environment environment, Global globalSSLConfiguration,
                            ResourceWatcherService resourceWatcherService) {
        super(settings, environment, globalSSLConfiguration, resourceWatcherService);
    }

    @Override
    protected void validateSSLConfiguration(SSLConfiguration sslConfiguration) {
        if (sslConfiguration.keyConfig() == KeyConfig.NONE) {
            throw new IllegalArgumentException("a key must be configured to act as a server");
        }
        sslConfiguration.keyConfig().validate();
        sslConfiguration.trustConfig().validate();
    }
}
