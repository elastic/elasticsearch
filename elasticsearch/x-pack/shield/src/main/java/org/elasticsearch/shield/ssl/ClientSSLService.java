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

public class ClientSSLService extends AbstractSSLService {

    @Inject
    public ClientSSLService(Settings settings, Global globalSSLConfiguration) {
        super(settings, null, globalSSLConfiguration, null);
    }

    @Inject(optional = true)
    public void setEnvironment(Environment environment) {
        this.env = environment;
    }

    @Inject(optional = true)
    public void setResourceWatcherService(ResourceWatcherService resourceWatcherService) {
        this.resourceWatcherService = resourceWatcherService;
    }

    @Override
    protected void validateSSLConfiguration(SSLConfiguration sslConfiguration) {
        sslConfiguration.keyConfig().validate();
        sslConfiguration.trustConfig().validate();
    }
}
