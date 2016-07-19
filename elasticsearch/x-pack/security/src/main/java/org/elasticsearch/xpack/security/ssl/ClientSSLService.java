/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;

public class ClientSSLService extends AbstractSSLService {

    public ClientSSLService(Settings settings, Environment env, Global globalSSLConfiguration,
                            ResourceWatcherService resourceWatcherService) {
        super(settings, env, globalSSLConfiguration, resourceWatcherService);
    }

    @Override
    protected void validateSSLConfiguration(SSLConfiguration sslConfiguration) {
        sslConfiguration.keyConfig().validate();
        sslConfiguration.trustConfig().validate();
    }
}
