/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class SSLModule extends AbstractShieldModule {

    public SSLModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configure(boolean clientMode) {
        bind(Global.class).asEagerSingleton();
        bind(ClientSSLService.class).asEagerSingleton();
        if (clientMode) {
            bind(ServerSSLService.class).toProvider(Providers.<ServerSSLService>of(null));
        } else {
            bind(ServerSSLService.class).asEagerSingleton();
        }
    }
}
