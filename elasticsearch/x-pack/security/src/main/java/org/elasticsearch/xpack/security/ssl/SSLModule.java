/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;
import org.elasticsearch.xpack.security.support.AbstractSecurityModule;

/**
 *
 */
public class SSLModule extends AbstractSecurityModule {

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
