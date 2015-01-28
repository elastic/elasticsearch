/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerModule;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 *
 */
public class ShieldNettyHttpServerTransportModule extends AbstractShieldModule implements PreProcessModule {

    public ShieldNettyHttpServerTransportModule(Settings settings) {
        super(settings);
    }

    @Override
    public void processModule(Module module) {
        if (module instanceof HttpServerModule) {
            ((HttpServerModule) module).setHttpServerTransport(ShieldNettyHttpServerTransport.class, ShieldPlugin.NAME);
        }
    }

    @Override
    protected void configure(boolean clientMode) {
    }
}