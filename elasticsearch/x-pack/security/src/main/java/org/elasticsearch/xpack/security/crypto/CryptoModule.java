/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.support.AbstractSecurityModule;

/**
 *
 */
public class CryptoModule extends AbstractSecurityModule.Node {

    public CryptoModule(Settings settings) {
        super(settings);
    }

    @Override
    protected void configureNode() {
        if (securityEnabled == false) {
            bind(CryptoService.class).toProvider(Providers.of(null));
            return;
        }
        bind(InternalCryptoService.class).asEagerSingleton();
        bind(CryptoService.class).to(InternalCryptoService.class).asEagerSingleton();
    }
}
