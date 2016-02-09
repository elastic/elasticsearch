/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.secret;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldPlugin;

/**
 *
 */
public class SecretModule extends AbstractModule {

    private final boolean shieldEnabled;

    public SecretModule(Settings settings) {
        shieldEnabled = ShieldPlugin.enabled(settings);
    }

    @Override
    protected void configure() {
        if (shieldEnabled) {
            bind(SecretService.Secure.class).asEagerSingleton();
            bind(SecretService.class).to(SecretService.Secure.class);
        } else {
            bind(SecretService.class).toInstance(SecretService.Insecure.INSTANCE);
        }
    }
}
