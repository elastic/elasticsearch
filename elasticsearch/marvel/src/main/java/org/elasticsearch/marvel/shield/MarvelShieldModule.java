/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public class MarvelShieldModule extends AbstractModule {

    private final MarvelInternalUserHolder userHolder;
    private final boolean enabled;

    public MarvelShieldModule(Settings settings) {
        this.enabled = MarvelShieldIntegration.enabled(settings);
        userHolder = enabled ? new MarvelInternalUserHolder() : null;
    }

    @Override
    protected void configure() {
        bind(MarvelShieldIntegration.class).asEagerSingleton();
        bind(SecuredClient.class).asEagerSingleton();
        bind(MarvelInternalUserHolder.class).toProvider(Providers.of(userHolder));
        if (enabled) {
            bind(MarvelSettingsFilter.Shield.class).asEagerSingleton();
            bind(MarvelSettingsFilter.class).to(MarvelSettingsFilter.Shield.class);
        } else {
            bind(MarvelSettingsFilter.class).toInstance(MarvelSettingsFilter.Noop.INSTANCE);
        }
    }
}
