/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.MarvelPlugin;

/**
 *
 */
public class MarvelShieldModule extends AbstractModule {

    private final boolean shieldEnabled;
    private final boolean marvelEnabled;

    public MarvelShieldModule(Settings settings) {
        this.shieldEnabled = MarvelShieldIntegration.enabled(settings);
        this.marvelEnabled = MarvelPlugin.marvelEnabled(settings);;
    }

    @Override
    protected void configure() {
        bind(MarvelShieldIntegration.class).asEagerSingleton();
        if (marvelEnabled) {
            bind(SecuredClient.class).asEagerSingleton();
        }
        if (shieldEnabled) {
            bind(MarvelSettingsFilter.Shield.class).asEagerSingleton();
            bind(MarvelSettingsFilter.class).to(MarvelSettingsFilter.Shield.class);
        } else {
            bind(MarvelSettingsFilter.class).toInstance(MarvelSettingsFilter.Noop.INSTANCE);
        }
    }
}
