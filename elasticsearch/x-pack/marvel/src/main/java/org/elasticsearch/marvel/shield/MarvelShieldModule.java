/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public class MarvelShieldModule extends AbstractModule {

    private final boolean shieldEnabled;

    public MarvelShieldModule(Settings settings) {
        this.shieldEnabled = MarvelShieldIntegration.enabled(settings);
    }

    @Override
    protected void configure() {
        bind(MarvelShieldIntegration.class).asEagerSingleton();
        if (shieldEnabled) {
            bind(MarvelSettingsFilter.Shield.class).asEagerSingleton();
            bind(MarvelSettingsFilter.class).to(MarvelSettingsFilter.Shield.class);
        } else {
            bind(MarvelSettingsFilter.class).toInstance(MarvelSettingsFilter.Noop.INSTANCE);
        }
    }
}
