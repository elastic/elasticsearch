/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.shield.authc.AuthenticationService;

import java.io.IOException;

/**
 *
 */
public class MarvelShieldIntegration {

    private final boolean enabled;
    private final AuthenticationService authcService;
    private final ShieldSettingsFilter settingsFilter;
    private final Client client;

    @Inject
    public MarvelShieldIntegration(Settings settings, Injector injector) {
        enabled = enabled(settings);
        authcService = enabled ? injector.getInstance(AuthenticationService.class) : null;
        settingsFilter = enabled ? injector.getInstance(ShieldSettingsFilter.class) : null;
        client = injector.getInstance(Client.class);
    }

    public Client getClient() {
        return client;
    }

    public void bindInternalMarvelUser() {
        if (authcService != null) {
            try {
                authcService.attachUserHeaderIfMissing(InternalMarvelUser.INSTANCE);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to attach marvel user to request", e);
            }
        }
    }

    public void filterOutSettings(String... patterns) {
        if (settingsFilter != null) {
            settingsFilter.filterOut(patterns);
        }
    }

    public static boolean enabled(Settings settings) {
        return ShieldPlugin.shieldEnabled(settings);
    }

}
