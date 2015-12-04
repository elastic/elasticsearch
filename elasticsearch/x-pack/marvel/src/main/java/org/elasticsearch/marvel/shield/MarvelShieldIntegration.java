/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.HasContext;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.transport.TransportMessage;

import java.io.IOException;

/**
 *
 */
public class MarvelShieldIntegration {

    private final Object authcService;
    private final Object userHolder;
    private final Object settingsFilter;

    @Inject
    public MarvelShieldIntegration(Settings settings, Injector injector) {
        boolean enabled = enabled(settings);
        authcService = enabled ? injector.getInstance(AuthenticationService.class) : null;
        userHolder = enabled ? injector.getInstance(MarvelInternalUserHolder.class) : null;
        settingsFilter = enabled ? injector.getInstance(ShieldSettingsFilter.class) : null;
    }

    public void bindInternalMarvelUser(TransportMessage message) {
        if (authcService != null) {
            try {
                ((AuthenticationService) authcService).attachUserHeaderIfMissing(message, ((MarvelInternalUserHolder) userHolder).user);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to attach watcher user to request", e);
            }
        }
    }

    public void filterOutSettings(String... patterns) {
        if (settingsFilter != null) {
            ((ShieldSettingsFilter) settingsFilter).filterOut(patterns);
        }
    }

    static boolean installed() {
        try {
            MarvelShieldIntegration.class.getClassLoader().loadClass("org.elasticsearch.shield.ShieldPlugin");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public static boolean enabled(Settings settings) {
        return installed() && ShieldPlugin.shieldEnabled(settings);
    }

}
