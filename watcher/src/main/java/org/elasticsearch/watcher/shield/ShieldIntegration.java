/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.shield;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.HasContext;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.transport.TransportMessage;

import java.io.IOException;

/**
 *
 */
public class ShieldIntegration {

    private static final int MIN_SHIELD_VERSION = /*00*/2000001; // 2.0.0_beta1

    private final Object authcService;
    private final Object userHolder;
    private final Object settingsFilter;

    @Inject
    public ShieldIntegration(Settings settings, Injector injector) {
        boolean enabled = enabled(settings);
        authcService = enabled ? injector.getInstance(AuthenticationService.class) : null;
        userHolder = enabled ? injector.getInstance(WatcherUserHolder.class) : null;
        settingsFilter = enabled ? injector.getInstance(ShieldSettingsFilter.class) : null;
    }

    public void bindWatcherUser(TransportMessage message) {
        if (authcService != null) {
            try {
                ((AuthenticationService) authcService).attachUserHeaderIfMissing(message, ((WatcherUserHolder) userHolder).user);
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

    // TODO this is a hack that needs to go away with proper fixes in core
    public void putUserInContext(HasContext context) {
        if (userHolder != null) {
            context.putInContext("_shield_user", ((WatcherUserHolder) userHolder).user);
        }
    }

    static boolean installed() {
        try {
            ShieldIntegration.class.getClassLoader().loadClass("org.elasticsearch.shield.ShieldPlugin");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public static boolean enabled(Settings settings) {
        return installed() && ShieldPlugin.shieldEnabled(settings);
    }

}
