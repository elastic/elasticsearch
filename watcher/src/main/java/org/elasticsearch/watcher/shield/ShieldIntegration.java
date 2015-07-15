/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.shield;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.shield.ShieldVersion;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.watcher.WatcherVersion;

import java.io.IOException;

/**
 *
 */
public class ShieldIntegration {

    private static final int MIN_SHIELD_VERSION = /*00*/2000001; // 2.0.0_beta1

    private final boolean installed;
    private final boolean enabled;
    private final Object authcService;
    private final Object userHolder;
    private final Object settingsFilter;

    @Inject
    public ShieldIntegration(Settings settings, Injector injector) {
        installed = installed(settings);
        enabled = installed && ShieldPlugin.shieldEnabled(settings);
        authcService = enabled ? injector.getInstance(AuthenticationService.class) : null;
        userHolder = enabled ? injector.getInstance(WatcherUserHolder.class) : null;
        settingsFilter = enabled ? injector.getInstance(ShieldSettingsFilter.class) : null;

    }

    public boolean installed() {
        return installed;
    }

    public boolean enabled() {
        return enabled;
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

    static boolean installed(Settings settings) {
        try {
            Class clazz = settings.getClassLoader().loadClass("org.elasticsearch.shield.ShieldPlugin");
            if (clazz == null) {
                return false;
            }

            // lets check min compatibility
            ShieldVersion minShieldVersion = ShieldVersion.fromId(MIN_SHIELD_VERSION);
            if (!ShieldVersion.CURRENT.onOrAfter(minShieldVersion)) {
                throw new IllegalStateException("watcher [" + WatcherVersion.CURRENT + "] requires " +
                        "minimum shield plugin version [" + minShieldVersion + "], but installed shield plugin version is " +
                        "[" + ShieldVersion.CURRENT + "]");
            }

            return true;

        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public static boolean enabled(Settings settings) {
        return installed(settings) && ShieldPlugin.shieldEnabled(settings);
    }

}
