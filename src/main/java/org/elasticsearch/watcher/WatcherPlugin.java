/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.watcher.actions.email.service.InternalEmailService;
import org.elasticsearch.watcher.history.HistoryModule;
import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.init.InitializingService;
import org.elasticsearch.watcher.support.validation.WatcherSettingsValidation;

import java.util.Collection;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public class WatcherPlugin extends AbstractPlugin {

    public static final String NAME = "watcher";
    public static final String ENABLED_SETTING = NAME + ".enabled";

    private final Settings settings;
    private final boolean transportClient;
    protected final boolean enabled;

    public WatcherPlugin(Settings settings) {
        this.settings = settings;
        transportClient = "transport".equals(settings.get(Client.CLIENT_TYPE_SETTING));
        enabled = watcherEnabled(settings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public String description() {
        return "Elasticsearch Watcher";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        if (!enabled) {
            return ImmutableList.of();
        }
        return transportClient ?
                ImmutableList.<Class<? extends Module>>of(TransportClientWatcherModule.class) :
                ImmutableList.<Class<? extends Module>>of(WatcherModule.class);
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        if (!enabled || transportClient) {
            return ImmutableList.of();
        }
        return ImmutableList.<Class<? extends LifecycleComponent>>of(
                // the initialization service must be first in the list
                // as other services may depend on one of the initialized
                // constructs
                InitializingService.class,
                LicenseService.class,
                InternalEmailService.class,
                HttpClient.class,
                WatcherSettingsValidation.class);
    }

    @Override
    public Settings additionalSettings() {
        if (!enabled || transportClient) {
            return Settings.EMPTY;
        }
        Settings additionalSettings = settingsBuilder()
                .put(HistoryModule.additionalSettings(settings))
                .build();

        return additionalSettings;
    }

    public static boolean watcherEnabled(Settings settings) {
        return settings.getAsBoolean(ENABLED_SETTING, true);
    }

}
