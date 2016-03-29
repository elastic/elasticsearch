/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.consumer;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;

public abstract class TestConsumerPluginBase extends Plugin {

    private final boolean isEnabled;

    public TestConsumerPluginBase(Settings settings) {
        this.isEnabled = TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey())) == false;
    }

    @Override
    public String name() {
        return pluginName();
    }

    @Override
    public String description() {
        return "test licensing consumer plugin";
    }


    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        if (isEnabled) {
            services.add(service());
        }
        return services;
    }

    public void onModule(SettingsModule module) {
        try {
            module.registerSetting(Setting.simpleString("_trial_license_duration_in_seconds", Setting.Property.NodeScope));
            module.registerSetting(Setting.simpleString("_grace_duration_in_seconds", Setting.Property.NodeScope));
        } catch (IllegalArgumentException ex) {
            // already loaded
        }
    }

    public abstract Class<? extends TestPluginServiceBase> service();

    protected abstract String pluginName();

    public abstract String id();
}
