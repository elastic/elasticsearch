/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.inject.AbstractModule;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * A module that binds the provided settings to the {@link Settings} interface.
 *
 *
 */
public class SettingsModule extends AbstractModule {

    private final Settings settings;
    private final SettingsFilter settingsFilter;
    private final Map<String, Setting<?>> clusterDynamicSettings = new HashMap<>();


    public SettingsModule(Settings settings, SettingsFilter settingsFilter) {
        this.settings = settings;
        this.settingsFilter = settingsFilter;
        for (Setting<?> setting : ClusterSettings.BUILT_IN_CLUSTER_SETTINGS) {
            registerSetting(setting);
        }
    }

    @Override
    protected void configure() {
        bind(Settings.class).toInstance(settings);
        bind(SettingsFilter.class).toInstance(settingsFilter);
        final ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(clusterDynamicSettings.values()));
        bind(ClusterSettings.class).toInstance(clusterSettings);
    }

    public void registerSetting(Setting<?> setting) {
        switch (setting.getScope()) {
            case CLUSTER:
                if (clusterDynamicSettings.containsKey(setting.getKey())) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                clusterDynamicSettings.put(setting.getKey(), setting);
                break;
            case INDEX:
                throw new UnsupportedOperationException("not yet implemented");
        }
    }

}
