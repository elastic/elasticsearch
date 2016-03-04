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
import org.elasticsearch.tribe.TribeService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A module that binds the provided settings to the {@link Settings} interface.
 */
public class SettingsModule extends AbstractModule {

    private final Settings settings;
    private final Set<String> settingsFilterPattern = new HashSet<>();
    private final Map<String, Setting<?>> nodeSettings = new HashMap<>();
    private final Map<String, Setting<?>> indexSettings = new HashMap<>();
    private static final Predicate<String> TRIBE_CLIENT_NODE_SETTINGS_PREDICATE =  (s) -> s.startsWith("tribe.") && TribeService.TRIBE_SETTING_KEYS.contains(s) == false;

    public SettingsModule(Settings settings) {
        this.settings = settings;
        for (Setting<?> setting : ClusterSettings.BUILT_IN_CLUSTER_SETTINGS) {
            registerSetting(setting);
        }
        for (Setting<?> setting : IndexScopedSettings.BUILT_IN_INDEX_SETTINGS) {
            registerSetting(setting);
        }
    }

    @Override
    protected void configure() {
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(settings, new HashSet<>(this.indexSettings.values()));
        final ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(this.nodeSettings.values()));
        // by now we are fully configured, lets check node level settings for unregistered index settings
        indexScopedSettings.validate(settings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE));
        final Predicate<String> acceptOnlyClusterSettings = TRIBE_CLIENT_NODE_SETTINGS_PREDICATE.or(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE).negate();
        clusterSettings.validate(settings.filter(acceptOnlyClusterSettings));
        validateTribeSettings(settings, clusterSettings);
        bind(Settings.class).toInstance(settings);
        bind(SettingsFilter.class).toInstance(new SettingsFilter(settings, settingsFilterPattern));

        bind(ClusterSettings.class).toInstance(clusterSettings);
        bind(IndexScopedSettings.class).toInstance(indexScopedSettings);
    }

    /**
     * Registers a new setting. This method should be used by plugins in order to expose any custom settings the plugin defines.
     * Unless a setting is registered the setting is unusable. If a setting is never the less specified the node will reject
     * the setting during startup.
     */
    public void registerSetting(Setting<?> setting) {
        if (setting.isFiltered()) {
            if (settingsFilterPattern.contains(setting.getKey()) == false) {
                registerSettingsFilter(setting.getKey());
            }
        }
        if (setting.hasNodeScope()) {
            if (nodeSettings.containsKey(setting.getKey())) {
                throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
            }
            nodeSettings.put(setting.getKey(), setting);
        } else if (setting.hasIndexScope()) {
            if (indexSettings.containsKey(setting.getKey())) {
                throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
            }
            indexSettings.put(setting.getKey(), setting);
        } else {
            throw new IllegalArgumentException("No scope found for setting [" + setting.getKey() + "]");
        }
    }

    /**
     * Registers a settings filter pattern that allows to filter out certain settings that for instance contain sensitive information
     * or if a setting is for internal purposes only. The given pattern must either be a valid settings key or a simple regexp pattern.
     */
    public void registerSettingsFilter(String filter) {
        if (SettingsFilter.isValidPattern(filter) == false) {
            throw new IllegalArgumentException("filter [" + filter +"] is invalid must be either a key or a regex pattern");
        }
        if (settingsFilterPattern.contains(filter)) {
            throw new IllegalArgumentException("filter [" + filter + "] has already been registered");
        }
        settingsFilterPattern.add(filter);
    }

    /**
     * Check if a setting has already been registered
     */
    public boolean exists(Setting<?> setting) {
        if (setting.hasNodeScope()) {
            return nodeSettings.containsKey(setting.getKey());
        }
        if (setting.hasIndexScope()) {
            return indexSettings.containsKey(setting.getKey());
        }
        throw new IllegalArgumentException("setting scope is unknown. This should never happen!");
    }

    private void validateTribeSettings(Settings settings, ClusterSettings clusterSettings) {
        Map<String, Settings> groups = settings.filter(TRIBE_CLIENT_NODE_SETTINGS_PREDICATE).getGroups("tribe.", true);
        for (Map.Entry<String, Settings>  tribeSettings : groups.entrySet()) {
            Settings thisTribesSettings = tribeSettings.getValue();
            for (Map.Entry<String, String> entry : thisTribesSettings.getAsMap().entrySet()) {
                try {
                    clusterSettings.validate(entry.getKey(), thisTribesSettings);
                } catch (IllegalArgumentException ex) {
                    throw new IllegalArgumentException("tribe." + tribeSettings.getKey() +" validation failed: "+ ex.getMessage(), ex);
                }
            }
        }
    }
}
