/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.injection.guice.Binder;
import org.elasticsearch.injection.guice.Module;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A module that binds the provided settings to the {@link Settings} interface.
 */
public class SettingsModule implements Module {
    private static final Logger logger = LogManager.getLogger(SettingsModule.class);

    private final Settings settings;
    private final Set<String> settingsFilterPattern = new HashSet<>();
    private final Map<String, Setting<?>> nodeSettings = new HashMap<>();
    private final Map<String, Setting<?>> projectSettings = new HashMap<>();
    private final Map<String, Setting<?>> indexSettings = new HashMap<>();
    private final Set<Setting<?>> consistentSettings = new HashSet<>();
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final ProjectScopedSettings projectScopedSettings;
    private final SettingsFilter settingsFilter;

    public SettingsModule(Settings settings, Setting<?>... additionalSettings) {
        this(settings, Arrays.asList(additionalSettings), Collections.emptyList());
    }

    public SettingsModule(Settings settings, List<Setting<?>> additionalSettings, List<String> settingsFilter) {
        this(
            settings,
            additionalSettings,
            settingsFilter,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
        );
    }

    SettingsModule(
        final Settings settings,
        final List<Setting<?>> additionalSettings,
        final List<String> settingsFilter,
        final Set<Setting<?>> registeredClusterSettings,
        final Set<Setting<?>> registeredIndexSettings
    ) {
        this.settings = settings;
        for (Setting<?> setting : registeredClusterSettings) {
            registerSetting(setting);
        }
        for (Setting<?> setting : registeredIndexSettings) {
            registerSetting(setting);
        }

        for (Setting<?> setting : additionalSettings) {
            registerSetting(setting);
        }
        for (String filter : settingsFilter) {
            registerSettingsFilter(filter);
        }
        this.indexScopedSettings = new IndexScopedSettings(settings, new HashSet<>(this.indexSettings.values()));
        this.clusterSettings = new ClusterSettings(settings, new HashSet<>(this.nodeSettings.values()));
        this.projectScopedSettings = new ProjectScopedSettings(settings, new HashSet<>(this.projectSettings.values()));
        Settings indexSettings = settings.filter((s) -> s.startsWith("index.") && clusterSettings.get(s) == null);
        if (indexSettings.isEmpty() == false) {
            try {
                String separator = IntStream.range(0, 85).mapToObj(s -> "*").collect(Collectors.joining("")).trim();
                StringBuilder builder = new StringBuilder();
                builder.append(System.lineSeparator());
                builder.append(separator);
                builder.append(System.lineSeparator());
                builder.append("Found index level settings on node level configuration.");
                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                int count = 0;
                for (String word : ("Since elasticsearch 5.x index level settings can NOT be set on the nodes configuration like "
                    + "the elasticsearch.yaml, in system properties or command line arguments."
                    + "In order to upgrade all indices the settings must be updated via the /${index}/_settings API. "
                    + "Unless all settings are dynamic all indices must be closed in order to apply the upgrade"
                    + "Indices created in the future should use index templates to set default values.").split(" ")) {
                    if (count + word.length() > 85) {
                        builder.append(System.lineSeparator());
                        count = 0;
                    }
                    count += word.length() + 1;
                    builder.append(word).append(" ");
                }

                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                builder.append("Please ensure all required values are updated on all indices by executing: ");
                builder.append(System.lineSeparator());
                builder.append(System.lineSeparator());
                builder.append("curl -XPUT 'http://localhost:9200/_all/_settings?preserve_existing=true' -d '");
                try (XContentBuilder xContentBuilder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    xContentBuilder.prettyPrint();
                    xContentBuilder.startObject();
                    indexSettings.toXContent(xContentBuilder, Settings.FLAT_SETTINGS_TRUE);
                    xContentBuilder.endObject();
                    builder.append(Strings.toString(xContentBuilder));
                }
                builder.append("'");
                builder.append(System.lineSeparator());
                builder.append(separator);
                builder.append(System.lineSeparator());

                logger.warn(builder.toString());
                throw new IllegalArgumentException("node settings must not contain any index level settings");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // by now we are fully configured, lets check node level settings for unregistered index settings
        clusterSettings.validate(settings, true);
        this.settingsFilter = new SettingsFilter(settingsFilterPattern);
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(Settings.class).toInstance(settings);
        binder.bind(SettingsFilter.class).toInstance(settingsFilter);
        binder.bind(ClusterSettings.class).toInstance(clusterSettings);
        binder.bind(IndexScopedSettings.class).toInstance(indexScopedSettings);
        binder.bind(ProjectScopedSettings.class).toInstance(projectScopedSettings);
    }

    /**
     * Registers a new setting. This method should be used by plugins in order to expose any custom settings the plugin defines.
     * Unless a setting is registered the setting is unusable. If a setting is never the less specified the node will reject
     * the setting during startup.
     */
    private void registerSetting(Setting<?> setting) {
        if (setting.getKey().contains(".") == false && isS3InsecureCredentials(setting) == false) {
            throw new IllegalArgumentException("setting [" + setting.getKey() + "] is not in any namespace, its name must contain a dot");
        }
        if (setting.isFiltered()) {
            if (settingsFilterPattern.contains(setting.getKey()) == false) {
                registerSettingsFilter(setting.getKey());
            }
        }
        if (setting.hasNodeScope() || setting.hasIndexScope()) {
            if (setting.hasNodeScope()) {
                Setting<?> existingSetting = nodeSettings.get(setting.getKey());
                if (existingSetting != null) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                if (setting.isConsistent()) {
                    if (setting instanceof Setting.AffixSetting<?>) {
                        if (((Setting.AffixSetting<?>) setting).getConcreteSettingForNamespace("_na_") instanceof SecureSetting<?>) {
                            consistentSettings.add(setting);
                        } else {
                            throw new IllegalArgumentException("Invalid consistent secure setting [" + setting.getKey() + "]");
                        }
                    } else if (setting instanceof SecureSetting<?>) {
                        consistentSettings.add(setting);
                    } else {
                        throw new IllegalArgumentException("Invalid consistent secure setting [" + setting.getKey() + "]");
                    }
                }
                nodeSettings.put(setting.getKey(), setting);

                if (setting.getProperties().contains(Setting.Property.ProjectScope)) {
                    projectSettings.put(setting.getKey(), setting);
                }
            }
            if (setting.hasIndexScope()) {
                if (setting.getProperties().contains(Setting.Property.ProjectScope)) {
                    throw new IllegalStateException("setting [" + setting.getKey() + "] cannot be both project and index scoped");
                }
                Setting<?> existingSetting = indexSettings.get(setting.getKey());
                if (existingSetting != null) {
                    throw new IllegalArgumentException("Cannot register setting [" + setting.getKey() + "] twice");
                }
                if (setting.isConsistent()) {
                    throw new IllegalStateException("Consistent setting [" + setting.getKey() + "] cannot be index scoped");
                }
                indexSettings.put(setting.getKey(), setting);
            }
        } else {
            throw new IllegalArgumentException("No scope found for setting [" + setting.getKey() + "]");
        }
    }

    // TODO: remove this hack once we remove the deprecated ability to use repository settings in the cluster state in the S3 snapshot
    // module
    private static boolean isS3InsecureCredentials(Setting<?> setting) {
        final String settingKey = setting.getKey();
        return settingKey.equals("access_key") || settingKey.equals("secret_key");
    }

    /**
     * Registers a settings filter pattern that allows to filter out certain settings that for instance contain sensitive information
     * or if a setting is for internal purposes only. The given pattern must either be a valid settings key or a simple regexp pattern.
     */
    private void registerSettingsFilter(String filter) {
        if (SettingsFilter.isValidPattern(filter) == false) {
            throw new IllegalArgumentException("filter [" + filter + "] is invalid must be either a key or a regex pattern");
        }
        if (settingsFilterPattern.contains(filter)) {
            throw new IllegalArgumentException("filter [" + filter + "] has already been registered");
        }
        settingsFilterPattern.add(filter);
    }

    public Settings getSettings() {
        return settings;
    }

    public IndexScopedSettings getIndexScopedSettings() {
        return indexScopedSettings;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public ProjectScopedSettings getProjectScopedSettings() {
        return projectScopedSettings;
    }

    public Set<Setting<?>> getConsistentSettings() {
        return consistentSettings;
    }

    public SettingsFilter getSettingsFilter() {
        return settingsFilter;
    }

}
