/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.runtimefields;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

/**
 * The plugin class for all the runtime fields common functionality that requires large dependencies.
 * This plugin sets up the environment for the grok function to work in painless as part of the different
 * runtime fields contexts.
 */
public final class RuntimeFieldsCommonPlugin extends Plugin {

    static final Setting<TimeValue> GROK_WATCHDOG_INTERVAL = Setting.timeSetting(
        "runtime_fields.grok.watchdog.interval",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );
    static final Setting<TimeValue> GROK_WATCHDOG_MAX_EXECUTION_TIME = Setting.timeSetting(
        "runtime_fields.grok.watchdog.max_execution_time",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    private final NamedGroupExtractor.GrokHelper grokHelper;

    /**
     * Constructs a new RuntimeFieldsCommonPlugin with grok watchdog settings.
     * <p>
     * This constructor initializes the grok helper with configured watchdog interval and
     * maximum execution time settings to protect against long-running or infinite loops
     * in grok pattern matching.
     * </p>
     *
     * @param settings the node settings containing grok watchdog configuration
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Constructor is called automatically by Elasticsearch plugin system
     * Settings settings = Settings.builder()
     *     .put("runtime_fields.grok.watchdog.interval", "1s")
     *     .put("runtime_fields.grok.watchdog.max_execution_time", "1s")
     *     .build();
     * RuntimeFieldsCommonPlugin plugin = new RuntimeFieldsCommonPlugin(settings);
     * }</pre>
     */
    public RuntimeFieldsCommonPlugin(Settings settings) {
        grokHelper = new NamedGroupExtractor.GrokHelper(
            GROK_WATCHDOG_INTERVAL.get(settings),
            GROK_WATCHDOG_MAX_EXECUTION_TIME.get(settings)
        );
    }

    /**
     * Returns the list of settings provided by this plugin.
     * <p>
     * This method exposes the grok watchdog configuration settings that control
     * pattern matching timeouts and intervals.
     * </p>
     *
     * @return an immutable list containing the grok watchdog interval and max execution time settings
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Setting<?>> settings = plugin.getSettings();
     * // Returns: [grok watchdog interval, grok watchdog max execution time]
     * }</pre>
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(GROK_WATCHDOG_INTERVAL, GROK_WATCHDOG_MAX_EXECUTION_TIME);
    }

    /**
     * Creates and initializes plugin components for runtime fields functionality.
     * <p>
     * This method completes the initialization of the grok helper by providing it with
     * the thread pool needed for watchdog scheduling. The watchdog monitors grok pattern
     * matching operations to prevent runaway executions.
     * </p>
     *
     * @param services the plugin services providing access to the thread pool
     * @return an empty collection (this plugin creates internal components only)
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called automatically by Elasticsearch during plugin initialization
     * Collection<?> components = plugin.createComponents(pluginServices);
     * }</pre>
     */
    @Override
    public Collection<?> createComponents(PluginServices services) {
        grokHelper.finishInitializing(services.threadPool());
        return List.of();
    }

    /**
     * Returns the grok helper instance for creating grok-based named group extractors.
     * <p>
     * The grok helper provides functionality for compiling grok patterns and creating
     * extractors that can parse text and extract named capture groups.
     * </p>
     *
     * @return the grok helper instance
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * NamedGroupExtractor.GrokHelper helper = plugin.grokHelper();
     * NamedGroupExtractor extractor = helper.grok("%{WORD:name} %{INT:age}");
     * Map<String, ?> groups = extractor.extract("John 30");
     * // Returns: {name=John, age=30}
     * }</pre>
     */
    public NamedGroupExtractor.GrokHelper grokHelper() {
        return grokHelper;
    }
}
