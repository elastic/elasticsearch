/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.TemplateBundle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.template.IndexTemplateRegistry.parseComposableTemplates;

public class StackTemplateBundle implements TemplateBundle {

    public static final Setting<Boolean> STACK_TEMPLATES_ENABLED = Setting.boolSetting(
        "stack.templates.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // The stack template registry version. This number must be incremented when we make changes
    // to built-in templates.
    public static final int REGISTRY_VERSION = 1;
    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.stack.template.version";

    //////////////////////////////////////////////////////////
    // Logs components (for matching logs-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "logs-mappings";
    public static final String LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME = "logs-settings";
    public static final String LOGS_INDEX_TEMPLATE_NAME = "logs";

    //////////////////////////////////////////////////////////
    // Metrics components (for matching metric-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "metrics-mappings";
    public static final String METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "metrics-settings";
    public static final String METRICS_INDEX_TEMPLATE_NAME = "metrics";

    //////////////////////////////////////////////////////////
    // Synthetics components (for matching synthetics-*-* indices)
    //////////////////////////////////////////////////////////
    public static final String SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "synthetics-mappings";
    public static final String SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME = "synthetics-settings";
    public static final String SYNTHETICS_INDEX_TEMPLATE_NAME = "synthetics";

    // General mappings conventions for any data that ends up in a data stream
    public static final String DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME = "data-streams-mappings";

    private static final Map<String, ComponentTemplate> COMPONENT_TEMPLATE_CONFIGS;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/data-streams-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/logs-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/logs-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/metrics-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/metrics-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
                "/synthetics-mappings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                "/synthetics-settings.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            )
        )) {
            try {
                componentTemplates.put(
                    config.getTemplateName(),
                    ComponentTemplate.parse(JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes()))
                );
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        COMPONENT_TEMPLATE_CONFIGS = Map.copyOf(componentTemplates);
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(LOGS_INDEX_TEMPLATE_NAME, "/logs-template.json", REGISTRY_VERSION, TEMPLATE_VERSION_VARIABLE),
        new IndexTemplateConfig(METRICS_INDEX_TEMPLATE_NAME, "/metrics-template.json", REGISTRY_VERSION, TEMPLATE_VERSION_VARIABLE),
        new IndexTemplateConfig(SYNTHETICS_INDEX_TEMPLATE_NAME, "/synthetics-template.json", REGISTRY_VERSION, TEMPLATE_VERSION_VARIABLE)
    );

    // TODO: make dynamic
    private final boolean stackTemplateEnabled;

    public StackTemplateBundle(StackPlugin plugin) {
        this.stackTemplateEnabled = STACK_TEMPLATES_ENABLED.get(plugin.settings);
    }

    @Override
    public Map<String, ComponentTemplate> getComponentTemplates() {
        return stackTemplateEnabled ? COMPONENT_TEMPLATE_CONFIGS : Map.of();
    }

    @Override
    public Map<String, ComposableIndexTemplate> getComposableIndexTemplates() {
        return stackTemplateEnabled ? COMPOSABLE_INDEX_TEMPLATE_CONFIGS : Map.of();
    }

    @Override
    public String getName() {
        return ClientHelper.STACK_ORIGIN;
    }

}
