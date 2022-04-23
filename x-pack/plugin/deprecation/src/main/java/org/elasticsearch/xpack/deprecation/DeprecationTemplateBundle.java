/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.TemplateBundle;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.DEPRECATION_ORIGIN;
import static org.elasticsearch.xpack.core.template.IndexTemplateRegistry.parseComposableTemplates;

public class DeprecationTemplateBundle implements TemplateBundle {

    // history (please add a comment why you increased the version here)
    // version 1: initial
    public static final int INDEX_TEMPLATE_VERSION = 1;

    public static final String DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE = "xpack.deprecation.indexing.template.version";

    public static final String DEPRECATION_INDEXING_MAPPINGS_NAME = ".deprecation-indexing-mappings";
    public static final String DEPRECATION_INDEXING_SETTINGS_NAME = ".deprecation-indexing-settings";
    public static final String DEPRECATION_INDEXING_TEMPLATE_NAME = ".deprecation-indexing-template";

    private static final Map<String, ComponentTemplate> COMPONENT_TEMPLATE_CONFIGS;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                DEPRECATION_INDEXING_MAPPINGS_NAME,
                "/org/elasticsearch/xpack/deprecation/deprecation-indexing-mappings.json",
                INDEX_TEMPLATE_VERSION,
                DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                DEPRECATION_INDEXING_SETTINGS_NAME,
                "/org/elasticsearch/xpack/deprecation/deprecation-indexing-settings.json",
                INDEX_TEMPLATE_VERSION,
                DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
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

    @Override
    public Map<String, ComponentTemplate> getComponentTemplates() {
        return COMPONENT_TEMPLATE_CONFIGS;
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(
            DEPRECATION_INDEXING_TEMPLATE_NAME,
            "/org/elasticsearch/xpack/deprecation/deprecation-indexing-template.json",
            INDEX_TEMPLATE_VERSION,
            DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
        )
    );

    @Override
    public Map<String, ComposableIndexTemplate> getComposableIndexTemplates() {
        return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
    }

    @Override
    public String getName() {
        return DEPRECATION_ORIGIN;
    }
}
