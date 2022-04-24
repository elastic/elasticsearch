/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.TemplateBundle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.template.IndexTemplateRegistry.parseComposableTemplates;

public class MonitoringTemplateBundle implements TemplateBundle {

    /**
     * The stack monitoring template registry version. This is the version id for templates used by Metricbeat in version 8.x. Metricbeat
     * writes monitoring data in ECS format as of 8.0. These templates define the ECS schema as well as alias fields for the old monitoring
     * mappings that point to the corresponding ECS fields.
     */
    public static final int STACK_MONITORING_REGISTRY_VERSION = Version.V_8_0_0.id + 2;
    public static final Setting<Boolean> MONITORING_TEMPLATES_ENABLED = Setting.boolSetting(
        "xpack.monitoring.templates.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private static final String STACK_MONITORING_REGISTRY_VERSION_VARIABLE = "xpack.stack.monitoring.template.release.version";
    private static final String STACK_TEMPLATE_VERSION = "8";
    private static final String STACK_TEMPLATE_VERSION_VARIABLE = "xpack.stack.monitoring.template.version";
    private static final Map<String, String> STACK_TEMPLATE_VARIABLES = Map.of(STACK_TEMPLATE_VERSION_VARIABLE, STACK_TEMPLATE_VERSION);

    //////////////////////////////////////////////////////////
    // Beats metricbeat template (for matching ".monitoring-beats-8-*" indices)
    //////////////////////////////////////////////////////////
    public static final String BEATS_STACK_INDEX_TEMPLATE_NAME = ".monitoring-beats-mb";
    public static final IndexTemplateConfig BEATS_STACK_INDEX_TEMPLATE = new IndexTemplateConfig(
        BEATS_STACK_INDEX_TEMPLATE_NAME,
        "/monitoring-beats-mb.json",
        STACK_MONITORING_REGISTRY_VERSION,
        STACK_MONITORING_REGISTRY_VERSION_VARIABLE,
        STACK_TEMPLATE_VARIABLES
    );

    //////////////////////////////////////////////////////////
    // ES metricbeat template (for matching ".monitoring-es-8-*" indices)
    //////////////////////////////////////////////////////////
    public static final String ES_STACK_INDEX_TEMPLATE_NAME = ".monitoring-es-mb";
    public static final IndexTemplateConfig ES_STACK_INDEX_TEMPLATE = new IndexTemplateConfig(
        ES_STACK_INDEX_TEMPLATE_NAME,
        "/monitoring-es-mb.json",
        STACK_MONITORING_REGISTRY_VERSION,
        STACK_MONITORING_REGISTRY_VERSION_VARIABLE,
        STACK_TEMPLATE_VARIABLES
    );

    //////////////////////////////////////////////////////////
    // Kibana metricbeat template (for matching ".monitoring-kibana-8-*" indices)
    //////////////////////////////////////////////////////////
    public static final String KIBANA_STACK_INDEX_TEMPLATE_NAME = ".monitoring-kibana-mb";
    public static final IndexTemplateConfig KIBANA_STACK_INDEX_TEMPLATE = new IndexTemplateConfig(
        KIBANA_STACK_INDEX_TEMPLATE_NAME,
        "/monitoring-kibana-mb.json",
        STACK_MONITORING_REGISTRY_VERSION,
        STACK_MONITORING_REGISTRY_VERSION_VARIABLE,
        STACK_TEMPLATE_VARIABLES
    );

    //////////////////////////////////////////////////////////
    // Logstash metricbeat template (for matching ".monitoring-logstash-8-*" indices)
    //////////////////////////////////////////////////////////
    public static final String LOGSTASH_STACK_INDEX_TEMPLATE_NAME = ".monitoring-logstash-mb";
    public static final IndexTemplateConfig LOGSTASH_STACK_INDEX_TEMPLATE = new IndexTemplateConfig(
        LOGSTASH_STACK_INDEX_TEMPLATE_NAME,
        "/monitoring-logstash-mb.json",
        STACK_MONITORING_REGISTRY_VERSION,
        STACK_MONITORING_REGISTRY_VERSION_VARIABLE,
        STACK_TEMPLATE_VARIABLES
    );

    //////////////////////////////////////////////////////////
    // Enterprise Search metricbeat template (for matching ".monitoring-ent-search-8-*" indices)
    //////////////////////////////////////////////////////////
    public static final String ENTERPRISE_SEARCH_STACK_INDEX_TEMPLATE_NAME = ".monitoring-ent-search-mb";
    public static final IndexTemplateConfig ENTERPRISE_SEARCH_STACK_INDEX_TEMPLATE = new IndexTemplateConfig(
        ENTERPRISE_SEARCH_STACK_INDEX_TEMPLATE_NAME,
        "/monitoring-ent-search-mb.json",
        STACK_MONITORING_REGISTRY_VERSION,
        STACK_MONITORING_REGISTRY_VERSION_VARIABLE,
        STACK_TEMPLATE_VARIABLES
    );

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        BEATS_STACK_INDEX_TEMPLATE,
        ES_STACK_INDEX_TEMPLATE,
        KIBANA_STACK_INDEX_TEMPLATE,
        LOGSTASH_STACK_INDEX_TEMPLATE,
        ENTERPRISE_SEARCH_STACK_INDEX_TEMPLATE
    );

    private volatile boolean monitoringTemplatesEnabled;

    public MonitoringTemplateBundle(Monitoring monitoring) {
        monitoringTemplatesEnabled = MONITORING_TEMPLATES_ENABLED.get(monitoring.settings);
    }

    @Override
    public Map<String, ComposableIndexTemplate> getComposableIndexTemplates() {
        return monitoringTemplatesEnabled ? COMPOSABLE_INDEX_TEMPLATE_CONFIGS : Map.of();
    }

    @Override
    public String getName() {
        return ClientHelper.MONITORING_ORIGIN;
    }

    @Override
    public void registerEnabledSettingHandler(BiConsumer<Setting<Boolean>, Consumer<Boolean>> consumer) {
        consumer.accept(MONITORING_TEMPLATES_ENABLED, this::updateEnabledSetting);
    }

    private void updateEnabledSetting(boolean newValue) {
        if (newValue) {
            monitoringTemplatesEnabled = true;
        } else {
//            logger.info(
//                "monitoring templates [{}] will not be installed or reinstalled",
//                getLegacyTemplateConfigs().stream().map(IndexTemplateConfig::getTemplateName).collect(Collectors.joining(","))
//            );
            monitoringTemplatesEnabled = false;
        }
    }
}
