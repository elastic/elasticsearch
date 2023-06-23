/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.monitoring.MonitoringField.HISTORY_DURATION;

/**
 * Template registry for monitoring templates. Templates are loaded and installed shortly after cluster startup.
 *
 * This template registry manages templates for two purposes:
 * 1) Internal Monitoring Collection (.monitoring-{product}-7-*)
 * 2) Stack Monitoring templates for bridging ECS format data to legacy monitoring data (.monitoring-{product}-8-*)
 */
public class MonitoringTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(MonitoringTemplateRegistry.class);

    /**
     * The monitoring template registry version. This version number is normally incremented each change starting at "1", but
     * the legacy monitoring templates used release version numbers within their version fields instead. Because of this, we
     * continue to use the release version number in this registry, even though this is not standard practice for template
     * registries.
     */
    public static final int REGISTRY_VERSION = Version.V_8_8_0.id;
    private static final String REGISTRY_VERSION_VARIABLE = "xpack.monitoring.template.release.version";

    /**
     * Current version of templates used in their name to differentiate from breaking changes (separate from product version).
     * This would have been used for {@link MonitoringTemplateRegistry#REGISTRY_VERSION}, but the legacy monitoring
     * template installation process used the release version of the last template change in the template version
     * field instead. We keep it around to substitute into the template names.
     */
    private static final String TEMPLATE_VERSION = "7";
    private static final String TEMPLATE_VERSION_VARIABLE = "xpack.monitoring.template.version";
    private static final Map<String, String> ADDITIONAL_TEMPLATE_VARIABLES = Map.of(TEMPLATE_VERSION_VARIABLE, TEMPLATE_VERSION);

    /**
     * The stack monitoring ILM policy information. The template variables for the ILM policy are generated when the
     * registry is created so that we can pick a default retention value that is sensitive to legacy monitoring settings.
     */
    public static final String MONITORING_POLICY_NAME = ".monitoring-8-ilm-policy";
    private static final String MONITORING_POLICY_NAME_VARIABLE = "xpack.stack.monitoring.policy.name";
    public static final String MONITORING_POLICY_DEFAULT_RETENTION = "3d";
    private static final String MONITORING_POLICY_RETENTION_VARIABLE = "xpack.stack.monitoring.history.duration";
    private static final String MONITORING_POLICY_RETENTION_REASON_VARIABLE = "xpack.stack.monitoring.history.duration.reason";

    /**
     * The stack monitoring template registry version. This is the version id for templates used by Metricbeat in version 8.x. Metricbeat
     * writes monitoring data in ECS format as of 8.0. These templates define the ECS schema as well as alias fields for the old monitoring
     * mappings that point to the corresponding ECS fields.
     */
    public static final int STACK_MONITORING_REGISTRY_VERSION = Version.V_8_0_0.id + 9;
    private static final String STACK_MONITORING_REGISTRY_VERSION_VARIABLE = "xpack.stack.monitoring.template.release.version";
    private static final String STACK_TEMPLATE_VERSION = "8";
    private static final String STACK_TEMPLATE_VERSION_VARIABLE = "xpack.stack.monitoring.template.version";
    private static final Map<String, String> STACK_TEMPLATE_VARIABLES = Map.of(
        STACK_TEMPLATE_VERSION_VARIABLE,
        STACK_TEMPLATE_VERSION,
        MONITORING_POLICY_NAME_VARIABLE,
        MONITORING_POLICY_NAME
    );

    public static final Setting<Boolean> MONITORING_TEMPLATES_ENABLED = Setting.boolSetting(
        "xpack.monitoring.templates.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ClusterService clusterService;
    private volatile boolean monitoringTemplatesEnabled;

    //////////////////////////////////////////////////////////
    // Alerts template (for matching the ".monitoring-alerts-${version}" index)
    //////////////////////////////////////////////////////////
    public static final String ALERTS_INDEX_TEMPLATE_NAME = ".monitoring-alerts-7";
    public static final IndexTemplateConfig ALERTS_INDEX_TEMPLATE = new IndexTemplateConfig(
        ALERTS_INDEX_TEMPLATE_NAME,
        "/monitoring-alerts-7.json",
        REGISTRY_VERSION,
        REGISTRY_VERSION_VARIABLE,
        ADDITIONAL_TEMPLATE_VARIABLES
    );

    //////////////////////////////////////////////////////////
    // Beats template (for matching ".monitoring-beats-${version}-*" indices)
    //////////////////////////////////////////////////////////
    public static final String BEATS_INDEX_TEMPLATE_NAME = ".monitoring-beats";
    public static final IndexTemplateConfig BEATS_INDEX_TEMPLATE = new IndexTemplateConfig(
        BEATS_INDEX_TEMPLATE_NAME,
        "/monitoring-beats.json",
        REGISTRY_VERSION,
        REGISTRY_VERSION_VARIABLE,
        ADDITIONAL_TEMPLATE_VARIABLES
    );

    //////////////////////////////////////////////////////////
    // ES template (for matching ".monitoring-es-${version}-*" indices)
    //////////////////////////////////////////////////////////
    public static final String ES_INDEX_TEMPLATE_NAME = ".monitoring-es";
    public static final IndexTemplateConfig ES_INDEX_TEMPLATE = new IndexTemplateConfig(
        ES_INDEX_TEMPLATE_NAME,
        "/monitoring-es.json",
        REGISTRY_VERSION,
        REGISTRY_VERSION_VARIABLE,
        ADDITIONAL_TEMPLATE_VARIABLES
    );

    //////////////////////////////////////////////////////////
    // Kibana template (for matching ".monitoring-kibana-${version}-*" indices)
    //////////////////////////////////////////////////////////
    public static final String KIBANA_INDEX_TEMPLATE_NAME = ".monitoring-kibana";
    public static final IndexTemplateConfig KIBANA_INDEX_TEMPLATE = new IndexTemplateConfig(
        KIBANA_INDEX_TEMPLATE_NAME,
        "/monitoring-kibana.json",
        REGISTRY_VERSION,
        REGISTRY_VERSION_VARIABLE,
        ADDITIONAL_TEMPLATE_VARIABLES
    );

    //////////////////////////////////////////////////////////
    // Logstash template (for matching ".monitoring-logstash-${version}-*" indices)
    //////////////////////////////////////////////////////////
    public static final String LOGSTASH_INDEX_TEMPLATE_NAME = ".monitoring-logstash";
    public static final IndexTemplateConfig LOGSTASH_INDEX_TEMPLATE = new IndexTemplateConfig(
        LOGSTASH_INDEX_TEMPLATE_NAME,
        "/monitoring-logstash.json",
        REGISTRY_VERSION,
        REGISTRY_VERSION_VARIABLE,
        ADDITIONAL_TEMPLATE_VARIABLES
    );

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

    public static final String[] TEMPLATE_NAMES = new String[] {
        ALERTS_INDEX_TEMPLATE_NAME,
        BEATS_INDEX_TEMPLATE_NAME,
        ES_INDEX_TEMPLATE_NAME,
        KIBANA_INDEX_TEMPLATE_NAME,
        LOGSTASH_INDEX_TEMPLATE_NAME };

    private static final Map<String, IndexTemplateConfig> MONITORED_SYSTEM_CONFIG_LOOKUP = new HashMap<>();
    static {
        MONITORED_SYSTEM_CONFIG_LOOKUP.put(MonitoredSystem.BEATS.getSystem(), BEATS_INDEX_TEMPLATE);
        MONITORED_SYSTEM_CONFIG_LOOKUP.put(MonitoredSystem.ES.getSystem(), ES_INDEX_TEMPLATE);
        MONITORED_SYSTEM_CONFIG_LOOKUP.put(MonitoredSystem.KIBANA.getSystem(), KIBANA_INDEX_TEMPLATE);
        MONITORED_SYSTEM_CONFIG_LOOKUP.put(MonitoredSystem.LOGSTASH.getSystem(), LOGSTASH_INDEX_TEMPLATE);
    }

    public static IndexTemplateConfig getTemplateConfigForMonitoredSystem(MonitoredSystem system) {
        return Optional.ofNullable(MONITORED_SYSTEM_CONFIG_LOOKUP.get(system.getSystem()))
            .orElseThrow(() -> new IllegalArgumentException("Invalid system [" + system + "]"));
    }

    private final List<LifecyclePolicy> ilmPolicies;

    public MonitoringTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        this.clusterService = clusterService;
        this.monitoringTemplatesEnabled = MONITORING_TEMPLATES_ENABLED.get(nodeSettings);
        this.ilmPolicies = loadPolicies(nodeSettings);
    }

    private List<LifecyclePolicy> loadPolicies(Settings nodeSettings) {
        Map<String, String> templateVars = new HashMap<>();
        if (HISTORY_DURATION.exists(nodeSettings)) {
            templateVars.put(MONITORING_POLICY_RETENTION_VARIABLE, HISTORY_DURATION.get(nodeSettings).getStringRep());
            templateVars.put(
                MONITORING_POLICY_RETENTION_REASON_VARIABLE,
                "the value of the [" + HISTORY_DURATION.getKey() + "] setting at node startup"
            );
        } else {
            templateVars.put(MONITORING_POLICY_RETENTION_VARIABLE, MONITORING_POLICY_DEFAULT_RETENTION);
            templateVars.put(MONITORING_POLICY_RETENTION_REASON_VARIABLE, "the monitoring plugin default");
        }
        LifecyclePolicy monitoringPolicy = new LifecyclePolicyConfig(MONITORING_POLICY_NAME, "/monitoring-mb-ilm-policy.json", templateVars)
            .load(LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY);
        return Collections.singletonList(monitoringPolicy);
    }

    @Override
    public void initialize() {
        super.initialize();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MONITORING_TEMPLATES_ENABLED, this::updateEnabledSetting);
    }

    private void updateEnabledSetting(boolean newValue) {
        if (newValue) {
            monitoringTemplatesEnabled = true;
        } else {
            logger.info(
                "monitoring templates [{}] will not be installed or reinstalled",
                getLegacyTemplateConfigs().stream().map(IndexTemplateConfig::getTemplateName).collect(Collectors.joining(","))
            );
            monitoringTemplatesEnabled = false;
        }
    }

    @Override
    protected List<IndexTemplateConfig> getLegacyTemplateConfigs() {
        if (monitoringTemplatesEnabled) {
            return Arrays.asList(
                ALERTS_INDEX_TEMPLATE,
                BEATS_INDEX_TEMPLATE,
                ES_INDEX_TEMPLATE,
                KIBANA_INDEX_TEMPLATE,
                LOGSTASH_INDEX_TEMPLATE
            );
        } else {
            return Collections.emptyList();
        }
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        BEATS_STACK_INDEX_TEMPLATE,
        ES_STACK_INDEX_TEMPLATE,
        KIBANA_STACK_INDEX_TEMPLATE,
        LOGSTASH_STACK_INDEX_TEMPLATE,
        ENTERPRISE_SEARCH_STACK_INDEX_TEMPLATE
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return monitoringTemplatesEnabled ? COMPOSABLE_INDEX_TEMPLATE_CONFIGS : Map.of();
    }

    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        if (monitoringTemplatesEnabled) {
            return ilmPolicies;
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.MONITORING_ORIGIN;
    }

    @Override
    protected boolean requiresMasterNode() {
        // Monitoring templates have historically been installed from the master node of the cluster only.
        // Other nodes use the existence of templates as a coordination barrier in some parts of the code.
        // Templates should only be installed from the master node while we await the deprecation and
        // removal of those features so as to avoid ordering issues with exporters.
        return true;
    }
}
