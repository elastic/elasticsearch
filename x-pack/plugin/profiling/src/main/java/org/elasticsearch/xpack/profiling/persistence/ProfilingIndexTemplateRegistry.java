/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.isDataStreamsLifecycleOnlyMode;

/**
 * Creates all index-templates and ILM policies that are required for using Elastic Universal Profiling.
 */
public class ProfilingIndexTemplateRegistry extends IndexTemplateRegistry {
    private static final Logger logger = LogManager.getLogger(ProfilingIndexTemplateRegistry.class);
    // history (please add a comment why you increased the version here)
    // version 1: initial
    // version 2: Added 'profiling.host.machine' keyword mapping to profiling-hosts
    // version 3: Add optional component template 'profiling-ilm@custom' to all ILM-managed index templates
    // version 4: Added 'service.name' keyword mapping to profiling-events
    // version 5: Add optional component template '<idx-name>@custom' to all index templates that reference component templates
    // version 6: Added 'host.arch' keyword mapping to profiling-hosts
    // version 7: Added 'host.type', 'cloud.provider', 'cloud.region' keyword mappings to profiling-hosts
    // version 8: Changed from disabled _source to synthetic _source for profiling-events-* and profiling-metrics
    // version 9: Changed sort order for profiling-events-*
    // version 10: changed mapping profiling-events @timestamp to 'date_nanos' from 'date'
    // version 11: Added 'profiling.agent.protocol' keyword mapping to profiling-hosts
    // version 12: Added 'profiling.agent.env_https_proxy' keyword mapping to profiling-hosts
    // version 13: Added 'container.id' keyword mapping to profiling-events
    // version 14: Stop using using _source.mode attribute in index templates
    // version 15: Use LogsDB mode for profiling-events-* (~30% smaller storage footprint)
    // version 16: Added 'profiling.executable.name' keyword mapping to profiling-events
    public static final int INDEX_TEMPLATE_VERSION = 15;

    // history for individual indices / index templates. Only bump these for breaking changes that require to create a new index
    public static final int PROFILING_EVENTS_VERSION = 6;
    public static final int PROFILING_EXECUTABLES_VERSION = 1;
    public static final int PROFILING_METRICS_VERSION = 2;
    public static final int PROFILING_HOSTS_VERSION = 2;
    public static final int PROFILING_STACKFRAMES_VERSION = 1;
    public static final int PROFILING_STACKTRACES_VERSION = 1;
    public static final int PROFILING_SYMBOLS_VERSION = 1;
    public static final int PROFILING_RETURNPADS_PRIVATE_VERSION = 1;
    public static final int PROFILING_SQ_EXECUTABLES_VERSION = 1;
    public static final int PROFILING_SQ_LEAFFRAMES_VERSION = 1;
    public static final String PROFILING_TEMPLATE_VERSION_VARIABLE = "xpack.profiling.template.version";

    private volatile boolean templatesEnabled;

    public ProfilingIndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ProjectResolver projectResolver
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, projectResolver);
    }

    public void setTemplatesEnabled(boolean templatesEnabled) {
        this.templatesEnabled = templatesEnabled;
    }

    public void close() {
        clusterService.removeListener(this);
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.PROFILING_ORIGIN;
    }

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    private static final List<LifecyclePolicyConfig> LIFECYCLE_POLICY_CONFIGS = List.of(
        new LifecyclePolicyConfig(
            "profiling-60-days",
            "/profiling/ilm-policy/profiling-60-days.json",
            Map.of(PROFILING_TEMPLATE_VERSION_VARIABLE, String.valueOf(INDEX_TEMPLATE_VERSION))
        )
    );

    @Override
    protected List<LifecyclePolicyConfig> getLifecycleConfigs() {
        return LIFECYCLE_POLICY_CONFIGS;
    }

    @Override
    protected List<LifecyclePolicy> getLifecyclePolicies() {
        return templatesEnabled ? lifecyclePolicies : Collections.emptyList();
    }

    private static final Map<String, ComponentTemplate> COMPONENT_TEMPLATE_CONFIGS;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                "profiling-events",
                "/profiling/component-template/profiling-events.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE,
                indexVersion("events", PROFILING_EVENTS_VERSION)
            ),
            new IndexTemplateConfig(
                "profiling-executables",
                "/profiling/component-template/profiling-executables.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE,
                indexVersion("executables", PROFILING_EXECUTABLES_VERSION)
            ),
            new IndexTemplateConfig(
                "profiling-ilm",
                "/profiling/component-template/profiling-ilm.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                "profiling-hot-tier",
                "/profiling/component-template/profiling-hot-tier.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                "profiling-metrics",
                "/profiling/component-template/profiling-metrics.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE,
                indexVersion("metrics", PROFILING_METRICS_VERSION)
            ),
            new IndexTemplateConfig(
                "profiling-hosts",
                "/profiling/component-template/profiling-hosts.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE,
                indexVersion("hosts", PROFILING_HOSTS_VERSION)
            ),
            new IndexTemplateConfig(
                "profiling-stackframes",
                "/profiling/component-template/profiling-stackframes.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE,
                indexVersion("stackframes", PROFILING_STACKFRAMES_VERSION)
            ),
            new IndexTemplateConfig(
                "profiling-stacktraces",
                "/profiling/component-template/profiling-stacktraces.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE,
                indexVersion("stacktraces", PROFILING_STACKTRACES_VERSION)
            ),
            new IndexTemplateConfig(
                "profiling-symbols",
                "/profiling/component-template/profiling-symbols.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE,
                indexVersion("symbols", PROFILING_SYMBOLS_VERSION)
            )
        )) {
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes())) {
                componentTemplates.put(config.getTemplateName(), ComponentTemplate.parse(parser));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        COMPONENT_TEMPLATE_CONFIGS = Collections.unmodifiableMap(componentTemplates);
    }

    private static Map<String, String> indexVersion(String index, int version) {
        return Map.of(String.format(Locale.ROOT, "xpack.profiling.index.%s.version", index), String.valueOf(version));
    }

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        return templatesEnabled ? COMPONENT_TEMPLATE_CONFIGS : Collections.emptyMap();
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(
            "profiling-events",
            "/profiling/index-template/profiling-events.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-metrics",
            "/profiling/index-template/profiling-metrics.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-hosts",
            "/profiling/index-template/profiling-hosts.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-executables",
            "/profiling/index-template/profiling-executables.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-stackframes",
            "/profiling/index-template/profiling-stackframes.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-stacktraces",
            "/profiling/index-template/profiling-stacktraces.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        // templates for regular indices
        new IndexTemplateConfig(
            "profiling-returnpads-private",
            "/profiling/index-template/profiling-returnpads-private.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE,
            indexVersion("returnpads.private", PROFILING_RETURNPADS_PRIVATE_VERSION)
        ),
        new IndexTemplateConfig(
            "profiling-sq-executables",
            "/profiling/index-template/profiling-sq-executables.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE,
            indexVersion("sq.executables", PROFILING_SQ_EXECUTABLES_VERSION)
        ),
        new IndexTemplateConfig(
            "profiling-sq-leafframes",
            "/profiling/index-template/profiling-sq-leafframes.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE,
            indexVersion("sq.leafframes", PROFILING_SQ_LEAFFRAMES_VERSION)
        ),
        new IndexTemplateConfig(
            "profiling-symbols-global",
            "/profiling/index-template/profiling-symbols-global.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-symbols-private",
            "/profiling/index-template/profiling-symbols-private.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        )
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return templatesEnabled ? COMPOSABLE_INDEX_TEMPLATE_CONFIGS : Collections.emptyMap();
    }

    @Override
    protected boolean isUpgradeRequired(LifecyclePolicy currentPolicy, LifecyclePolicy newPolicy) {
        try {
            return getVersion(currentPolicy, "current") < getVersion(newPolicy, "new");
        } catch (IllegalArgumentException ex) {
            logger.warn("Cannot determine whether lifecycle policy upgrade is required.", ex);
            // don't attempt an upgrade on invalid data
            return false;
        }
    }

    private static int getVersion(LifecyclePolicy policy, String logicalVersion) {
        Map<String, Object> meta = policy.getMetadata();
        try {
            return meta != null ? Integer.parseInt(meta.getOrDefault("version", Integer.MIN_VALUE).toString()) : Integer.MIN_VALUE;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Invalid version metadata for %s lifecycle policy [%s]", logicalVersion, policy.getName()),
                ex
            );
        }
    }

    /**
     * Determines whether all resources (component templates, composable templates and lifecycle policies) have been created. This method
     * also checks whether resources have been created for the expected version.
     *
     * @param state Current cluster state.
     * @param settings Current cluster settings.
     * @return <code>true</code> if and only if all resources managed by this registry have been created and are current.
     */
    public static boolean isAllResourcesCreated(ClusterState state, Settings settings) {
        for (String name : COMPONENT_TEMPLATE_CONFIGS.keySet()) {
            ComponentTemplate componentTemplate = state.metadata().getProject().componentTemplates().get(name);
            if (componentTemplate == null || componentTemplate.version() < INDEX_TEMPLATE_VERSION) {
                return false;
            }
        }
        for (String name : COMPOSABLE_INDEX_TEMPLATE_CONFIGS.keySet()) {
            ComposableIndexTemplate composableIndexTemplate = state.metadata().getProject().templatesV2().get(name);
            if (composableIndexTemplate == null || composableIndexTemplate.version() < INDEX_TEMPLATE_VERSION) {
                return false;
            }
        }
        if (isDataStreamsLifecycleOnlyMode(settings) == false) {
            IndexLifecycleMetadata ilmMetadata = state.metadata().getProject().custom(IndexLifecycleMetadata.TYPE);
            if (ilmMetadata == null) {
                return false;
            }
            for (LifecyclePolicyConfig lifecyclePolicy : LIFECYCLE_POLICY_CONFIGS) {
                LifecyclePolicy existingPolicy = ilmMetadata.getPolicies().get(lifecyclePolicy.getPolicyName());
                if (existingPolicy == null || getVersion(existingPolicy, "current") < INDEX_TEMPLATE_VERSION) {
                    return false;
                }
            }
        }
        return true;
    }
}
