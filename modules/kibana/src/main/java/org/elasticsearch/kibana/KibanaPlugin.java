/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;

import java.util.Collection;
import java.util.List;

public class KibanaPlugin extends Plugin implements SystemIndexPlugin {

    private static final List<String> KIBANA_PRODUCT_ORIGIN = List.of("kibana");

    private static final String KIBANA_WORKFLOWS_ORIGIN = "kibana";

    private static final String KIBANA_WORKFLOWS_VERSION_VARIABLE = "kibana.workflows.version";

    private static final int WORKFLOWS_EVENTS_MAPPINGS_VERSION = 2;

    private static final int WORKFLOWS_EXECUTION_LOGS_MAPPINGS_VERSION = 1;

    /** Log data stream registered in {@link #workflowsEventsSystemDataStreamDescriptor()}. */
    public static final String WORKFLOWS_EVENTS_DATA_STREAM_NAME = ".workflows-events*";

    /** Composable index template resource for {@value #WORKFLOWS_EVENTS_DATA_STREAM_NAME}. */
    public static final String WORKFLOWS_EVENTS_COMPOSABLE_TEMPLATE_RESOURCE = "workflows-events.json";

    /** Substitution key for managed index version in {@value #WORKFLOWS_EVENTS_COMPOSABLE_TEMPLATE_RESOURCE}. */
    public static final String WORKFLOWS_EVENTS_MANAGED_INDEX_VERSION_VARIABLE = "kibana.workflows.events.managed.index.version";

    /** Log data stream registered in {@link #workflowsExecutionDataStreamLogsSystemDataStreamDescriptor()}. */
    public static final String WORKFLOWS_EXECUTION_LOGS_DATA_STREAM_NAME = ".workflows-execution-data-stream-logs*";

    /** Composable index template resource for {@value #WORKFLOWS_EXECUTION_LOGS_DATA_STREAM_NAME}. */
    public static final String WORKFLOWS_EXECUTION_LOGS_COMPOSABLE_TEMPLATE_RESOURCE = "workflows-execution-data-stream-logs.json";

    /** Substitution key for managed index version in {@value #WORKFLOWS_EXECUTION_LOGS_COMPOSABLE_TEMPLATE_RESOURCE}. */
    public static final String WORKFLOWS_EXECUTION_LOGS_MANAGED_INDEX_VERSION_VARIABLE =
        "kibana.workflows.execution.logs.managed.index.version";

    /**
     * Matches workflows-related system <strong>indices</strong> under {@code .workflows-}, but not the log
     * {@linkplain SystemDataStreamDescriptor system data streams} registered in {@link #getSystemDataStreamDescriptors()}
     * ({@value #WORKFLOWS_EVENTS_DATA_STREAM_NAME} and {@value #WORKFLOWS_EXECUTION_LOGS_DATA_STREAM_NAME}).
     * <p>
     * A plain {@code .workflows-*} pattern is invalid here: it matches those data stream names, and
     * {@link org.elasticsearch.indices.SystemIndices} forbids overlap between a {@link SystemIndexDescriptor} pattern and
     * a {@link SystemDataStreamDescriptor} (see {@code checkForOverlappingPatterns}). Uses the same complement style as Fleet's
     * {@code .fleet-actions~(-results*)}; see {@link SystemIndexDescriptor} for pattern syntax.
     */
    public static final String WORKFLOWS_SYSTEM_INDEX_PATTERN = ".workflows~(-events*|-execution-data-stream-logs*)";

    public static final SystemIndexDescriptor KIBANA_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".kibana_*")
        .setDescription("Kibana saved objects system index")
        .setAliasName(".kibana")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .setAllowsTemplates()
        .build();

    public static final SystemIndexDescriptor REPORTING_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".reporting-*")
        .setDescription("system index for reporting")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .build();

    public static final SystemIndexDescriptor ONECHAT_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".chat-*")
        .setDescription("Onechat system index")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .setAllowsTemplates()
        .build();

    public static final SystemIndexDescriptor APM_AGENT_CONFIG_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".apm-agent-configuration*")
        .setDescription("system index for APM agent configuration")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .build();

    public static final SystemIndexDescriptor APM_CUSTOM_LINK_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".apm-custom-link*")
        .setDescription("system index for APM custom links")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .build();

    public static final SystemIndexDescriptor WORKFLOWS_INDEX_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(".workflows-*")
        .setDescription("Workflows system index")
        .setType(Type.EXTERNAL_UNMANAGED)
        .setAllowedElasticProductOrigins(KIBANA_PRODUCT_ORIGIN)
        .setAllowsTemplates()
        .build();

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            KIBANA_INDEX_DESCRIPTOR,
            REPORTING_INDEX_DESCRIPTOR,
            ONECHAT_INDEX_DESCRIPTOR,
            WORKFLOWS_INDEX_DESCRIPTOR,
            APM_AGENT_CONFIG_INDEX_DESCRIPTOR,
            APM_CUSTOM_LINK_INDEX_DESCRIPTOR
        );
    }

    @Override
    public String getFeatureName() {
        return "kibana";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages Kibana configuration and reports";
    }
}
