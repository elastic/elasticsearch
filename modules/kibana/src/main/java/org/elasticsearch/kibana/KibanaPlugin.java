/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse.ResetFeatureStateStatus;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction.Request;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class KibanaPlugin extends Plugin implements SystemIndexPlugin {

    private static final List<String> KIBANA_PRODUCT_ORIGIN = List.of("kibana");

    private static final String KIBANA_WORKFLOWS_ORIGIN = "kibana";

    private static final String KIBANA_WORKFLOWS_VERSION_VARIABLE = "kibana.workflows.version";

    private static final int WORKFLOWS_EVENTS_MAPPINGS_VERSION = 2;

    private static final int WORKFLOWS_EXECUTION_LOGS_MAPPINGS_VERSION = 1;

    /** Log data stream registered in {@link #workflowsEventsSystemDataStreamDescriptor()}. */
    public static final String WORKFLOWS_EVENTS_DATA_STREAM_NAME = ".workflows-events";

    /** Log data stream registered in {@link #workflowsExecutionDataStreamLogsSystemDataStreamDescriptor()}. */
    public static final String WORKFLOWS_EXECUTION_LOGS_DATA_STREAM_NAME = ".workflows-execution-data-stream-logs";

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
        .setIndexPattern(WORKFLOWS_SYSTEM_INDEX_PATTERN)
        .setDescription("Workflows system indices")
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
    public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
        return List.of(workflowsEventsSystemDataStreamDescriptor(), workflowsExecutionDataStreamLogsSystemDataStreamDescriptor());
    }

    private static SystemDataStreamDescriptor workflowsEventsSystemDataStreamDescriptor() {
        try {
            ComposableIndexTemplate composableIndexTemplate = loadWorkflowsComposableTemplate(
                "/workflows-events.json",
                Map.of("kibana.workflows.events.managed.index.version", Integer.toString(WORKFLOWS_EVENTS_MAPPINGS_VERSION))
            );
            return new SystemDataStreamDescriptor(
                WORKFLOWS_EVENTS_DATA_STREAM_NAME,
                "Workflows execution events",
                SystemDataStreamDescriptor.Type.EXTERNAL,
                composableIndexTemplate,
                Map.of(),
                KIBANA_PRODUCT_ORIGIN,
                KIBANA_WORKFLOWS_ORIGIN,
                ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static SystemDataStreamDescriptor workflowsExecutionDataStreamLogsSystemDataStreamDescriptor() {
        try {
            ComposableIndexTemplate composableIndexTemplate = loadWorkflowsComposableTemplate(
                "/workflows-execution-data-stream-logs.json",
                Map.of(
                    "kibana.workflows.execution.logs.managed.index.version",
                    Integer.toString(WORKFLOWS_EXECUTION_LOGS_MAPPINGS_VERSION)
                )
            );
            return new SystemDataStreamDescriptor(
                WORKFLOWS_EXECUTION_LOGS_DATA_STREAM_NAME,
                "Workflows execution logs",
                SystemDataStreamDescriptor.Type.EXTERNAL,
                composableIndexTemplate,
                Map.of(),
                KIBANA_PRODUCT_ORIGIN,
                KIBANA_WORKFLOWS_ORIGIN,
                ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ComposableIndexTemplate loadWorkflowsComposableTemplate(String resource, Map<String, String> variables)
        throws IOException {
        return TemplateUtils.loadTemplate(
            resource,
            Version.CURRENT.toString(),
            KIBANA_WORKFLOWS_VERSION_VARIABLE,
            variables,
            false,
            ComposableIndexTemplate::parse
        );
    }

    @Override
    public void cleanUpFeature(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Client client,
        TimeValue masterNodeTimeout,
        ActionListener<ResetFeatureStateStatus> listener
    ) {
        Collection<SystemDataStreamDescriptor> dataStreamDescriptors = getSystemDataStreamDescriptors();
        if (dataStreamDescriptors.isEmpty() == false) {
            try {
                Request request = new Request(
                    TimeValue.THIRTY_SECONDS,
                    dataStreamDescriptors.stream().map(SystemDataStreamDescriptor::getDataStreamName).toArray(String[]::new)
                );
                request.indicesOptions(
                    IndicesOptions.builder(request.indicesOptions())
                        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
                        .build()
                );

                client.execute(
                    DeleteDataStreamAction.INSTANCE,
                    request,
                    ActionListener.wrap(
                        response -> SystemIndexPlugin.super.cleanUpFeature(
                            clusterService,
                            projectResolver,
                            client,
                            masterNodeTimeout,
                            listener
                        ),
                        e -> {
                            Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                            if (unwrapped instanceof ResourceNotFoundException) {
                                SystemIndexPlugin.super.cleanUpFeature(
                                    clusterService,
                                    projectResolver,
                                    client,
                                    masterNodeTimeout,
                                    listener
                                );
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    )
                );
            } catch (Exception e) {
                Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                if (unwrapped instanceof ResourceNotFoundException) {
                    SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, masterNodeTimeout, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        } else {
            SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, masterNodeTimeout, listener);
        }
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
