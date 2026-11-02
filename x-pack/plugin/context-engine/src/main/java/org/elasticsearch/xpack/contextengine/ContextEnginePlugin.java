/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine;

import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse.ResetFeatureStateStatus;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.contextengine.events.action.GetSessionEventsAction;
import org.elasticsearch.xpack.contextengine.events.action.RestGetSessionEventsAction;
import org.elasticsearch.xpack.contextengine.events.action.RestSaveEventAction;
import org.elasticsearch.xpack.contextengine.events.action.SaveEventAction;
import org.elasticsearch.xpack.contextengine.events.action.TransportGetSessionEventsAction;
import org.elasticsearch.xpack.contextengine.events.action.TransportSaveEventAction;
import org.elasticsearch.xpack.contextengine.memory.action.GetSessionMemoryAction;
import org.elasticsearch.xpack.contextengine.memory.action.RememberAction;
import org.elasticsearch.xpack.contextengine.memory.action.RestGetSessionMemoryAction;
import org.elasticsearch.xpack.contextengine.memory.action.RestRememberAction;
import org.elasticsearch.xpack.contextengine.memory.action.RestSearchSessionMemoryAction;
import org.elasticsearch.xpack.contextengine.memory.action.SearchSessionMemoryAction;
import org.elasticsearch.xpack.contextengine.memory.action.TransportGetSessionMemoryAction;
import org.elasticsearch.xpack.contextengine.memory.action.TransportRememberAction;
import org.elasticsearch.xpack.contextengine.memory.action.TransportSearchSessionMemoryAction;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.CONTEXT_ENGINE_ORIGIN;

/**
 * Hosts the Context Engine system data streams and the REST and transport actions that operate on them.
 */
public class ContextEnginePlugin extends Plugin implements SystemIndexPlugin {

    public static final String FEATURE_NAME = "context_engine";

    public static final String SESSION_MEMORY_DATA_STREAM_NAME = ".context-engine-session-memory";
    public static final String SESSION_EVENTS_DATA_STREAM_NAME = ".context-engine-session-events";

    private static final String MAPPING_VERSION_VARIABLE = "xpack.contextengine.version";
    private static final String MAPPING_MANAGED_VERSION_VARIABLE = "xpack.contextengine.managed.index.version";
    private static final String INFERENCE_ID_VARIABLE = "xpack.contextengine.session_memory.inference_id";
    static final int SESSION_MEMORY_MAPPINGS_VERSION = 1;
    static final int SESSION_EVENTS_MAPPINGS_VERSION = 1;

    /**
     * The embedding endpoint used by the session memory {@code content.semantic} and {@code media} fields. It must support the
     * {@code embedding} task type; the default is the preconfigured multimodal Jina endpoint.
     */
    public static final Setting<String> SESSION_MEMORY_INFERENCE_ID = Setting.simpleString(
        "xpack.context_engine.session_memory.inference_id",
        ".jina-embeddings-v5-omni-small",
        Setting.Property.NodeScope
    );

    private final String sessionMemoryInferenceId;

    public ContextEnginePlugin(Settings settings) {
        this.sessionMemoryInferenceId = SESSION_MEMORY_INFERENCE_ID.get(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SESSION_MEMORY_INFERENCE_ID);
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(RememberAction.INSTANCE, TransportRememberAction.class),
            new ActionHandler(GetSessionMemoryAction.INSTANCE, TransportGetSessionMemoryAction.class),
            new ActionHandler(SearchSessionMemoryAction.INSTANCE, TransportSearchSessionMemoryAction.class),
            new ActionHandler(SaveEventAction.INSTANCE, TransportSaveEventAction.class),
            new ActionHandler(GetSessionEventsAction.INSTANCE, TransportGetSessionEventsAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(
            new RestRememberAction(),
            new RestGetSessionMemoryAction(),
            new RestSearchSessionMemoryAction(),
            new RestSaveEventAction(),
            new RestGetSessionEventsAction()
        );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of();
    }

    @Override
    public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
        return List.of(sessionMemoryDataStreamDescriptor(), sessionEventsDataStreamDescriptor());
    }

    private SystemDataStreamDescriptor sessionMemoryDataStreamDescriptor() {
        return systemDataStreamDescriptor(
            SESSION_MEMORY_DATA_STREAM_NAME,
            "Session memories the agent chose to keep",
            "/context-engine-session-memory.json",
            Map.of(
                MAPPING_MANAGED_VERSION_VARIABLE,
                Integer.toString(SESSION_MEMORY_MAPPINGS_VERSION),
                INFERENCE_ID_VARIABLE,
                sessionMemoryInferenceId
            )
        );
    }

    private static SystemDataStreamDescriptor sessionEventsDataStreamDescriptor() {
        return systemDataStreamDescriptor(
            SESSION_EVENTS_DATA_STREAM_NAME,
            "Raw transcripts of agent session rounds",
            "/context-engine-session-events.json",
            Map.of(MAPPING_MANAGED_VERSION_VARIABLE, Integer.toString(SESSION_EVENTS_MAPPINGS_VERSION))
        );
    }

    private static SystemDataStreamDescriptor systemDataStreamDescriptor(
        String dataStreamName,
        String description,
        String templateResource,
        Map<String, String> templateVariables
    ) {
        try {
            ComposableIndexTemplate composableIndexTemplate = TemplateUtils.loadTemplate(
                templateResource,
                Build.current().version(),
                MAPPING_VERSION_VARIABLE,
                templateVariables,
                false,
                ComposableIndexTemplate::parse
            );
            return new SystemDataStreamDescriptor(
                dataStreamName,
                description,
                SystemDataStreamDescriptor.Type.INTERNAL,
                composableIndexTemplate,
                Map.of(),
                List.of(),
                CONTEXT_ENGINE_ORIGIN,
                ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void cleanUpFeature(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        Client client,
        TimeValue masterNodeTimeout,
        ActionListener<ResetFeatureStateStatus> listener
    ) {
        DeleteDataStreamAction.Request request = new DeleteDataStreamAction.Request(
            masterNodeTimeout,
            getSystemDataStreamDescriptors().stream().map(SystemDataStreamDescriptor::getDataStreamName).toArray(String[]::new)
        );
        request.indicesOptions(
            IndicesOptions.builder(request.indicesOptions())
                .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
                .build()
        );
        client.execute(DeleteDataStreamAction.INSTANCE, request, ActionListener.wrap(response -> {
            SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, masterNodeTimeout, listener);
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, masterNodeTimeout, listener);
            } else {
                listener.onFailure(e);
            }
        }));
    }

    @Override
    public String getFeatureName() {
        return FEATURE_NAME;
    }

    @Override
    public String getFeatureDescription() {
        return "Manages Context Engine system data streams";
    }
}
