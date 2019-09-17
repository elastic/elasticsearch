/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry.Entry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.transform.TransformNamedXContentProvider;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformsAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformsStatsAction;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.transform.action.TransportDeleteTransformAction;
import org.elasticsearch.xpack.transform.action.TransportGetTransformsAction;
import org.elasticsearch.xpack.transform.action.TransportGetTransformsStatsAction;
import org.elasticsearch.xpack.transform.action.TransportPreviewTransformAction;
import org.elasticsearch.xpack.transform.action.TransportPutTransformAction;
import org.elasticsearch.xpack.transform.action.TransportStartTransformAction;
import org.elasticsearch.xpack.transform.action.TransportStopTransformAction;
import org.elasticsearch.xpack.transform.action.TransportUpdateTransformAction;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.rest.action.RestDeleteTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestGetTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestGetTransformStatsAction;
import org.elasticsearch.xpack.transform.rest.action.RestPreviewTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestPutTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestStartTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestStopTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestUpdateTransformAction;
import org.elasticsearch.xpack.transform.transforms.TransformPersistentTasksExecutor;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.Collections.emptyList;

public class Transform extends Plugin implements ActionPlugin, PersistentTaskPlugin {

    public static final String NAME = "transform";
    public static final String TASK_THREAD_POOL_NAME = "transform_indexing";

    private static final Logger logger = LogManager.getLogger(Transform.class);

    private final boolean enabled;
    private final Settings settings;
    private final SetOnce<TransformConfigManager> transformsConfigManager = new SetOnce<>();
    private final SetOnce<TransformAuditor> transformAuditor = new SetOnce<>();
    private final SetOnce<TransformCheckpointService> transformCheckpointService = new SetOnce<>();
    private final SetOnce<SchedulerEngine> schedulerEngine = new SetOnce<>();

    public Transform(Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.TRANSFORM_ENABLED.get(settings);
    }

    protected XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    @Override
    public List<RestHandler> getRestHandlers(final Settings settings, final RestController restController,
            final ClusterSettings clusterSettings, final IndexScopedSettings indexScopedSettings, final SettingsFilter settingsFilter,
            final IndexNameExpressionResolver indexNameExpressionResolver, final Supplier<DiscoveryNodes> nodesInCluster) {

        if (!enabled) {
            return emptyList();
        }

        return Arrays.asList(
                new RestPutTransformAction(restController),
                new RestStartTransformAction(restController),
                new RestStopTransformAction(restController),
                new RestDeleteTransformAction(restController),
                new RestGetTransformAction(restController),
                new RestGetTransformStatsAction(restController),
                new RestPreviewTransformAction(restController),
                new RestUpdateTransformAction(restController)
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction = new ActionHandler<>(XPackUsageFeatureAction.TRANSFORM, TransformUsageTransportAction.class);
        var infoAction = new ActionHandler<>(XPackInfoFeatureAction.TRANSFORM, TransformInfoTransportAction.class);
        if (enabled == false) {
            return Arrays.asList(usageAction, infoAction);
        }

        return Arrays.asList(
                new ActionHandler<>(PutTransformAction.INSTANCE, TransportPutTransformAction.class),
                new ActionHandler<>(StartTransformAction.INSTANCE, TransportStartTransformAction.class),
                new ActionHandler<>(StopTransformAction.INSTANCE, TransportStopTransformAction.class),
                new ActionHandler<>(DeleteTransformAction.INSTANCE, TransportDeleteTransformAction.class),
                new ActionHandler<>(GetTransformsAction.INSTANCE, TransportGetTransformsAction.class),
                new ActionHandler<>(GetTransformsStatsAction.INSTANCE, TransportGetTransformsStatsAction.class),
                new ActionHandler<>(PreviewTransformAction.INSTANCE, TransportPreviewTransformAction.class),
                new ActionHandler<>(UpdateTransformAction.INSTANCE, TransportUpdateTransformAction.class),
                usageAction,
                infoAction);
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (false == enabled) {
            return emptyList();
        }

        FixedExecutorBuilder indexing = new FixedExecutorBuilder(settings, TASK_THREAD_POOL_NAME, 4, 4,
                "data_frame.task_thread_pool");

        return Collections.singletonList(indexing);
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
            Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        if (enabled == false) {
            return emptyList();
        }
        transformAuditor.set(new TransformAuditor(client, clusterService.getNodeName()));
        transformsConfigManager.set(new TransformConfigManager(client, xContentRegistry));
        transformCheckpointService.set(new TransformCheckpointService(client,
                                                                      transformsConfigManager.get(),
                                                                      transformAuditor.get()));

        return Arrays.asList(transformsConfigManager.get(), transformAuditor.get(), transformCheckpointService.get());
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return templates -> {
            try {
                templates.put(TransformInternalIndex.LATEST_INDEX_VERSIONED_NAME, TransformInternalIndex.getIndexTemplateMetaData());
            } catch (IOException e) {
                logger.error("Error creating data frame index template", e);
            }
            try {
                templates.put(TransformInternalIndex.AUDIT_INDEX, TransformInternalIndex.getAuditIndexTemplateMetaData());
            } catch (IOException e) {
                logger.warn("Error creating data frame audit index", e);
            }
            return templates;
        };
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService, ThreadPool threadPool,
            Client client, SettingsModule settingsModule) {
        if (enabled == false) {
            return emptyList();
        }

        schedulerEngine.set(new SchedulerEngine(settings, Clock.systemUTC()));

        // the transforms config manager should have been created
        assert transformsConfigManager.get() != null;
        // the auditor should have been created
        assert transformAuditor.get() != null;
        assert transformCheckpointService.get() != null;

        return Collections.singletonList(
            new TransformPersistentTasksExecutor(client,
                transformsConfigManager.get(),
                transformCheckpointService.get(),
                schedulerEngine.get(),
                transformAuditor.get(),
                threadPool,
                clusterService,
                settingsModule.getSettings()));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(TransformTask.NUM_FAILURE_RETRIES_SETTING);
    }

    @Override
    public void close() {
        if (schedulerEngine.get() != null) {
            schedulerEngine.get().stop();
        }
    }

    @Override
    public List<Entry> getNamedXContent() {
        return new TransformNamedXContentProvider().getNamedXContentParsers();
    }
}
