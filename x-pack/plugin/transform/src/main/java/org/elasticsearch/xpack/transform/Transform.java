/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.AssociatedIndexDescriptor;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.NamedXContentRegistry.Entry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.SetResetModeActionRequest;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.TransformNamedXContentProvider;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.ResetTransformAction;
import org.elasticsearch.xpack.core.transform.action.ScheduleNowTransformAction;
import org.elasticsearch.xpack.core.transform.action.SetResetModeAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpgradeTransformsAction;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.transform.action.TransportDeleteTransformAction;
import org.elasticsearch.xpack.transform.action.TransportGetCheckpointAction;
import org.elasticsearch.xpack.transform.action.TransportGetCheckpointNodeAction;
import org.elasticsearch.xpack.transform.action.TransportGetTransformAction;
import org.elasticsearch.xpack.transform.action.TransportGetTransformStatsAction;
import org.elasticsearch.xpack.transform.action.TransportPreviewTransformAction;
import org.elasticsearch.xpack.transform.action.TransportPutTransformAction;
import org.elasticsearch.xpack.transform.action.TransportResetTransformAction;
import org.elasticsearch.xpack.transform.action.TransportScheduleNowTransformAction;
import org.elasticsearch.xpack.transform.action.TransportSetTransformResetModeAction;
import org.elasticsearch.xpack.transform.action.TransportStartTransformAction;
import org.elasticsearch.xpack.transform.action.TransportStopTransformAction;
import org.elasticsearch.xpack.transform.action.TransportUpdateTransformAction;
import org.elasticsearch.xpack.transform.action.TransportUpgradeTransformsAction;
import org.elasticsearch.xpack.transform.action.TransportValidateTransformAction;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;
import org.elasticsearch.xpack.transform.rest.action.RestCatTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestDeleteTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestGetTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestGetTransformStatsAction;
import org.elasticsearch.xpack.transform.rest.action.RestPreviewTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestPutTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestResetTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestScheduleNowTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestStartTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestStopTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestUpdateTransformAction;
import org.elasticsearch.xpack.transform.rest.action.RestUpgradeTransformsAction;
import org.elasticsearch.xpack.transform.transforms.TransformPersistentTasksExecutor;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.transform.TransformMessages.FAILED_TO_UNSET_RESET_MODE;
import static org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants.AUDIT_INDEX_PATTERN;
import static org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants.TRANSFORM_PREFIX;
import static org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants.TRANSFORM_PREFIX_DEPRECATED;

public class Transform extends Plugin implements SystemIndexPlugin, PersistentTaskPlugin {

    public static final String NAME = "transform";

    private static final Logger logger = LogManager.getLogger(Transform.class);

    private final Settings settings;
    private final SetOnce<TransformServices> transformServices = new SetOnce<>();

    public static final Integer DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE = Integer.valueOf(500);
    public static final TimeValue DEFAULT_TRANSFORM_FREQUENCY = TimeValue.timeValueSeconds(60);

    public static final int DEFAULT_FAILURE_RETRIES = 10;
    // How many times the transform task can retry on a non-critical failure.
    // This cluster-level setting is deprecated, the users should be using transform-level setting instead.
    // In order to ensure BWC, this cluster-level setting serves as a fallback in case the transform-level setting is not specified.
    public static final Setting<Integer> NUM_FAILURE_RETRIES_SETTING = Setting.intSetting(
        "xpack.transform.num_transform_failure_retries",
        DEFAULT_FAILURE_RETRIES,
        0,
        SettingsConfig.MAX_NUM_FAILURE_RETRIES,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final TimeValue DEFAULT_SCHEDULER_FREQUENCY = TimeValue.timeValueSeconds(1);
    // How often does the transform scheduler process the tasks
    public static final Setting<TimeValue> SCHEDULER_FREQUENCY = Setting.timeSetting(
        "xpack.transform.transform_scheduler_frequency",
        DEFAULT_SCHEDULER_FREQUENCY,
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope
    );

    public Transform(Settings settings) {
        this.settings = settings;
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public List<RestHandler> getRestHandlers(
        final Settings unused,
        final RestController restController,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster
    ) {

        return Arrays.asList(
            new RestPutTransformAction(),
            new RestStartTransformAction(),
            new RestStopTransformAction(),
            new RestDeleteTransformAction(),
            new RestGetTransformAction(),
            new RestGetTransformStatsAction(),
            new RestPreviewTransformAction(),
            new RestUpdateTransformAction(),
            new RestCatTransformAction(),
            new RestUpgradeTransformsAction(),
            new RestResetTransformAction(),
            new RestScheduleNowTransformAction()
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {

        return Arrays.asList(
            new ActionHandler<>(PutTransformAction.INSTANCE, TransportPutTransformAction.class),
            new ActionHandler<>(StartTransformAction.INSTANCE, TransportStartTransformAction.class),
            new ActionHandler<>(StopTransformAction.INSTANCE, TransportStopTransformAction.class),
            new ActionHandler<>(DeleteTransformAction.INSTANCE, TransportDeleteTransformAction.class),
            new ActionHandler<>(GetTransformAction.INSTANCE, TransportGetTransformAction.class),
            new ActionHandler<>(GetTransformStatsAction.INSTANCE, TransportGetTransformStatsAction.class),
            new ActionHandler<>(PreviewTransformAction.INSTANCE, TransportPreviewTransformAction.class),
            new ActionHandler<>(UpdateTransformAction.INSTANCE, TransportUpdateTransformAction.class),
            new ActionHandler<>(SetResetModeAction.INSTANCE, TransportSetTransformResetModeAction.class),
            new ActionHandler<>(UpgradeTransformsAction.INSTANCE, TransportUpgradeTransformsAction.class),
            new ActionHandler<>(ResetTransformAction.INSTANCE, TransportResetTransformAction.class),
            new ActionHandler<>(ScheduleNowTransformAction.INSTANCE, TransportScheduleNowTransformAction.class),

            // internal, no rest endpoint
            new ActionHandler<>(ValidateTransformAction.INSTANCE, TransportValidateTransformAction.class),
            new ActionHandler<>(GetCheckpointAction.INSTANCE, TransportGetCheckpointAction.class),
            new ActionHandler<>(GetCheckpointNodeAction.INSTANCE, TransportGetCheckpointNodeAction.class),

            // usage and info
            new ActionHandler<>(XPackUsageFeatureAction.TRANSFORM, TransformUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.TRANSFORM, TransformInfoTransportAction.class)
        );
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        TransformConfigManager configManager = new IndexBasedTransformConfigManager(
            clusterService,
            expressionResolver,
            client,
            xContentRegistry
        );
        TransformAuditor auditor = new TransformAuditor(client, clusterService.getNodeName(), clusterService, includeNodeInfo());
        Clock clock = Clock.systemUTC();
        TransformCheckpointService checkpointService = new TransformCheckpointService(
            clock,
            settings,
            clusterService,
            configManager,
            auditor
        );
        TransformScheduler scheduler = new TransformScheduler(clock, threadPool, settings);
        scheduler.start();

        transformServices.set(new TransformServices(configManager, checkpointService, auditor, scheduler));

        return Arrays.asList(transformServices.get(), new TransformClusterStateListener(clusterService, client));
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        // the transform services should have been created
        assert transformServices.get() != null;

        return List.of(
            new TransformPersistentTasksExecutor(
                client,
                transformServices.get(),
                threadPool,
                clusterService,
                settingsModule.getSettings(),
                getTransformInternalIndexAdditionalSettings(),
                expressionResolver
            )
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(NUM_FAILURE_RETRIES_SETTING, SCHEDULER_FREQUENCY);
    }

    @Override
    public void close() {
        if (transformServices.get() != null) {
            transformServices.get().getScheduler().stop();
        }
    }

    @Override
    public List<Entry> getNamedXContent() {
        return new TransformNamedXContentProvider().getNamedXContentParsers();
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetadata>> getIndexTemplateMetadataUpgrader() {
        return templates -> {
            // These are all legacy templates that were created in old versions. None are needed now.
            // The "internal" indices became system indices and the "notifications" indices now use composable templates.
            templates.remove(".data-frame-internal-1");
            templates.remove(".data-frame-internal-2");
            templates.remove(".transform-internal-003");
            templates.remove(".transform-internal-004");
            templates.remove(".transform-internal-005");
            templates.remove(".data-frame-notifications-1");
            templates.remove(".transform-notifications-000001");
            templates.remove(".transform-notifications-000002");
            return templates;
        };
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        try {
            return List.of(TransformInternalIndex.getSystemIndexDescriptor(getTransformInternalIndexAdditionalSettings()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Collection<AssociatedIndexDescriptor> getAssociatedIndexDescriptors() {
        return List.of(new AssociatedIndexDescriptor(AUDIT_INDEX_PATTERN, "Audit index"));
    }

    @Override
    public void cleanUpFeature(
        ClusterService clusterService,
        Client unwrappedClient,
        ActionListener<ResetFeatureStateResponse.ResetFeatureStateStatus> finalListener
    ) {
        OriginSettingClient client = new OriginSettingClient(unwrappedClient, TRANSFORM_ORIGIN);
        ActionListener<ResetFeatureStateResponse.ResetFeatureStateStatus> unsetResetModeListener = ActionListener.wrap(
            success -> client.execute(
                SetResetModeAction.INSTANCE,
                SetResetModeActionRequest.disabled(true),
                ActionListener.wrap(resetSuccess -> finalListener.onResponse(success), resetFailure -> {
                    logger.error("failed to disable reset mode after otherwise successful transform reset", resetFailure);
                    finalListener.onFailure(
                        new ElasticsearchStatusException(
                            TransformMessages.getMessage(FAILED_TO_UNSET_RESET_MODE, "a successful feature reset"),
                            RestStatus.INTERNAL_SERVER_ERROR,
                            resetFailure
                        )
                    );
                })
            ),
            failure -> client.execute(
                SetResetModeAction.INSTANCE,
                SetResetModeActionRequest.disabled(false),
                ActionListener.wrap(resetSuccess -> finalListener.onFailure(failure), resetFailure -> {
                    logger.error(TransformMessages.getMessage(FAILED_TO_UNSET_RESET_MODE, "a failed feature reset"), resetFailure);
                    Exception ex = new ElasticsearchException(
                        TransformMessages.getMessage(FAILED_TO_UNSET_RESET_MODE, "a failed feature reset")
                    );
                    ex.addSuppressed(resetFailure);
                    failure.addSuppressed(ex);
                    finalListener.onFailure(failure);
                })
            )
        );

        ActionListener<ListTasksResponse> afterWaitingForTasks = ActionListener.wrap(listTasksResponse -> {
            listTasksResponse.rethrowFailures("Waiting for transform indexing tasks");
            SystemIndexPlugin.super.cleanUpFeature(clusterService, client, unsetResetModeListener);
        }, unsetResetModeListener::onFailure);

        ActionListener<StopTransformAction.Response> afterStoppingTransforms = ActionListener.wrap(stopTransformsResponse -> {
            if (stopTransformsResponse.isAcknowledged()
                && stopTransformsResponse.getTaskFailures().isEmpty()
                && stopTransformsResponse.getNodeFailures().isEmpty()) {
                client.admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(TransformField.TASK_NAME)
                    .setWaitForCompletion(true)
                    .execute(ActionListener.wrap(listTransformTasks -> {
                        listTransformTasks.rethrowFailures("Waiting for transform tasks");
                        client.admin()
                            .cluster()
                            .prepareListTasks()
                            .setActions("indices:data/write/bulk")
                            .setDetailed(true)
                            .setWaitForCompletion(true)
                            .setDescriptions("*" + TRANSFORM_PREFIX + "*", "*" + TRANSFORM_PREFIX_DEPRECATED + "*")
                            .execute(afterWaitingForTasks);
                    }, unsetResetModeListener::onFailure));
            } else {
                String errMsg = "Failed to reset Transform: "
                    + (stopTransformsResponse.isAcknowledged() ? "" : "not acknowledged ")
                    + (stopTransformsResponse.getNodeFailures().isEmpty()
                        ? ""
                        : "node failures: " + stopTransformsResponse.getNodeFailures() + " ")
                    + (stopTransformsResponse.getTaskFailures().isEmpty()
                        ? ""
                        : "task failures: " + stopTransformsResponse.getTaskFailures());
                unsetResetModeListener.onResponse(
                    ResetFeatureStateResponse.ResetFeatureStateStatus.failure(this.getFeatureName(), new ElasticsearchException(errMsg))
                );
            }
        }, unsetResetModeListener::onFailure);

        ActionListener<AcknowledgedResponse> afterResetModeSet = ActionListener.wrap(response -> {
            StopTransformAction.Request stopTransformsRequest = new StopTransformAction.Request(
                Metadata.ALL,
                true,
                true,
                null,
                true,
                false
            );
            client.execute(StopTransformAction.INSTANCE, stopTransformsRequest, afterStoppingTransforms);
        }, finalListener::onFailure);

        client.execute(SetResetModeAction.INSTANCE, SetResetModeActionRequest.enabled(), afterResetModeSet);
    }

    @Override
    public String getFeatureName() {
        return "transform";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages configuration and state for transforms";
    }

    public boolean includeNodeInfo() {
        return true;
    }

    public Settings getTransformInternalIndexAdditionalSettings() {
        return Settings.EMPTY;
    }
}
