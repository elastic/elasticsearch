/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Response;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.TransformDestIndexSettings;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformIndex;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.FunctionFactory;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.time.Clock;
import java.util.List;
import java.util.Map;

public class TransportUpdateTransformAction extends TransportTasksAction<TransformTask, Request, Response, Response> {

    private static final Logger logger = LogManager.getLogger(TransportUpdateTransformAction.class);
    private final XPackLicenseState licenseState;
    private final Client client;
    private final TransformConfigManager transformConfigManager;
    private final SecurityContext securityContext;
    private final TransformAuditor auditor;
    private final ThreadPool threadPool;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportUpdateTransformAction(
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        TransformServices transformServices,
        Client client,
        IngestService ingestService
    ) {
        this(
            UpdateTransformAction.NAME,
            settings,
            transportService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            clusterService,
            licenseState,
            transformServices,
            client,
            ingestService
        );
    }

    protected TransportUpdateTransformAction(
        String name,
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        TransformServices transformServices,
        Client client,
        IngestService ingestService
    ) {
        super(
            name,
            clusterService,
            transportService,
            actionFilters,
            Request::fromStreamWithBWC,
            Response::fromStreamWithBWC,
            Response::fromStreamWithBWC,
            ThreadPool.Names.SAME
        );

        this.licenseState = licenseState;
        this.client = client;
        this.transformConfigManager = transformServices.getConfigManager();
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.auditor = transformServices.getAuditor();
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState clusterState = clusterService.state();
        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

        final DiscoveryNodes nodes = clusterState.nodes();

        if (nodes.isLocalNodeElectedMaster() == false) {
            // Delegates update transform to elected master node so it becomes the coordinating node.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException());
            } else {
                transportService.sendRequest(
                    nodes.getMasterNode(),
                    actionName,
                    request,
                    new ActionListenerResponseHandler<>(listener, Response::fromStreamWithBWC)
                );
            }
            return;
        }

        // set headers to run transform as calling user
        Map<String, String> filteredHeaders = ClientHelper.filterSecurityHeaders(threadPool.getThreadContext().getHeaders());

        TransformConfigUpdate update = request.getUpdate();
        update.setHeaders(filteredHeaders);

        // GET transform and attempt to update
        // We don't want the update to complete if the config changed between GET and INDEX
        transformConfigManager.getTransformConfigurationForUpdate(request.getId(), ActionListener.wrap(configAndVersion -> {
            final TransformConfig oldConfig = configAndVersion.v1();
            final TransformConfig config = TransformConfig.rewriteForUpdate(oldConfig);

            // If it is a noop don't bother even writing the doc, save the cycles, just return here.
            // skip when:
            // - config is in the latest index
            // - rewrite did not change the config
            // - update is not making any changes
            if (config.getVersion() != null
                && config.getVersion().onOrAfter(TransformInternalIndexConstants.INDEX_VERSION_LAST_CHANGED)
                && config.equals(oldConfig)
                && update.isNoop(config)) {
                listener.onResponse(new Response(config));
                return;
            }
            TransformConfig updatedConfig = update.apply(config);

            final ActionListener<Response> updateListener;
            if (update.changesSettings(config)) {
                PersistentTasksCustomMetadata tasksMetadata = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(clusterState);
                PersistentTasksCustomMetadata.PersistentTask<?> transformTask = tasksMetadata.getTask(request.getId());

                // to send a request to apply new settings at runtime, several requirements must be met:
                // - transform must be running, meaning a task exists
                // - transform is not failed (stopped transforms do not have a task)
                // - the node where transform is executed on is at least 7.8.0 in order to understand the request
                if (transformTask != null
                    && transformTask.isAssigned()
                    && transformTask.getState() instanceof TransformState
                    && ((TransformState) transformTask.getState()).getTaskState() != TransformTaskState.FAILED
                    && clusterState.nodes().get(transformTask.getExecutorNode()).getVersion().onOrAfter(Version.V_7_8_0)) {
                    request.setNodes(transformTask.getExecutorNode());
                    updateListener = ActionListener.wrap(updateResponse -> {
                        request.setConfig(updateResponse.getConfig());
                        super.doExecute(task, request, listener);
                    }, listener::onFailure);
                } else {
                    updateListener = listener;
                }
            } else {
                updateListener = listener;
            }

            // <3> Update the transform
            ActionListener<ValidateTransformAction.Response> validateTransformListener = ActionListener.wrap(
                validationResponse -> {
                    updateTransform(
                        request,
                        updatedConfig,
                        validationResponse.getDestIndexMappings(),
                        configAndVersion.v2(),
                        clusterState,
                        updateListener);
                },
                listener::onFailure
            );

            // <2> Validate source and destination indices
            ActionListener<Void> checkPrivilegesListener = ActionListener.wrap(
                aVoid -> {
                    client.execute(
                        ValidateTransformAction.INSTANCE,
                        new ValidateTransformAction.Request(updatedConfig, request.isDeferValidation()),
                        validateTransformListener
                    );
                },
                listener::onFailure);

            // <1> Early check to verify that the user can create the destination index and can read from the source
            if (licenseState.isSecurityEnabled() && request.isDeferValidation() == false) {
                TransformPrivilegeChecker.checkPrivileges(
                    "update",
                    securityContext,
                    indexNameExpressionResolver,
                    clusterState,
                    client,
                    updatedConfig,
                    true,
                    checkPrivilegesListener);
            } else { // No security enabled, just move on
                checkPrivilegesListener.onResponse(null);
            }
        }, listener::onFailure));
    }

    @Override
    protected void taskOperation(Request request, TransformTask transformTask, ActionListener<Response> listener) {
        // apply the settings
        transformTask.applyNewSettings(request.getConfig().getSettings());
        listener.onResponse(new Response(request.getConfig()));
    }

    @Override
    protected Response newResponse(
        Request request,
        List<Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        // there should be only 1 response, todo: check
        return tasks.get(0);
    }

    private void updateTransform(
        Request request,
        TransformConfig config,
        Map<String, String> mappings,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        ClusterState clusterState,
        ActionListener<Response> listener
    ) {
        final Function function = FunctionFactory.create(config);

        // <3> Return to the listener
        ActionListener<Boolean> putTransformConfigurationListener = ActionListener.wrap(putTransformConfigurationResult -> {
            auditor.info(config.getId(), "Updated transform.");
            List<String> warnings = TransformConfigLinter.getWarnings(function, config.getSource(), config.getSyncConfig());
            for (String warning : warnings) {
                logger.warn(new ParameterizedMessage("[{}] {}", config.getId(), warning));
                auditor.warning(config.getId(), warning);
            }
            transformConfigManager.deleteOldTransformConfigurations(request.getId(), ActionListener.wrap(r -> {
                logger.trace("[{}] successfully deleted old transform configurations", request.getId());
                listener.onResponse(new Response(config));
            }, e -> {
                logger.warn(LoggerMessageFormat.format("[{}] failed deleting old transform configurations.", request.getId()), e);
                listener.onResponse(new Response(config));
            }));
        },
            // If we failed to INDEX AND we created the destination index, the destination index will still be around
            // This is a similar behavior to _start
            listener::onFailure
        );

        // <2> Update our transform
        ActionListener<Boolean> createDestinationListener = ActionListener.wrap(
            createDestResponse -> transformConfigManager.updateTransformConfiguration(
                config,
                seqNoPrimaryTermAndIndex,
                putTransformConfigurationListener
            ),
            listener::onFailure
        );

        // <1> Create destination index if necessary
        String[] dest = indexNameExpressionResolver.concreteIndexNames(
            clusterState,
            IndicesOptions.lenientExpandOpen(),
            config.getDestination().getIndex()
        );
        String[] src = indexNameExpressionResolver.concreteIndexNames(
            clusterState,
            IndicesOptions.lenientExpandOpen(),
            true,
            config.getSource().getIndex()
        );
        // If we are running, we should verify that the destination index exists and create it if it does not
        if (PersistentTasksCustomMetadata.getTaskWithId(clusterState, request.getId()) != null && dest.length == 0
        // Verify we have source indices. The user could defer_validations and if the task is already running
        // we allow source indices to disappear. If the source and destination indices do not exist, don't do anything
        // the transform will just have to dynamically create the destination index without special mapping.
            && src.length > 0) {
            createDestinationIndex(config, mappings, createDestinationListener);
        } else {
            createDestinationListener.onResponse(null);
        }
    }

    private void createDestinationIndex(TransformConfig config, Map<String, String> mappings, ActionListener<Boolean> listener) {
        TransformDestIndexSettings generatedDestIndexSettings = TransformIndex.createTransformDestIndexSettings(
            mappings,
            config.getId(),
            Clock.systemUTC()
        );
        TransformIndex.createDestinationIndex(client, config, generatedDestIndexSettings, listener);
    }
}
