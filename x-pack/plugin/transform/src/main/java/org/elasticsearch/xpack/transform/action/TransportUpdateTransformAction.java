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
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.ingest.IngestService;
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
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.FunctionFactory;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.List;
import java.util.Map;

public class TransportUpdateTransformAction extends TransportTasksAction<TransformTask, Request, Response, Response> {

    private static final Logger logger = LogManager.getLogger(TransportUpdateTransformAction.class);
    private final Settings settings;
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
        TransformServices transformServices,
        Client client,
        IngestService ingestService
    ) {
        super(
            UpdateTransformAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            Response::new,
            Response::new,
            ThreadPool.Names.SAME
        );

        this.settings = settings;
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
                    new ActionListenerResponseHandler<>(listener, Response::new)
                );
            }
            return;
        }

        // set headers to run transform as calling user
        Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(
            threadPool.getThreadContext(),
            clusterService.state()
        );

        TransformConfigUpdate update = request.getUpdate();
        update.setHeaders(filteredHeaders);

        // GET transform and attempt to update
        // We don't want the update to complete if the config changed between GET and INDEX
        transformConfigManager.getTransformConfigurationForUpdate(request.getId(), ActionListener.wrap(configAndVersion -> {
            TransformUpdater.updateTransform(
                securityContext,
                indexNameExpressionResolver,
                clusterState,
                settings,
                client,
                transformConfigManager,
                configAndVersion.v1(),
                update,
                configAndVersion.v2(),
                request.isDeferValidation(),
                false, // dryRun
                true, // checkAccess
                request.getTimeout(),
                ActionListener.wrap(updateResponse -> {
                    TransformConfig updatedConfig = updateResponse.getConfig();
                    auditor.info(updatedConfig.getId(), "Updated transform.");
                    logger.debug("[{}] Updated transform [{}]", updatedConfig.getId(), updateResponse.getStatus());

                    checkTransformConfigAndLogWarnings(updatedConfig);

                    if (update.changesSettings(configAndVersion.v1())) {
                        PersistentTasksCustomMetadata.PersistentTask<?> transformTask = TransformTask.getTransformTask(
                            request.getId(),
                            clusterState
                        );

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
                            request.setConfig(updatedConfig);
                            super.doExecute(task, request, listener);
                            return;
                        }
                    }
                    listener.onResponse(new Response(updatedConfig));
                }, listener::onFailure)
            );
        }, listener::onFailure));
    }

    private void checkTransformConfigAndLogWarnings(TransformConfig config) {
        final Function function = FunctionFactory.create(config);
        List<String> warnings = TransformConfigLinter.getWarnings(function, config.getSource(), config.getSyncConfig());

        for (String warning : warnings) {
            logger.warn(new ParameterizedMessage("[{}] {}", config.getId(), warning));
            auditor.warning(config.getId(), warning);
        }
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

}
