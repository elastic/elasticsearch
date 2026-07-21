/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.TransformExtensionHolder;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.AuthorizationStatePersistenceUtils;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.FunctionFactory;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.transform.utils.SecondaryAuthorizationUtils.getSecurityHeadersPreferringSecondary;

public class TransportUpdateTransformAction extends TransportTasksAction<TransformTask, Request, Response, Response> {

    private static final Logger logger = LogManager.getLogger(TransportUpdateTransformAction.class);
    private final Settings settings;
    private final Client client;
    private final TransformConfigManager transformConfigManager;
    private final SecurityContext securityContext;
    private final TransformAuditor auditor;
    private final ThreadPool threadPool;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings destIndexSettings;
    private final BooleanSupplier hasLinkedProjects;
    private final ProjectResolver projectResolver;
    private final TransformCloudCredentialManager cloudCredentialManager;

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
        TransformExtensionHolder transformExtensionHolder,
        ProjectResolver projectResolver
    ) {
        super(
            UpdateTransformAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.settings = settings;
        this.client = client;
        this.transformConfigManager = transformServices.configManager();
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.auditor = transformServices.auditor();
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.destIndexSettings = transformExtensionHolder.getTransformExtension().getTransformDestinationIndexSettings();
        this.hasLinkedProjects = () -> transformServices.hasLinkedProjects().apply(projectResolver.getProjectId());
        this.projectResolver = projectResolver;
        this.cloudCredentialManager = transformServices.cloudCredentialManager();
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        // Extract on the coordinating node, before the request is forwarded to master — the
        // AUTHENTICATING_CLOUD_TOKEN_THREAD_CONTEXT transient does not survive master forwarding.
        // A no-op when this doExecute re-runs on the master with an already-deserialized request
        // carrying the credential from the coordinating node's extraction.
        CloudCredential callerCredential = cloudCredentialManager.currentCallerCredential();
        if (callerCredential != null) {
            request.setCloudCredential(callerCredential);
        }
        final ActionListener<Response> releasingListener = ActionListener.releaseAfter(listener, request);

        final ClusterState clusterState = clusterService.state();
        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);
        if (TransformMetadata.isUpgradeMode(projectResolver.getProjectMetadata(clusterState))) {
            releasingListener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot update any Transform while the Transform feature is upgrading.",
                    RestStatus.CONFLICT
                )
            );
            return;
        }

        final DiscoveryNodes nodes = clusterState.nodes();

        if (nodes.isLocalNodeElectedMaster() == false) {
            // Delegates update transform to elected master node so it becomes the coordinating node.
            if (nodes.getMasterNode() == null) {
                releasingListener.onFailure(new MasterNotDiscoveredException());
            } else {
                transportService.sendRequest(
                    nodes.getMasterNode(),
                    actionName,
                    request,
                    new ActionListenerResponseHandler<>(releasingListener, Response::new, TransportResponseHandler.TRANSPORT_WORKER)
                );
            }
            return;
        }

        TransformConfigUpdate update = request.getUpdate();
        update.setHeaders(getSecurityHeadersPreferringSecondary(threadPool, securityContext, clusterState));

        // GET transform and attempt to update
        // We don't want the update to complete if the config changed between GET and INDEX
        transformConfigManager.getTransformConfigurationForUpdate(
            request.getId(),
            ActionListener.wrap(
                configAndVersion -> TransformUpdater.updateTransform(
                    securityContext,
                    indexNameExpressionResolver,
                    clusterState,
                    settings,
                    client,
                    transformConfigManager,
                    auditor,
                    configAndVersion.v1(),
                    update,
                    configAndVersion.v2(),
                    request.isDeferValidation(),
                    false, // dryRun
                    true, // checkAccess
                    hasLinkedProjects.getAsBoolean(),
                    request.getTimeout(),
                    destIndexSettings,
                    cloudCredentialManager,
                    true, // mintCloudCredential
                    request.getCloudCredential(),
                    ActionListener.wrap(updateResult -> {
                        TransformConfig originalConfig = configAndVersion.v1();
                        TransformConfig updatedConfig = updateResult.getConfig();
                        AuthorizationState authState = updateResult.getAuthState();
                        auditor.info(updatedConfig.getId(), "Updated transform.");
                        logger.info("[{}] Updated transform [{}]", updatedConfig.getId(), updateResult.getStatus());

                        auditProjectRoutingChanges(originalConfig, updatedConfig);

                        checkTransformConfigAndLogWarnings(updatedConfig);

                        PersistentTasksCustomMetadata.PersistentTask<?> transformTask = TransformTask.getTransformTask(
                            request.getId(),
                            projectResolver.getProjectMetadata(clusterState)
                        );
                        // a task can receive runtime settings updates only when it exists, is assigned
                        // to a node, and is not in the failed state (stopped transforms have no task)
                        boolean isRunning = transformTask != null
                            && transformTask.isAssigned()
                            && transformTask.getState() instanceof TransformState
                            && ((TransformState) transformTask.getState()).getTaskState() != TransformTaskState.FAILED;

                        // If the credentialId changed and the task is NOT running, revoke + delete the
                        // prior credential here — the indexer will never see the new config to do it.
                        // For running tasks, the indexer's onStart hook handles the swap on next reload.
                        ActionListener<Response> afterCredentialCleanup = wrapWithPriorCredentialCleanupIfNeeded(
                            releasingListener,
                            originalConfig.getCredentialId(),
                            updatedConfig.getCredentialId(),
                            updatedConfig.getId(),
                            isRunning
                        );

                        boolean updateChangesSettings = update.changesSettings(originalConfig);
                        boolean updateChangesHeaders = update.changesHeaders(originalConfig);
                        boolean updateChangesDestIndex = update.changesDestIndex(originalConfig);
                        boolean updateFrequency = update.changesFrequency(originalConfig);
                        if (updateChangesSettings || updateChangesHeaders || updateChangesDestIndex || updateFrequency) {
                            if (isRunning) {

                                ActionListener<Response> taskUpdateListener = ActionListener.wrap(afterCredentialCleanup::onResponse, e -> {
                                    // benign: A transform might be stopped meanwhile, this is not a problem
                                    if (e instanceof TransformTaskDisappearedDuringUpdateException) {
                                        logger.debug("[{}] transform task disappeared during update, ignoring", request.getId());
                                        afterCredentialCleanup.onResponse(new Response(updatedConfig));
                                        return;
                                    }

                                    if (e instanceof TransformTaskUpdateException) {
                                        // BWC: only log a warning as response object can not be changed
                                        logger.warn(
                                            () -> format(
                                                "[%s] failed to notify running transform task about update. "
                                                    + "New settings will be applied after next checkpoint.",
                                                request.getId()
                                            ),
                                            e
                                        );

                                        afterCredentialCleanup.onResponse(new Response(updatedConfig));
                                        return;
                                    }

                                    afterCredentialCleanup.onFailure(e);
                                });

                                request.setNodes(transformTask.getExecutorNode());
                                request.setConfig(updatedConfig);
                                request.setAuthState(authState);
                                // Mint/validate (if any) already consumed their own copies of the
                                // credential; avoid re-shipping it to the task's executor node, which
                                // has no releaseAfter to close it.
                                IOUtils.closeWhileHandlingException(request.setCloudCredential(null));
                                super.doExecute(task, request, taskUpdateListener);
                                return;
                            } else if (updateChangesHeaders) {
                                AuthorizationStatePersistenceUtils.persistAuthState(
                                    settings,
                                    transformConfigManager,
                                    updatedConfig.getId(),
                                    authState,
                                    ActionListener.wrap(
                                        aVoid -> afterCredentialCleanup.onResponse(new Response(updatedConfig)),
                                        afterCredentialCleanup::onFailure
                                    )
                                );
                            } else {
                                afterCredentialCleanup.onResponse(new Response(updatedConfig));
                            }
                        } else {
                            afterCredentialCleanup.onResponse(new Response(updatedConfig));
                        }
                    }, releasingListener::onFailure)
                ),
                releasingListener::onFailure
            )
        );
    }

    private void auditProjectRoutingChanges(TransformConfig originalConfig, TransformConfig updatedConfig) {
        if (Objects.equals(originalConfig.getSource().getProjectRouting(), updatedConfig.getSource().getProjectRouting()) == false) {
            var originalProjectRouting = originalConfig.getSource().getProjectRouting();
            var updatedProjectRouting = updatedConfig.getSource().getProjectRouting();

            if (originalProjectRouting == null) {
                auditor.info(updatedConfig.getId(), format("project_routing has been set to [%s].", updatedProjectRouting));
                logger.info("[{}] project_routing has been set to [{}].", updatedConfig.getId(), updatedProjectRouting);
            } else if (updatedProjectRouting == null) {
                auditor.info(updatedConfig.getId(), format("project_routing [%s] has been removed.", originalProjectRouting));
                logger.info("[{}] project_routing [{}] has been removed.", updatedConfig.getId(), originalProjectRouting);
            } else {
                auditor.info(
                    updatedConfig.getId(),
                    format("project_routing updated from [%s] to [%s].", originalProjectRouting, updatedProjectRouting)
                );
                logger.info(
                    "[{}] project_routing updated from [{}] to [{}].",
                    updatedConfig.getId(),
                    originalProjectRouting,
                    updatedProjectRouting
                );
            }
        }
    }

    private void checkTransformConfigAndLogWarnings(TransformConfig config) {
        final Function function = FunctionFactory.create(config);
        List<String> warnings = TransformConfigLinter.getWarnings(function, config.getSource(), config.getSyncConfig());

        for (String warning : warnings) {
            logger.warn(() -> format("[%s] %s", config.getId(), warning));
            auditor.warning(config.getId(), warning);
        }
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        Request request,
        TransformTask transformTask,
        ActionListener<Response> listener
    ) {
        // Apply settings synchronously to the running task. The new cloud credential, if any, is
        // picked up by the indexer's onStart hook on the next config-reload — there is no separate
        // refresh-from-store path for credentials anymore.
        transformTask.applyNewSettings(request.getConfig().getSettings());
        transformTask.applyNewAuthState(request.getAuthState());
        transformTask.checkAndResetDestinationIndexBlock(request.getConfig());
        transformTask.applyNewFrequency(request.getConfig());
        listener.onResponse(new Response(request.getConfig()));
    }

    /**
     * Wraps the given listener so that — on success — the prior credential is revoked and deleted
     * before the response is delivered to the caller. Skipped when (a) the cloud credential manager
     * is not available (Reset / Upgrade paths), (b) the credentialId did not change, or (c) the
     * task is still running (the indexer's onStart hook will handle the swap + revoke on next
     * config reload). Otherwise — task stopped/failed/disappeared — the master node must perform
     * the cleanup itself because the indexer will never see the new config.
     */
    private ActionListener<Response> wrapWithPriorCredentialCleanupIfNeeded(
        ActionListener<Response> listener,
        String priorCredentialId,
        String newCredentialId,
        String transformId,
        boolean isRunning
    ) {
        if (cloudCredentialManager == null
            || priorCredentialId == null
            || Objects.equals(priorCredentialId, newCredentialId)
            || isRunning) {
            return listener;
        }
        return listener.delegateFailureAndWrap(
            (l, response) -> cloudCredentialManager.loadRevokeAndDeleteByTokenId(
                transformId,
                priorCredentialId,
                ActionListener.running(() -> l.onResponse(response))
            )
        );
    }

    @Override
    protected Response newResponse(
        Request request,
        List<Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        if (tasks.isEmpty()) {
            if (taskOperationFailures.isEmpty() == false) {
                throw new TransformTaskUpdateException("Failed to update running transform task.", taskOperationFailures.get(0).getCause());
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw new TransformTaskUpdateException("Failed to update running transform task.", failedNodeExceptions.get(0));
            } else {
                throw new TransformTaskDisappearedDuringUpdateException("Could not update running transform as it has been stopped.");
            }
        }

        return tasks.get(0);
    }

    private static class TransformTaskUpdateException extends ElasticsearchException {
        TransformTaskUpdateException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }

    private static class TransformTaskDisappearedDuringUpdateException extends ElasticsearchException {
        TransformTaskDisappearedDuringUpdateException(String msg) {
            super(msg);
        }
    }

}
