/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.ml.action.AuditMlNotificationAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction.Request;
import org.elasticsearch.xpack.ml.packageloader.MachineLearningPackageLoader;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ml.MlTasks.MODEL_IMPORT_TASK_ACTION;
import static org.elasticsearch.xpack.core.ml.MlTasks.MODEL_IMPORT_TASK_TYPE;
import static org.elasticsearch.xpack.core.ml.MlTasks.downloadModelTaskDescription;

public class TransportLoadTrainedModelPackage extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportLoadTrainedModelPackage.class);

    private final Client client;

    @Inject
    public TransportLoadTrainedModelPackage(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            LoadTrainedModelPackageAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            LoadTrainedModelPackageAction.Request::new,
            indexNameExpressionResolver,
            NodeAcknowledgedResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return null;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        ModelDownloadTask downloadTask = createDownloadTask(request);

        try {
            ParentTaskAssigningClient parentTaskAssigningClient = getParentTaskAssigningClient(downloadTask);

            ModelImporter modelImporter = new ModelImporter(
                parentTaskAssigningClient,
                request.getModelId(),
                request.getModelPackageConfig(),
                downloadTask
            );

            threadPool.executor(MachineLearningPackageLoader.UTILITY_THREAD_POOL_NAME)
                .execute(() -> importModel(client, taskManager, request, modelImporter, listener, downloadTask));
        } catch (Exception e) {
            taskManager.unregister(downloadTask);
            listener.onFailure(e);
            return;
        }

        if (request.isWaitForCompletion() == false) {
            listener.onResponse(AcknowledgedResponse.TRUE);
        }
    }

    private ParentTaskAssigningClient getParentTaskAssigningClient(Task originTask) {
        var parentTaskId = new TaskId(clusterService.localNode().getId(), originTask.getId());
        return new ParentTaskAssigningClient(client, parentTaskId);
    }

    /**
     * This is package scope so that we can test the logic directly.
     * This should only be called from the masterOperation method and the tests
     *
     * @param auditClient a client which should only be used to send audit notifications. This client cannot be associated with the passed
     *                    in task, that way when the task is cancelled the notification requests can
     *                    still be performed. If it is associated with the task (i.e. via ParentTaskAssigningClient),
     *                    then the requests will throw a TaskCancelledException.
     */
    static void importModel(
        Client auditClient,
        TaskManager taskManager,
        Request request,
        ModelImporter modelImporter,
        ActionListener<AcknowledgedResponse> listener,
        Task task
    ) {
        String modelId = request.getModelId();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        try {
            final long relativeStartNanos = System.nanoTime();

            logAndWriteNotificationAtLevel(auditClient, modelId, "starting model import", Level.INFO);

            modelImporter.doImport();

            final long totalRuntimeNanos = System.nanoTime() - relativeStartNanos;
            logAndWriteNotificationAtLevel(
                auditClient,
                modelId,
                format("finished model import after [%d] seconds", TimeUnit.NANOSECONDS.toSeconds(totalRuntimeNanos)),
                Level.INFO
            );
        } catch (TaskCancelledException e) {
            recordError(auditClient, modelId, exceptionRef, e, Level.WARNING);
        } catch (ElasticsearchException e) {
            recordError(auditClient, modelId, exceptionRef, e, Level.ERROR);
        } catch (MalformedURLException e) {
            recordError(auditClient, modelId, "an invalid URL", exceptionRef, e, Level.ERROR, RestStatus.INTERNAL_SERVER_ERROR);
        } catch (URISyntaxException e) {
            recordError(auditClient, modelId, "an invalid URL syntax", exceptionRef, e, Level.ERROR, RestStatus.INTERNAL_SERVER_ERROR);
        } catch (IOException e) {
            recordError(auditClient, modelId, "an IOException", exceptionRef, e, Level.ERROR, RestStatus.SERVICE_UNAVAILABLE);
        } catch (Exception e) {
            recordError(auditClient, modelId, "an Exception", exceptionRef, e, Level.ERROR, RestStatus.INTERNAL_SERVER_ERROR);
        } finally {
            taskManager.unregister(task);

            if (request.isWaitForCompletion()) {
                if (exceptionRef.get() != null) {
                    listener.onFailure(exceptionRef.get());
                } else {
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }

            }
        }
    }

    private ModelDownloadTask createDownloadTask(Request request) {
        // Loading the model is done by a separate task, so needs a new trace context
        try (var ignored = threadPool.getThreadContext().newTraceContext()) {
            return (ModelDownloadTask) taskManager.register(MODEL_IMPORT_TASK_TYPE, MODEL_IMPORT_TASK_ACTION, new TaskAwareRequest() {
                @Override
                public void setParentTask(TaskId taskId) {
                    request.setParentTask(taskId);
                }

                @Override
                public void setRequestId(long requestId) {
                    request.setRequestId(requestId);
                }

                @Override
                public TaskId getParentTask() {
                    return request.getParentTask();
                }

                @Override
                public ModelDownloadTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                    return new ModelDownloadTask(
                        id,
                        type,
                        action,
                        downloadModelTaskDescription(request.getModelId()),
                        parentTaskId,
                        headers
                    );
                }
            }, false);
        }
    }

    private static void recordError(
        Client client,
        String modelId,
        AtomicReference<Exception> exceptionRef,
        ElasticsearchException e,
        Level level
    ) {
        String message = format("Model importing failed due to [%s]", e.getDetailedMessage());
        logAndWriteNotificationAtLevel(client, modelId, message, level);
        exceptionRef.set(e);
    }

    private static void recordError(
        Client client,
        String modelId,
        String failureType,
        AtomicReference<Exception> exceptionRef,
        Exception e,
        Level level,
        RestStatus status
    ) {
        String message = format("Model importing failed due to %s [%s]", failureType, e);
        logAndWriteNotificationAtLevel(client, modelId, message, level);
        exceptionRef.set(new ElasticsearchStatusException(message, status, e));
    }

    private static void logAndWriteNotificationAtLevel(Client client, String modelId, String message, Level level) {
        writeNotification(client, modelId, message, level);
        logger.log(level.log4jLevel(), format("[%s] %s", modelId, message));
    }

    private static void writeNotification(Client client, String modelId, String message, Level level) {
        client.execute(
            AuditMlNotificationAction.INSTANCE,
            new AuditMlNotificationAction.Request(AuditMlNotificationAction.AuditType.INFERENCE, modelId, message, level),
            ActionListener.noop()
        );
    }
}
