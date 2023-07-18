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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.ml.action.AuditMlNotificationAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction.Request;
import org.elasticsearch.xpack.ml.packageloader.MachineLearningPackageLoader;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

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
            ThreadPool.Names.SAME
        );
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        String modelId = request.getModelId();
        ModelPackageConfig packageConfig = request.getModelPackageConfig();
        ModelImporter modelImporter = new ModelImporter(client, modelId, packageConfig);

        threadPool.executor(MachineLearningPackageLoader.UTILITY_THREAD_POOL_NAME)
            .execute(() -> importModel(client, request, modelImporter, listener));

        if (request.isWaitForCompletion() == false) {
            listener.onResponse(AcknowledgedResponse.TRUE);
        }
    }

    /**
     * This is package scope so that we can test the logic directly.
     * This should only be called from the masterOperation method and the tests
     */
    static void importModel(Client client, Request request, ModelImporter modelImporter, ActionListener<AcknowledgedResponse> listener) {
        String modelId = request.getModelId();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        try {
            final long relativeStartNanos = System.nanoTime();

            modelImporter.doImport();

            final long totalRuntimeNanos = System.nanoTime() - relativeStartNanos;
            logAndWriteNotificationAtInfo(
                client,
                modelId,
                format("finished model import after [%d] seconds", TimeUnit.NANOSECONDS.toSeconds(totalRuntimeNanos))
            );
        } catch (ElasticsearchException e) {
            recordError(client, modelId, exceptionRef, e);
        } catch (MalformedURLException e) {
            recordError(client, modelId, "an invalid URL", exceptionRef, e, RestStatus.INTERNAL_SERVER_ERROR);
        } catch (URISyntaxException e) {
            recordError(client, modelId, "an invalid URL syntax", exceptionRef, e, RestStatus.INTERNAL_SERVER_ERROR);
        } catch (IOException e) {
            recordError(client, modelId, "an IOException", exceptionRef, e, RestStatus.SERVICE_UNAVAILABLE);
        } catch (Exception e) {
            recordError(client, modelId, "an Exception", exceptionRef, e, RestStatus.INTERNAL_SERVER_ERROR);
        } finally {
            if (request.isWaitForCompletion()) {
                if (exceptionRef.get() != null) {
                    listener.onFailure(exceptionRef.get());
                } else {
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }
            }
        }
    }

    private static void recordError(Client client, String modelId, AtomicReference<Exception> exceptionRef, Exception e) {
        logAndWriteNotificationAtError(client, modelId, e.toString());
        exceptionRef.set(e);
    }

    private static void recordError(
        Client client,
        String modelId,
        String failureType,
        AtomicReference<Exception> exceptionRef,
        Exception e,
        RestStatus status
    ) {
        String message = format("Model importing failed due to %s [%s]", failureType, e);
        logAndWriteNotificationAtError(client, modelId, message);
        exceptionRef.set(new ElasticsearchStatusException(message, status, e));
    }

    private static void logAndWriteNotificationAtError(Client client, String modelId, String message) {
        writeNotification(client, modelId, message, Level.ERROR);
        logger.error(format("[%s] %s", modelId, message));
    }

    private static void logAndWriteNotificationAtInfo(Client client, String modelId, String message) {
        writeNotification(client, modelId, message, Level.INFO);
        logger.info(format("[%s] %s", modelId, message));
    }

    private static void writeNotification(Client client, String modelId, String message, Level level) {
        client.execute(
            AuditMlNotificationAction.INSTANCE,
            new AuditMlNotificationAction.Request(AuditMlNotificationAction.AuditType.INFERENCE, modelId, message, level),
            ActionListener.noop()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return null;
    }
}
