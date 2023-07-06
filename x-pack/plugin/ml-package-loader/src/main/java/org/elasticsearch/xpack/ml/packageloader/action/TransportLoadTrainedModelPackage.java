/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.ml.action.AuditMlNotificationAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction.Request;
import org.elasticsearch.xpack.ml.packageloader.MachineLearningPackageLoader;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportLoadTrainedModelPackage extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private static final int DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB

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
        ModelPackageConfig modelPackageConfig = request.getModelPackageConfig();
        String repository = modelPackageConfig.getModelRepository();
        String modelId = request.getModelId();
        long size = modelPackageConfig.getSize();
        String packagedModelId = modelPackageConfig.getPackagedModelId();

        threadPool.executor(MachineLearningPackageLoader.UTILITY_THREAD_POOL_NAME).execute(() -> {
            try {
                final long relativeStartNanos = System.nanoTime();
                logAndWriteNotificationAtInfo(modelId, "starting model upload");

                URI uri = ModelLoaderUtils.resolvePackageLocation(repository, packagedModelId + ModelLoaderUtils.MODEL_FILE_EXTENSION);

                // Uploading other artefacts of the model first, that way the model is last and a simple search can be used to check if the
                // download is complete
                if (Strings.isNullOrEmpty(modelPackageConfig.getVocabularyFile()) == false) {
                    Tuple<List<String>, List<String>> vocabularyAndMerges = ModelLoaderUtils.loadVocabulary(
                        ModelLoaderUtils.resolvePackageLocation(repository, modelPackageConfig.getVocabularyFile())
                    );

                    PutTrainedModelVocabularyAction.Request r2 = new PutTrainedModelVocabularyAction.Request(
                        modelId,
                        vocabularyAndMerges.v1(),
                        vocabularyAndMerges.v2(),
                        List.of()
                    );
                    client.execute(PutTrainedModelVocabularyAction.INSTANCE, r2).actionGet();

                    logAndWriteNotificationAtDebug(
                        modelId,
                        format("uploaded model vocabulary [%s]", modelPackageConfig.getVocabularyFile())
                    );
                }

                InputStream modelInputStream = ModelLoaderUtils.getInputStreamFromModelRepository(uri);

                ModelLoaderUtils.InputStreamChunker chunkIterator = new ModelLoaderUtils.InputStreamChunker(
                    modelInputStream,
                    DEFAULT_CHUNK_SIZE
                );

                // simple round up
                int totalParts = (int) ((size + DEFAULT_CHUNK_SIZE - 1) / DEFAULT_CHUNK_SIZE);

                for (int part = 0; part < totalParts - 1; ++part) {
                    BytesArray definition = chunkIterator.next();

                    PutTrainedModelDefinitionPartAction.Request r = new PutTrainedModelDefinitionPartAction.Request(
                        modelId,
                        definition,
                        part,
                        size,
                        totalParts
                    );

                    client.execute(PutTrainedModelDefinitionPartAction.INSTANCE, r).actionGet();
                }

                // get the last part, this time verify the checksum and size
                BytesArray definition = chunkIterator.next();

                if (modelPackageConfig.getSha256().equals(chunkIterator.getSha256()) == false) {
                    String message = format(
                        "Model sha256 checksums do not match, expected [%s] but got [%s]",
                        modelPackageConfig.getSha256(),
                        chunkIterator.getSha256()
                    );
                    logAndWriteNotificationAtError(modelId, message);
                    return;
                }

                if (modelPackageConfig.getSize() != chunkIterator.getTotalBytesRead()) {
                    String message = format(
                        "Model size does not match, expected [%d] but got [%d]",
                        modelPackageConfig.getSize(),
                        chunkIterator.getTotalBytesRead()
                    );
                    logAndWriteNotificationAtError(modelId, message);
                    return;
                }

                PutTrainedModelDefinitionPartAction.Request r = new PutTrainedModelDefinitionPartAction.Request(
                    modelId,
                    definition,
                    totalParts - 1,
                    size,
                    totalParts
                );

                client.execute(PutTrainedModelDefinitionPartAction.INSTANCE, r).actionGet();
                logger.debug(format("finished uploading model [%s] using [%d] parts", modelId, totalParts));

                final long totalRuntimeNanos = System.nanoTime() - relativeStartNanos;
                logAndWriteNotificationAtInfo(
                    modelId,
                    format("finished model upload after [%d] seconds", TimeUnit.NANOSECONDS.toSeconds(totalRuntimeNanos))
                );
                listener.onResponse(AcknowledgedResponse.TRUE);

            } catch (MalformedURLException e) {
                logAndWriteNotificationAtError(modelId, format("Invalid URL [%s]", e));
            } catch (URISyntaxException e) {
                logAndWriteNotificationAtError(modelId, format("Invalid URL syntax [%s]", e));
            } catch (IOException e) {
                logAndWriteNotificationAtError(modelId, format("IOException [%s]", e));
            }
        });

        // listener.onResponse(AcknowledgedResponse.TRUE);
    }

    private void logAndWriteNotificationAtError(String modelId, String message) {
        writeNotification(modelId, message, Level.ERROR);
        logger.error(format("[%s] %s", modelId, message));
    }

    private void logAndWriteNotificationAtDebug(String modelId, String message) {
        writeNotification(modelId, message, Level.INFO); // info is the lowest level
        logger.debug(() -> format("[%s] %s", modelId, message));
    }

    private void logAndWriteNotificationAtInfo(String modelId, String message) {
        writeNotification(modelId, message, Level.INFO);
        logger.info(format("[%s] %s", modelId, message));
    }

    private void writeNotification(String modelId, String message, Level level) {
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
