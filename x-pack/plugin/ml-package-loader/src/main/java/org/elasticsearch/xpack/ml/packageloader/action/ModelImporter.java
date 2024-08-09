/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;

/**
 * A helper class for abstracting out the use of the ModelLoaderUtils to make dependency injection testing easier.
 */
class ModelImporter {
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1MB
    private static final int MAX_IN_FLIGHT_REQUESTS = 3;
    private static final Logger logger = LogManager.getLogger(ModelImporter.class);
    private final Client client;
    private final String modelId;
    private final ModelPackageConfig config;
    private final ModelDownloadTask task;

    ModelImporter(Client client, String modelId, ModelPackageConfig packageConfig, ModelDownloadTask task) {
        this.client = client;
        this.modelId = Objects.requireNonNull(modelId);
        this.config = Objects.requireNonNull(packageConfig);
        this.task = Objects.requireNonNull(task);
    }

    public void doImport(ActionListener<AcknowledgedResponse> finalListener) {
        long size = config.getSize();

        var firstError = new AtomicReference<Exception>();
        var requestLimiter = new Semaphore(MAX_IN_FLIGHT_REQUESTS);

        try (var countingListener = new RefCountingListener(1, finalListener.map(ignored -> AcknowledgedResponse.TRUE))) {
            var releasingListener = ActionListener.<AcknowledgedResponse>wrap(r -> requestLimiter.release(), e -> {
                requestLimiter.release();
                firstError.compareAndSet(null, e);
            });
            
            // Uploading other artefacts of the model first, that way the model is last and a simple search can be used to check if the
            // download is complete
            if (Strings.isNullOrEmpty(config.getVocabularyFile()) == false) {
                uploadVocabulary(requestLimiter, countingListener);

                logger.debug(() -> format("[%s] imported model vocabulary [%s]", modelId, config.getVocabularyFile()));
            }

            URI uri = ModelLoaderUtils.resolvePackageLocation(
                config.getModelRepository(),
                config.getPackagedModelId() + ModelLoaderUtils.MODEL_FILE_EXTENSION
            );

            InputStream modelInputStream = ModelLoaderUtils.getInputStreamFromModelRepository(uri);

            ModelLoaderUtils.InputStreamChunker chunkIterator = new ModelLoaderUtils.InputStreamChunker(
                modelInputStream,
                DEFAULT_CHUNK_SIZE
            );

            // simple round up
            int totalParts = (int) ((size + DEFAULT_CHUNK_SIZE - 1) / DEFAULT_CHUNK_SIZE);

            for (int part = 0; part < totalParts - 1; ++part) {
                task.setProgress(totalParts, part);
                BytesArray definition = chunkIterator.next();

                PutTrainedModelDefinitionPartAction.Request modelPartRequest = new PutTrainedModelDefinitionPartAction.Request(
                    modelId,
                    definition,
                    part,
                    size,
                    totalParts,
                    true
                );

                executeRequestIfNotCancelled(
                    PutTrainedModelDefinitionPartAction.INSTANCE,
                    modelPartRequest,
                    requestLimiter,
                    countingListener
                );
            }

            // get the last part, this time verify the checksum and size
            BytesArray definition = chunkIterator.next();

            if (config.getSha256().equals(chunkIterator.getSha256()) == false) {
                String message = format(
                    "Model sha256 checksums do not match, expected [%s] but got [%s]",
                    config.getSha256(),
                    chunkIterator.getSha256()
                );

                throw new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR);
            }

            if (config.getSize() != chunkIterator.getTotalBytesRead()) {
                String message = format(
                    "Model size does not match, expected [%d] but got [%d]",
                    config.getSize(),
                    chunkIterator.getTotalBytesRead()
                );

                throw new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR);
            }

            PutTrainedModelDefinitionPartAction.Request finalModelPartRequest = new PutTrainedModelDefinitionPartAction.Request(
                modelId,
                definition,
                totalParts - 1,
                size,
                totalParts,
                true
            );

            executeRequestIfNotCancelled(
                PutTrainedModelDefinitionPartAction.INSTANCE,
                finalModelPartRequest,
                requestLimiter,
                countingListener
            );

            logger.debug(format("finished importing model [%s] using [%d] parts", modelId, totalParts));
        } catch (Exception e) {


//            finalListener.onFailure(e); TODO is this called twice
        }
    }

    private void uploadVocabulary(Semaphore requestLimiter, RefCountingListener listener) throws URISyntaxException,
        InterruptedException {
        ModelLoaderUtils.VocabularyParts vocabularyParts = ModelLoaderUtils.loadVocabulary(
            ModelLoaderUtils.resolvePackageLocation(config.getModelRepository(), config.getVocabularyFile())
        );

        PutTrainedModelVocabularyAction.Request request = new PutTrainedModelVocabularyAction.Request(
            modelId,
            vocabularyParts.vocab(),
            vocabularyParts.merges(),
            vocabularyParts.scores(),
            true
        );

        executeRequestIfNotCancelled(PutTrainedModelVocabularyAction.INSTANCE, request, requestLimiter, listener);
    }

    private <Request extends ActionRequest, Response extends ActionResponse> void executeRequestIfNotCancelled(
        ActionType<Response> action,
        Request request,
        Semaphore requestLimiter,
        RefCountingListener listener
    ) throws InterruptedException {
        if (task.isCancelled()) {
            throw new TaskCancelledException(format("task cancelled with reason [%s]", task.getReasonCancelled()));
        }

        requestLimiter.acquire();
        client.execute(action, request, listener.acquire(response -> requestLimiter.release())
            .<Response>delegateResponse((l, e) -> { requestLimiter.release(); l.onFailure(e);})
        );
    }
}
