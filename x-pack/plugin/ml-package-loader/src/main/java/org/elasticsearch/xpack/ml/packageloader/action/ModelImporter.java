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
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.ml.packageloader.MachineLearningPackageLoader;

import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;

/**
 * For downloading the model vocabulary and definition file and
 * indexing those files in Elasticsearch.
 * Holding the large model definition file in memory will consume
 * too much memory, instead it is streamed in chunks and each chunk
 * written to the index in a non-blocking request. The number of
 * index requests is limited to {@link #MAX_IN_FLIGHT_REQUESTS}
 * also to prevent too much memory being used.
 * Only 1 thread can read the model definition stream at a time,
 * this is ensured by using a fixed size threadpool with a single
 * thread.
 */
class ModelImporter {
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1MB
    private static final int MAX_IN_FLIGHT_REQUESTS = 5;
    private static final Logger logger = LogManager.getLogger(ModelImporter.class);
    private final Client client;
    private final String modelId;
    private final ModelPackageConfig config;
    private final ModelDownloadTask task;
    private final ExecutorService executorService;
    private final AtomicBoolean listenerIsClosed = new AtomicBoolean(false);

    ModelImporter(Client client, String modelId, ModelPackageConfig packageConfig, ModelDownloadTask task, ThreadPool threadPool) {
        this.client = client;
        this.modelId = Objects.requireNonNull(modelId);
        this.config = Objects.requireNonNull(packageConfig);
        this.task = Objects.requireNonNull(task);
        this.executorService = threadPool.executor(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME);
    }

    public void doImport(ActionListener<AcknowledgedResponse> listener) {
        executorService.execute(() -> doImportInternal(listener));
    }

    private void doImportInternal(ActionListener<AcknowledgedResponse> finalListener) {
        assert ThreadPool.assertCurrentThreadPool(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME)
            : format(
                "Model download must execute from [%s] but thread is [%s]",
                MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME,
                Thread.currentThread().getName()
            );

        long size = config.getSize();
        // simple round up
        int totalParts = (int) ((size + DEFAULT_CHUNK_SIZE - 1) / DEFAULT_CHUNK_SIZE);
        InputStream modelInputStream;
        ModelLoaderUtils.VocabularyParts vocabularyParts = null;

        try {
            URI uri = ModelLoaderUtils.resolvePackageLocation(
                config.getModelRepository(),
                config.getPackagedModelId() + ModelLoaderUtils.MODEL_FILE_EXTENSION
            );
            modelInputStream = ModelLoaderUtils.getInputStreamFromModelRepository(uri);

            if (config.getVocabularyFile() != null) {
                vocabularyParts = ModelLoaderUtils.loadVocabulary(
                    ModelLoaderUtils.resolvePackageLocation(config.getModelRepository(), config.getVocabularyFile())
                );
            }
        } catch (Exception e) {
            finalListener.onFailure(e);
            return;
        }

        downloadParts(
            new ModelLoaderUtils.InputStreamChunker(modelInputStream, DEFAULT_CHUNK_SIZE, totalParts),
            size,
            vocabularyParts,
            finalListener
        );
    }

    void downloadParts(
        ModelLoaderUtils.InputStreamChunker chunkIterator,
        long size,
        @Nullable ModelLoaderUtils.VocabularyParts vocabularyParts,
        ActionListener<AcknowledgedResponse> finalListener
    ) {
        var countingListener = new RefCountingListener(1, finalListener.map(ignored -> {
            checkDownloadComplete(chunkIterator);
            return AcknowledgedResponse.TRUE;
        }));
        // Uploading other artefacts of the model first, that way the model is last and a simple search can be used to check if the
        // download is complete
        if (vocabularyParts != null) {
            uploadVocabulary(vocabularyParts, countingListener.acquire(r -> {
                logger.debug(() -> format("[%s] imported model vocabulary [%s]", modelId, config.getVocabularyFile()));
            }));
        }

        for (int part = 0; part < MAX_IN_FLIGHT_REQUESTS; ++part) {
            doNextPart(size, chunkIterator, countingListener);
        }
    }

    private void doNextPart(long size, ModelLoaderUtils.InputStreamChunker chunkIterator, RefCountingListener countingListener) {
        assert ThreadPool.assertCurrentThreadPool(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME)
            : format(
                "Model download must execute from [%s] but thread is [%s]",
                MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME,
                Thread.currentThread().getName()
            );

        if (countingListener.isFailing()) {
            if (listenerIsClosed.compareAndSet(false, true)) {
                countingListener.close();
            }
            return;
        }

        task.setProgress(chunkIterator.getTotalParts(), Math.max(0, chunkIterator.getCurrentPart().get()));
        try {
            BytesArray definition = chunkIterator.next();

            if (task.isCancelled()) {
                throw new TaskCancelledException(format("task cancelled with reason [%s]", task.getReasonCancelled()));
            }

            if (definition.length() == 0) {
                // download complete
                if (listenerIsClosed.compareAndSet(false, true)) {
                    countingListener.close();
                }
                return;
            }

            // Index the downloaded chunk and schedule the next download once
            // the chunk is written.
            // The key thing here is that the threadpool only has a single
            // thread preventing concurrent access to the model stream while
            // allowing multiple index requests to be in flight.
            indexPart(chunkIterator.getCurrentPart().get(), chunkIterator.getTotalParts(), size, definition, countingListener.acquire(r -> {
                executorService.execute(() -> doNextPart(size, chunkIterator, countingListener));
            }));
        } catch (Exception e) {
            countingListener.acquire().onFailure(e);
            if (listenerIsClosed.compareAndSet(false, true)) {
                countingListener.close();
            }
        }
    }

    private void uploadVocabulary(ModelLoaderUtils.VocabularyParts vocabularyParts, ActionListener<AcknowledgedResponse> listener) {
        PutTrainedModelVocabularyAction.Request request = new PutTrainedModelVocabularyAction.Request(
            modelId,
            vocabularyParts.vocab(),
            vocabularyParts.merges(),
            vocabularyParts.scores(),
            true
        );

        client.execute(PutTrainedModelVocabularyAction.INSTANCE, request, listener);
    }

    private void indexPart(int partIndex, int totalParts, long totalSize, BytesArray bytes, ActionListener<AcknowledgedResponse> listener) {
        PutTrainedModelDefinitionPartAction.Request modelPartRequest = new PutTrainedModelDefinitionPartAction.Request(
            modelId,
            bytes,
            partIndex,
            totalSize,
            totalParts,
            true
        );

        client.execute(PutTrainedModelDefinitionPartAction.INSTANCE, modelPartRequest, listener);
    }

    private void checkDownloadComplete(ModelLoaderUtils.InputStreamChunker chunkIterator) {
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

        logger.debug(format("finished importing model [%s] using [%d] parts", modelId, chunkIterator.getTotalParts()));
    }
}
