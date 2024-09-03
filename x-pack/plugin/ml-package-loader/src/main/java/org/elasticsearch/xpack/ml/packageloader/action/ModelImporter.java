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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
public class ModelImporter {
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1MB
    public static final int MAX_IN_FLIGHT_REQUESTS = 5;
    private static final Logger logger = LogManager.getLogger(ModelImporter.class);
    private final Client client;
    private final String modelId;
    private final ModelPackageConfig config;
    private final ModelDownloadTask task;
    private final ExecutorService executorService;
    private final AtomicBoolean listenerIsClosed = new AtomicBoolean(false);
    private final AtomicInteger progressCounter = new AtomicInteger();
    private final URI uri;

    ModelImporter(Client client, String modelId, ModelPackageConfig packageConfig, ModelDownloadTask task, ThreadPool threadPool)
        throws URISyntaxException {
        this.client = client;
        this.modelId = Objects.requireNonNull(modelId);
        this.config = Objects.requireNonNull(packageConfig);
        this.task = Objects.requireNonNull(task);
        this.executorService = threadPool.executor(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME);
        this.uri = ModelLoaderUtils.resolvePackageLocation(
            config.getModelRepository(),
            config.getPackagedModelId() + ModelLoaderUtils.MODEL_FILE_EXTENSION
        );
    }

    public void doImport(ActionListener<AcknowledgedResponse> listener) {
        // todo file import
        executorService.execute(() -> doImportInternal(listener));
    }

    private void doImportInternal(ActionListener<AcknowledgedResponse> finalListener) {
        assert ThreadPool.assertCurrentThreadPool(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME)
            : format(
                "Model download must execute from [%s] but thread is [%s]",
                MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME,
                Thread.currentThread().getName()
            );

        ModelLoaderUtils.VocabularyParts vocabularyParts = null;

        try {
            if (config.getVocabularyFile() != null) {
                vocabularyParts = ModelLoaderUtils.loadVocabulary(
                    ModelLoaderUtils.resolvePackageLocation(config.getModelRepository(), config.getVocabularyFile())
                );
            }
        } catch (Exception e) {
            finalListener.onFailure(e);
            return;
        }

        downloadModelDefinition(config.getSize(), vocabularyParts, finalListener);
    }

    void downloadModelDefinition(
        long size,
        @Nullable ModelLoaderUtils.VocabularyParts vocabularyParts,
        ActionListener<AcknowledgedResponse> finalListener
    ) {
        // simple round up
        int totalParts = (int) ((size + DEFAULT_CHUNK_SIZE - 1) / DEFAULT_CHUNK_SIZE);
        var ranges = ModelLoaderUtils.split(size, MAX_IN_FLIGHT_REQUESTS, DEFAULT_CHUNK_SIZE);

        var downloaders = new ArrayList<ModelLoaderUtils.HttStreamChunker>();
        for (var range : ranges) {
            downloaders.add(new ModelLoaderUtils.HttStreamChunker(uri, range, DEFAULT_CHUNK_SIZE));
        }

        try (var countingListener = new RefCountingListener(1, ActionListener.wrap(ignore -> executorService.execute(() -> {
            var finalDownloader = downloaders.get(downloaders.size() - 1);
            downloadFinalPart(size, totalParts, finalDownloader, finalListener.delegateFailureAndWrap((l, r) -> {
                checkDownloadComplete(downloaders);
                l.onResponse(AcknowledgedResponse.TRUE);
            }));
        }), finalListener::onFailure))) {
            // Uploading other artefacts of the model first, that way the model is last and a simple search can be used to check if the
            // download is complete
            if (vocabularyParts != null) {
                uploadVocabulary(vocabularyParts, countingListener.acquire(r -> {
                    logger.info(() -> format("[%s] imported model vocabulary [%s]", modelId, config.getVocabularyFile()));
                }));
            }

            for (int streamSplit = 0; streamSplit < MAX_IN_FLIGHT_REQUESTS; ++streamSplit) {
                final var downloader = downloaders.get(streamSplit);
                var doLast = countingListener.acquire();
                executorService.execute(() -> downloadPartsInRange(size, totalParts, downloader, countingListener, doLast));
            }
        }
    }

    private void downloadPartsInRange(
        long size,
        int totalParts,
        ModelLoaderUtils.HttStreamChunker downloadChunker,
        RefCountingListener countingListener,
        ActionListener<Void> doLast
    ) {
        assert ThreadPool.assertCurrentThreadPool(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME)
            : format(
                "Model download must execute from [%s] but thread is [%s]",
                MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME,
                Thread.currentThread().getName()
            );

        while (downloadChunker.hasNext()) {
            if (countingListener.isFailing()) {
                if (listenerIsClosed.compareAndSet(false, true)) {
                    logger.info("is failing");
                    countingListener.close();
                }
                return;
            }

            if (task.isCancelled()) {
                logger.info("task cancelled");
                throw new TaskCancelledException(format("task cancelled with reason [%s]", task.getReasonCancelled()));
            }

            try {
                var bytesAndIndex = downloadChunker.next();
                task.setProgress(totalParts, progressCounter.getAndIncrement());
                logger.info("Progress " + progressCounter.get() + " , " + totalParts);

                indexPart(bytesAndIndex.partIndex(), totalParts, size, bytesAndIndex.bytes(), countingListener.acquire(ack -> {}));
            } catch (Exception e) {
                logger.info("errr", e);
                countingListener.acquire().onFailure(e);
                if (listenerIsClosed.compareAndSet(false, true)) {
                    countingListener.close();
                }
            }
        }

        logger.info("split complete " + downloadChunker.getCurrentPart());
        doLast.onResponse(null);
    }

    private void downloadFinalPart(
        long size,
        int totalParts,
        ModelLoaderUtils.HttStreamChunker downloader,
        ActionListener<AcknowledgedResponse> lastPartWrittenListener
    ) {
        assert ThreadPool.assertCurrentThreadPool(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME)
            : format(
                "Model download must execute from [%s] but thread is [%s]",
                MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME,
                Thread.currentThread().getName()
            );

        logger.info("final part");

        try {
            var bytesAndIndex = downloader.next();
            task.setProgress(totalParts, progressCounter.getAndIncrement());

            indexPart(bytesAndIndex.partIndex(), totalParts, size, bytesAndIndex.bytes(), lastPartWrittenListener);
        } catch (Exception e) {
            lastPartWrittenListener.onFailure(e);
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

    private void checkDownloadComplete(List<ModelLoaderUtils.HttStreamChunker> downloaders) {
        // if (config.getSha256().equals(chunkIterator.getSha256()) == false) {
        // String message = format(
        // "Model sha256 checksums do not match, expected [%s] but got [%s]",
        // config.getSha256(),
        // chunkIterator.getSha256()
        // );
        //
        // throw new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR);
        // }

        long readSize = downloaders.stream().mapToLong(ModelLoaderUtils.HttStreamChunker::getTotalBytesRead).sum();

        if (config.getSize() != readSize) {
            String message = format("Model size does not match, expected [%d] but got [%d]", config.getSize(), readSize);
            throw new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR);
        }

        int totalParts = downloaders.stream().mapToInt(ModelLoaderUtils.HttStreamChunker::getCurrentPart).sum();

        logger.debug(format("finished importing model [%s] using [%d] parts", modelId, totalParts));
    }
}
