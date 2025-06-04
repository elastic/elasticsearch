/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.ml.packageloader.MachineLearningPackageLoader;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.core.Strings.format;

/**
 * For downloading and the vocabulary and model definition file and
 * indexing those files in Elasticsearch.
 * Holding the large model definition file in memory will consume
 * too much memory, instead it is streamed in chunks and each chunk
 * written to the index in a non-blocking request.
 * The model files may be installed from a local file or download
 * from a server. The server download uses {@link #NUMBER_OF_STREAMS}
 * connections each using the Range header to split the stream by byte
 * range. There is a complication in that the final part of the model
 * definition must be uploaded last as writing this part causes an index
 * refresh.
 * When read from file a single thread is used to read the file
 * stream, split into chunks and index those chunks.
 */
public class ModelImporter {
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1MB
    public static final int NUMBER_OF_STREAMS = 5;
    private static final Logger logger = LogManager.getLogger(ModelImporter.class);
    private final Client client;
    private final String modelId;
    private final ModelPackageConfig config;
    private final ModelDownloadTask task;
    private final ExecutorService executorService;
    private final AtomicInteger progressCounter = new AtomicInteger();
    private final URI uri;
    private final CircuitBreakerService breakerService;

    ModelImporter(
        Client client,
        String modelId,
        ModelPackageConfig packageConfig,
        ModelDownloadTask task,
        ThreadPool threadPool,
        CircuitBreakerService cbs
    ) throws URISyntaxException {
        this.client = client;
        this.modelId = Objects.requireNonNull(modelId);
        this.config = Objects.requireNonNull(packageConfig);
        this.task = Objects.requireNonNull(task);
        this.executorService = threadPool.executor(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME);
        this.uri = ModelLoaderUtils.resolvePackageLocation(
            config.getModelRepository(),
            config.getPackagedModelId() + ModelLoaderUtils.MODEL_FILE_EXTENSION
        );
        this.breakerService = cbs;
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

        ModelLoaderUtils.VocabularyParts vocabularyParts = null;
        try {
            if (config.getVocabularyFile() != null) {
                vocabularyParts = ModelLoaderUtils.loadVocabulary(
                    ModelLoaderUtils.resolvePackageLocation(config.getModelRepository(), config.getVocabularyFile())
                );
            }

            // simple round up
            int totalParts = (int) ((config.getSize() + DEFAULT_CHUNK_SIZE - 1) / DEFAULT_CHUNK_SIZE);

            if (ModelLoaderUtils.uriIsFile(uri) == false) {
                breakerService.getBreaker(CircuitBreaker.REQUEST)
                    .addEstimateBytesAndMaybeBreak(DEFAULT_CHUNK_SIZE * NUMBER_OF_STREAMS, "model importer");
                var breakerFreeingListener = ActionListener.runAfter(
                    finalListener,
                    () -> breakerService.getBreaker(CircuitBreaker.REQUEST).addWithoutBreaking(-(DEFAULT_CHUNK_SIZE * NUMBER_OF_STREAMS))
                );

                var ranges = ModelLoaderUtils.split(config.getSize(), NUMBER_OF_STREAMS, DEFAULT_CHUNK_SIZE);
                var downloaders = new ArrayList<ModelLoaderUtils.HttpStreamChunker>(ranges.size());
                for (var range : ranges) {
                    downloaders.add(new ModelLoaderUtils.HttpStreamChunker(uri, range, DEFAULT_CHUNK_SIZE));
                }
                downloadModelDefinition(config.getSize(), totalParts, vocabularyParts, downloaders, breakerFreeingListener);
            } else {
                InputStream modelInputStream = ModelLoaderUtils.getFileInputStream(uri);
                ModelLoaderUtils.InputStreamChunker chunkIterator = new ModelLoaderUtils.InputStreamChunker(
                    modelInputStream,
                    DEFAULT_CHUNK_SIZE
                );
                readModelDefinitionFromFile(config.getSize(), totalParts, chunkIterator, vocabularyParts, finalListener);
            }
        } catch (Exception e) {
            finalListener.onFailure(e);
        }
    }

    void downloadModelDefinition(
        long size,
        int totalParts,
        @Nullable ModelLoaderUtils.VocabularyParts vocabularyParts,
        List<ModelLoaderUtils.HttpStreamChunker> downloaders,
        ActionListener<AcknowledgedResponse> finalListener
    ) {
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
                uploadVocabulary(vocabularyParts, countingListener);
            }

            // Download all but the final split.
            // The final split is a single chunk
            for (int streamSplit = 0; streamSplit < downloaders.size() - 1; ++streamSplit) {
                final var downloader = downloaders.get(streamSplit);
                var rangeDownloadedListener = countingListener.acquire(); // acquire to keep the counting listener from closing
                executorService.execute(
                    () -> downloadPartInRange(size, totalParts, downloader, executorService, countingListener, rangeDownloadedListener)
                );
            }
        }
    }

    private void downloadPartInRange(
        long size,
        int totalParts,
        ModelLoaderUtils.HttpStreamChunker downloadChunker,
        ExecutorService executorService,
        RefCountingListener countingListener,
        ActionListener<Void> rangeFullyDownloadedListener
    ) {
        assert ThreadPool.assertCurrentThreadPool(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME)
            : format(
                "Model download must execute from [%s] but thread is [%s]",
                MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME,
                Thread.currentThread().getName()
            );

        if (countingListener.isFailing()) {
            rangeFullyDownloadedListener.onResponse(null); // the error has already been reported elsewhere
            return;
        }

        try {
            throwIfTaskCancelled();
            var bytesAndIndex = downloadChunker.next();
            task.setProgress(totalParts, progressCounter.getAndIncrement());

            indexPart(bytesAndIndex.partIndex(), totalParts, size, bytesAndIndex.bytes());
        } catch (Exception e) {
            rangeFullyDownloadedListener.onFailure(e);
            return;
        }

        if (downloadChunker.hasNext()) {
            executorService.execute(
                () -> downloadPartInRange(
                    size,
                    totalParts,
                    downloadChunker,
                    executorService,
                    countingListener,
                    rangeFullyDownloadedListener
                )
            );
        } else {
            rangeFullyDownloadedListener.onResponse(null);
        }
    }

    private void downloadFinalPart(
        long size,
        int totalParts,
        ModelLoaderUtils.HttpStreamChunker downloader,
        ActionListener<AcknowledgedResponse> lastPartWrittenListener
    ) {
        assert ThreadPool.assertCurrentThreadPool(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME)
            : format(
                "Model download must execute from [%s] but thread is [%s]",
                MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME,
                Thread.currentThread().getName()
            );

        try {
            var bytesAndIndex = downloader.next();
            task.setProgress(totalParts, progressCounter.getAndIncrement());

            indexPart(bytesAndIndex.partIndex(), totalParts, size, bytesAndIndex.bytes());
            lastPartWrittenListener.onResponse(AcknowledgedResponse.TRUE);
        } catch (Exception e) {
            lastPartWrittenListener.onFailure(e);
        }
    }

    void readModelDefinitionFromFile(
        long size,
        int totalParts,
        ModelLoaderUtils.InputStreamChunker chunkIterator,
        @Nullable ModelLoaderUtils.VocabularyParts vocabularyParts,
        ActionListener<AcknowledgedResponse> finalListener
    ) {
        try (var countingListener = new RefCountingListener(1, ActionListener.wrap(ignore -> executorService.execute(() -> {
            finalListener.onResponse(AcknowledgedResponse.TRUE);
        }), finalListener::onFailure))) {
            try {
                if (vocabularyParts != null) {
                    uploadVocabulary(vocabularyParts, countingListener);
                }

                for (int part = 0; part < totalParts; ++part) {
                    throwIfTaskCancelled();
                    task.setProgress(totalParts, part);
                    BytesArray definition = chunkIterator.next();
                    indexPart(part, totalParts, size, definition);
                }
                task.setProgress(totalParts, totalParts);

                checkDownloadComplete(chunkIterator, totalParts);
            } catch (Exception e) {
                countingListener.acquire().onFailure(e);
            }
        }
    }

    private void uploadVocabulary(ModelLoaderUtils.VocabularyParts vocabularyParts, RefCountingListener countingListener) {
        PutTrainedModelVocabularyAction.Request request = new PutTrainedModelVocabularyAction.Request(
            modelId,
            vocabularyParts.vocab(),
            vocabularyParts.merges(),
            vocabularyParts.scores(),
            true
        );

        client.execute(PutTrainedModelVocabularyAction.INSTANCE, request, countingListener.acquire(r -> {
            logger.debug(() -> format("[%s] imported model vocabulary [%s]", modelId, config.getVocabularyFile()));
        }));
    }

    private void indexPart(int partIndex, int totalParts, long totalSize, BytesArray bytes) {
        PutTrainedModelDefinitionPartAction.Request modelPartRequest = new PutTrainedModelDefinitionPartAction.Request(
            modelId,
            bytes,
            partIndex,
            totalSize,
            totalParts,
            true
        );

        client.execute(PutTrainedModelDefinitionPartAction.INSTANCE, modelPartRequest).actionGet();
    }

    private void checkDownloadComplete(List<ModelLoaderUtils.HttpStreamChunker> downloaders) {
        long totalBytesRead = downloaders.stream().mapToLong(ModelLoaderUtils.HttpStreamChunker::getTotalBytesRead).sum();
        int totalParts = downloaders.stream().mapToInt(ModelLoaderUtils.HttpStreamChunker::getCurrentPart).sum();
        checkSize(totalBytesRead);
        logger.debug(format("finished importing model [%s] using [%d] parts", modelId, totalParts));
    }

    private void checkDownloadComplete(ModelLoaderUtils.InputStreamChunker fileInputStream, int totalParts) {
        checkSha256(fileInputStream.getSha256());
        checkSize(fileInputStream.getTotalBytesRead());
        logger.debug(format("finished importing model [%s] using [%d] parts", modelId, totalParts));
    }

    private void checkSha256(String sha256) {
        if (config.getSha256().equals(sha256) == false) {
            String message = format("Model sha256 checksums do not match, expected [%s] but got [%s]", config.getSha256(), sha256);

            throw new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void checkSize(long definitionSize) {
        if (config.getSize() != definitionSize) {
            String message = format("Model size does not match, expected [%d] but got [%d]", config.getSize(), definitionSize);
            throw new ElasticsearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void throwIfTaskCancelled() {
        if (task.isCancelled()) {
            logger.info("Model [{}] download task cancelled", modelId);
            throw new TaskCancelledException(
                format("Model [%s] download task cancelled with reason [%s]", modelId, task.getReasonCancelled())
            );
        }
    }
}
