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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.ml.action.AuditMlNotificationAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction.Request;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

/**
 * A helper class for abstracting out the use of the ModelLoaderUtils to make dependency injection testing easier.
 */
class ModelUploader {
    private static final int DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB
    private static final Logger logger = LogManager.getLogger(ModelUploader.class);
    private final Client client;
    private final String modelId;
    private final ModelPackageConfig config;

    ModelUploader(Client client, Request request) {
        this.client = client;
        this.modelId = request.getModelId();
        this.config = request.getModelPackageConfig();
    }

    public void upload() throws URISyntaxException, IOException, ElasticsearchStatusException {
        long size = config.getSize();

        // Uploading other artefacts of the model first, that way the model is last and a simple search can be used to check if the
        // download is complete
        if (Strings.isNullOrEmpty(config.getVocabularyFile()) == false) {
            uploadVocabulary();

            writeDebugNotification(modelId, format("uploaded model vocabulary [%s]", config.getVocabularyFile()));
        }

        URI uri = ModelLoaderUtils.resolvePackageLocation(
            config.getModelRepository(),
            config.getPackagedModelId() + ModelLoaderUtils.MODEL_FILE_EXTENSION
        );

        InputStream modelInputStream = ModelLoaderUtils.getInputStreamFromModelRepository(uri);

        ModelLoaderUtils.InputStreamChunker chunkIterator = new ModelLoaderUtils.InputStreamChunker(modelInputStream, DEFAULT_CHUNK_SIZE);

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

        PutTrainedModelDefinitionPartAction.Request r = new PutTrainedModelDefinitionPartAction.Request(
            modelId,
            definition,
            totalParts - 1,
            size,
            totalParts
        );

        client.execute(PutTrainedModelDefinitionPartAction.INSTANCE, r).actionGet();
        logger.debug(format("finished uploading model [%s] using [%d] parts", modelId, totalParts));
    }

    private void uploadVocabulary() throws URISyntaxException {
        Tuple<List<String>, List<String>> vocabularyAndMerges = ModelLoaderUtils.loadVocabulary(
            ModelLoaderUtils.resolvePackageLocation(config.getModelRepository(), config.getVocabularyFile())
        );

        PutTrainedModelVocabularyAction.Request r2 = new PutTrainedModelVocabularyAction.Request(
            modelId,
            vocabularyAndMerges.v1(),
            vocabularyAndMerges.v2(),
            List.of()
        );

        client.execute(PutTrainedModelVocabularyAction.INSTANCE, r2).actionGet();
    }

    private void writeDebugNotification(String modelId, String message) {
        client.execute(
            AuditMlNotificationAction.INSTANCE,
            new AuditMlNotificationAction.Request(AuditMlNotificationAction.AuditType.INFERENCE, modelId, message, Level.INFO),
            ActionListener.noop()
        );

        logger.debug(() -> format("[%s] %s", modelId, message));
    }
}
