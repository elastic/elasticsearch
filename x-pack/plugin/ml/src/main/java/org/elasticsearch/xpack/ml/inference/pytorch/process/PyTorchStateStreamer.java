/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.pytorch.ModelStorage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * PyTorch models in the TorchScript format are binary files divided
 * into small chunks and base64 encoded for storage in Elasticsearch.
 * The model is restored by base64 decoding the stored state and streaming
 * the binary objects concatenated in order. There is no delineation between
 * individual chunks the state should appear as one contiguous file.
 */
public class PyTorchStateStreamer {

    private static final Logger logger = LogManager.getLogger(PyTorchStateStreamer.class);

    private final OriginSettingClient client;
    private final ByteBuffer lengthBuffer;
    private volatile boolean isCancelled;

    public PyTorchStateStreamer(Client client) {
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ML_ORIGIN);
        lengthBuffer = ByteBuffer.allocate(4);
    }

    /**
     * Cancels the state streaming at the first opportunity.
     */
    public void cancel() {
        isCancelled = true;
    }

    /**
     * Firsts writes the size of the model so the native process can
     * allocated memory then writes the chunks of binary state.
     *
     * @param index The index to read from
     * @param modelStorage  Metadata for the model state
     * @param restoreStream The stream to write to
     * @throws IOException
     */
    public void writeStateToStream(String index, ModelStorage modelStorage, OutputStream restoreStream) throws IOException {

        writeModelSize(modelStorage.getModelId(), modelStorage.getModelSize(), restoreStream);

        List<String> docIds = modelStorage.documentIds();
        for (String docId : docIds) {
            if (isCancelled) {
                return;
            }

            GetResponse got = client.prepareGet(index, docId)
                .setFetchSource(true)
                .setStoredFields(modelStorage.getFieldName())
                .get();

            if (got.isExists() == false) {
                String message = String.format(Locale.ROOT,
                    "missing state document [%s] for model [%s]", docId, modelStorage.getModelId());
                logger.error(message);
                throw new IllegalStateException(message);
            }

            logger.info("doc field: " + got.getField(modelStorage.getFieldName()));
            logger.info("response: " + got);

            writeBinaryData((String)got.getSource().get(modelStorage.getFieldName()), restoreStream);
        }

        logger.debug("model [{}] state restored from [{}] documents", modelStorage.getModelId(), docIds.size());
    }

    private void writeModelSize(String modelId, ByteSizeValue modelSize, OutputStream outputStream) throws IOException {
        if (modelSize.getBytes() <= 0) {
            // The other end expects an unsigned 32 bit int a -ve value is invalid.
            // ByteSizeValue allows -1 bytes as a valid value so this check is still required
            String message = String.format(Locale.ROOT,
                "The storage definition for model [%s] has a negative model size [%s]", modelId, modelSize.getBytes());
            logger.error(message);
            throw new IllegalStateException(message);
        }

        if (modelSize.getBytes() > Integer.MAX_VALUE) {
            // TODO use a long in case models are larger than 2^31 bytes
            String message = String.format(Locale.ROOT,
                "model [%s] has a size [%s] larger than the max size [%s]",
                modelId, modelSize.getBytes(), Integer.MAX_VALUE);
            logger.error(message);
            throw new IllegalStateException(message);
        }

        lengthBuffer.clear();
        lengthBuffer.putInt((int)modelSize.getBytes());
        outputStream.write(lengthBuffer.array());
    }

    private void writeBinaryData(String base64Encoded, OutputStream outputStream) throws IOException {
        assert base64Encoded != null;
        logger.info("encoded string: " + base64Encoded);

        byte[] rawBytes = Base64.getDecoder().decode(base64Encoded.getBytes(StandardCharsets.UTF_8));
        logger.info("bytes " + rawBytes[0]);
        outputStream.write(rawBytes);
    }
}
