/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings;

import com.amazonaws.services.bedrockruntime.model.InvokeModelRequest;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockInferenceClient;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class AmazonBedrockEmbeddingsRequest extends AmazonBedrockRequest {
    private final AmazonBedrockEmbeddingsModel embeddingsModel;
    private final ToXContent requestEntity;
    private InvokeModelResult result;
    private AmazonBedrockProvider provider;

    public AmazonBedrockEmbeddingsRequest(AmazonBedrockEmbeddingsModel model, ToXContent requestEntity) {
        super(model);
        this.embeddingsModel = model;
        this.provider = model.provider();
        this.requestEntity = requestEntity;
    }

    public InvokeModelResult result() {
        return result;
    }

    public AmazonBedrockProvider provider() {
        return provider;
    }

    @Override
    public void executeRequest(AmazonBedrockInferenceClient client) {
        try {
            var builder = requestEntity.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
            builder.close();

            BytesReference bodyBytes;
            OutputStream stream = builder.getOutputStream();
            if (stream instanceof ByteArrayOutputStream) {
                bodyBytes = new BytesArray(((ByteArrayOutputStream) stream).toByteArray());
            } else {
                bodyBytes = ((BytesStream) stream).bytes();
            }

            var charset = StandardCharsets.UTF_8;
            var bodyBuffer = charset.encode(bodyBytes.toString());

            var invokeModelRequest = new InvokeModelRequest().withModelId(embeddingsModel.model()).withBody(bodyBuffer);

            result = SocketAccess.doPrivileged(() -> client.invokeModel(invokeModelRequest));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
