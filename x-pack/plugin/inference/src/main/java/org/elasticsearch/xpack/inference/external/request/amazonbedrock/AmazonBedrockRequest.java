/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockBaseClient;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.net.URI;

public abstract class AmazonBedrockRequest implements Request {

    protected final AmazonBedrockModel amazonBedrockModel;
    protected final String inferenceId;
    protected final TimeValue timeout;

    protected AmazonBedrockRequest(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        this.amazonBedrockModel = model;
        this.inferenceId = model.getInferenceEntityId();
        this.timeout = timeout;
    }

    protected abstract void executeRequest(AmazonBedrockBaseClient client);

    public AmazonBedrockModel model() {
        return amazonBedrockModel;
    }

    /**
     * Amazon Bedrock uses the AWS SDK, and will not create its own Http Request
     * But, this is needed for the ExecutableInferenceRequest to get the inferenceEntityId
     * @return NoOp request
     */
    @Override
    public final HttpRequest createHttpRequest() {
        return new HttpRequest(new NoOpHttpRequest(), inferenceId);
    }

    /**
     * Amazon Bedrock uses the AWS SDK, and will not create its own URI
     * @return null
     */
    @Override
    public final URI getURI() {
        throw new UnsupportedOperationException();
    }

    /**
     * Should be overridden for text embeddings requests
     * @return null
     */
    @Override
    public Request truncate() {
        return this;
    }

    /**
     * Should be overridden for text embeddings requests
     * @return boolean[0]
     */
    @Override
    public boolean[] getTruncationInfo() {
        return new boolean[0];
    }

    @Override
    public String getInferenceEntityId() {
        return amazonBedrockModel.getInferenceEntityId();
    }

    public TimeValue timeout() {
        return timeout;
    }

    public abstract TaskType taskType();
}
