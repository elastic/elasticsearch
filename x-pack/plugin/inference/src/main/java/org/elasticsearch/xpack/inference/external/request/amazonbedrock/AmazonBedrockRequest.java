/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock;

import org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockInferenceClient;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.net.URI;

public abstract class AmazonBedrockRequest implements Request {

    protected final AmazonBedrockModel amazonBedrockModel;

    protected AmazonBedrockRequest(AmazonBedrockModel model) {
        this.amazonBedrockModel = model;
    }

    public abstract void executeRequest(AmazonBedrockInferenceClient client);

    public AmazonBedrockModel model() {
        return amazonBedrockModel;
    }

    /**
     * Amazon Bedrock uses the AWS SDK, and will not create its own Http Request
     * @return null
     */
    @Override
    public final HttpRequest createHttpRequest() {
        return null;
    }

    /**
     * Amazon Bedrock uses the AWS SDK, and will not create its own URI
     * @return null
     */
    @Override
    public final URI getURI() {
        return null;
    }

    /**
     * Should be overridden for text embeddings requests
     * @return null
     */
    @Override
    public Request truncate() {
        return null;
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
}
