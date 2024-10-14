/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;

import java.time.Instant;

public interface AmazonBedrockClient {
    void converse(ConverseRequest converseRequest, ActionListener<ConverseResponse> responseListener) throws ElasticsearchException;

    void invokeModel(InvokeModelRequest invokeModelRequest, ActionListener<InvokeModelResponse> responseListener)
        throws ElasticsearchException;

    boolean isExpired(Instant currentTimestampMs);

    void resetExpiration();
}
