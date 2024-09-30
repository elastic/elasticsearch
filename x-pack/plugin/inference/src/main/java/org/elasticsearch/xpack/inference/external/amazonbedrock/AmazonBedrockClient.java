/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import com.amazonaws.services.bedrockruntime.model.ConverseRequest;
import com.amazonaws.services.bedrockruntime.model.ConverseResult;
import com.amazonaws.services.bedrockruntime.model.InvokeModelRequest;
import com.amazonaws.services.bedrockruntime.model.InvokeModelResult;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;

import java.time.Instant;

public interface AmazonBedrockClient {
    void converse(ConverseRequest converseRequest, ActionListener<ConverseResult> responseListener) throws ElasticsearchException;

    void invokeModel(InvokeModelRequest invokeModelRequest, ActionListener<InvokeModelResult> responseListener)
        throws ElasticsearchException;

    boolean isExpired(Instant currentTimestampMs);

    void resetExpiration();
}
