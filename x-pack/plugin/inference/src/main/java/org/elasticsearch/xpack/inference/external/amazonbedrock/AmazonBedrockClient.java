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
import org.elasticsearch.core.Releasable;
import org.joda.time.Instant;

public interface AmazonBedrockClient extends Releasable {
    ConverseResult converse(ConverseRequest converseRequest) throws ElasticsearchException;

    InvokeModelResult invokeModel(InvokeModelRequest invokeModelRequest) throws ElasticsearchException;

    boolean isExpired(Instant currentTimestampMs);

    boolean tryToIncreaseReference();
}
