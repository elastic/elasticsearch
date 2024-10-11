/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;

public interface AmazonBedrockConverseRequestEntity {
    ConverseRequest.Builder addMessages(ConverseRequest.Builder request);

    ConverseRequest.Builder addInferenceConfig(ConverseRequest.Builder request);

    ConverseRequest.Builder addAdditionalModelFields(ConverseRequest.Builder request);
}
