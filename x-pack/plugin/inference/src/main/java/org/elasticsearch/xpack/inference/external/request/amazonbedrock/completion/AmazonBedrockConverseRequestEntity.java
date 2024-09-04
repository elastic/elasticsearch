/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import com.amazonaws.services.bedrockruntime.model.ConverseRequest;

public interface AmazonBedrockConverseRequestEntity {
    ConverseRequest addMessages(ConverseRequest request);

    ConverseRequest addInferenceConfig(ConverseRequest request);

    ConverseRequest addAdditionalModelFields(ConverseRequest request);
}
