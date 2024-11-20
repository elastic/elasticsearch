/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.remote;

import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultTaskSettings;

import java.util.Map;

public interface RemoteInferenceIntegration {

    /* Integration points for reading/writing model from storage */
    DefaultSecretSettings parseSecretSettings(Map<String, Object> config);

    DefaultServiceSettings parseServiceSettings(Map<String, Object> config);

    DefaultTaskSettings parseTaskSettings(Map<String, Object> config);

    /* Integration points for batching behavior */
    int maxNumberOfInputsPerBatch(RemoteInferenceModel model);

    EmbeddingRequestChunker.EmbeddingType embeddingType(RemoteInferenceModel model);

    /* Integration points for converting to/from provider interface */
    RemoteInferenceRequest parseInputs(RemoteInferenceModel model, InferenceInputs inputs, Map<String, Object> taskSettings);

    ResponseHandler responseHandler();
}
