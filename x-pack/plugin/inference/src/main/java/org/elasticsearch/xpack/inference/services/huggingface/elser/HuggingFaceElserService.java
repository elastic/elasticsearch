/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceBaseService;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;

import java.util.Map;

public class HuggingFaceElserService extends HuggingFaceBaseService {
    public static final String NAME = "hugging_face_elser";

    public HuggingFaceElserService(SetOnce<HttpRequestSenderFactory> factory, SetOnce<ServiceComponents> serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected HuggingFaceModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage
    ) {
        return switch (taskType) {
            case SPARSE_EMBEDDING -> new HuggingFaceElserModel(inferenceEntityId, taskType, NAME, serviceSettings, secretSettings);
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        };
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }
}
