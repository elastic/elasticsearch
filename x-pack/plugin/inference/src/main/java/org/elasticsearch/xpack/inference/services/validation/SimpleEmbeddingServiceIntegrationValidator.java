/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

public class SimpleEmbeddingServiceIntegrationValidator implements ServiceIntegrationValidator {
    static final InferenceStringGroup TEST_TEXT_INPUT = new InferenceStringGroup("text");
    // The below data URL represents the base64 encoding of a single black pixel
    static final InferenceStringGroup TEST_BASE64_INPUT = new InferenceStringGroup(
        new InferenceString(
            InferenceString.DataType.IMAGE_BASE64,
            "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVQImWNgYGAAAAAEAAGjChXjAAAAAElFTkSuQmCC"
        )
    );

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        EmbeddingRequest request = new EmbeddingRequest(List.of(TEST_TEXT_INPUT, TEST_BASE64_INPUT), InputType.INTERNAL_INGEST);
        service.embeddingInfer(model, request, timeout, ActionListener.wrap(r -> {
            if (r != null) {
                listener.onResponse(r);
            } else {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Could not complete inference endpoint creation as validation call to service returned null response.",
                        RestStatus.BAD_REQUEST
                    )
                );
            }
        }, e -> {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Could not complete inference endpoint creation as validation call to service threw an exception.",
                    RestStatus.BAD_REQUEST,
                    e
                )
            );
        }));
    }
}
