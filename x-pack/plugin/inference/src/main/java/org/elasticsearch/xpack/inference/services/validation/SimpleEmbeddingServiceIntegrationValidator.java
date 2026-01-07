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
import org.elasticsearch.inference.InferenceString.DataFormat;
import org.elasticsearch.inference.InferenceString.DataType;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

public class SimpleEmbeddingServiceIntegrationValidator implements ServiceIntegrationValidator {
    // The below data URI represents the base64 encoding of 28x28 pixel black square .jpg image
    private static final String BASE64_IMAGE_DATA = "data:image/jpg;base64,/9j/4QDKRXhpZgAATU0AKgAAAAgABgESAAMAAAABAAEAAAEaAAUAAAABAAAAV"
        + "gEbAAUAAAABAAAAXgEoAAMAAAABAAIAAAITAAMAAAABAAEAAIdpAAQAAAABAAAAZgAAAAAAAABIAAAAAQAAAEgAAAABAAeQAAAHAAAABDAyMjGRAQAHAAAABAECAw"
        + "CgAAAHAAAABDAxMDCgAQADAAAAAQABAACgAgAEAAAAAQAAABygAwAEAAAAAQAAABykBgADAAAAAQAAAAAAAAAAAAD/2wCEABwcHBwcHDAcHDBEMDAwRFxEREREXHR"
        + "cXFxcXHSMdHR0dHR0jIyMjIyMjIyoqKioqKjExMTExNzc3Nzc3Nzc3NwBIiQkODQ4YDQ0YOacgJzm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm5ubm"
        + "5ubm5ubm5ubm5ubm5v/dAAQAAv/AABEIABwAHAMBIgACEQEDEQH/xAGiAAABBQEBAQEBAQAAAAAAAAAAAQIDBAUGBwgJCgsQAAIBAwMCBAMFBQQEAAABfQECAwAEE"
        + "QUSITFBBhNRYQcicRQygZGhCCNCscEVUtHwJDNicoIJChYXGBkaJSYnKCkqNDU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6g4SFhoeIiYqSk5"
        + "SVlpeYmZqio6Slpqeoqaqys7S1tre4ubrCw8TFxsfIycrS09TV1tfY2drh4uPk5ebn6Onq8fLz9PX29/j5+gEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoLEQA"
        + "CAQIEBAMEBwUEBAABAncAAQIDEQQFITEGEkFRB2FxEyIygQgUQpGhscEJIzNS8BVictEKFiQ04SXxFxgZGiYnKCkqNTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZn"
        + "aGlqc3R1dnd4eXqCg4SFhoeIiYqSk5SVlpeYmZqio6Slpqeoqaqys7S1tre4ubrCw8TFxsfIycrS09TV1tfY2dri4+Tl5ufo6ery8/T19vf4+fr/2gAMAwEAAhEDE"
        + "QA/AObooooAKKKKAP/Q5uiiigAooooA/9k=";

    static final InferenceStringGroup TEST_TEXT_INPUT = new InferenceStringGroup("text");
    static final InferenceStringGroup TEST_IMAGE_BASE64_INPUT = new InferenceStringGroup(
        new InferenceString(DataType.IMAGE, DataFormat.BASE64, BASE64_IMAGE_DATA)
    );

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        EmbeddingRequest request = new EmbeddingRequest(List.of(TEST_TEXT_INPUT, TEST_IMAGE_BASE64_INPUT), InputType.INTERNAL_INGEST);
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
