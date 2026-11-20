/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectImage;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectImage.ContentObjectImageUrl;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectText;
import org.elasticsearch.inference.completion.ContentObjects;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.validation.ServiceIntegrationValidator;

import java.util.List;

import static org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionCreator.USER_ROLE;

/**
 * Validates a chat completion model that requires image input (for example, an OCR model) by sending a user message containing both
 * text and a minimal image. A text-only request is rejected by these models.
 */
public class ImageChatCompletionServiceIntegrationValidator implements ServiceIntegrationValidator {
    private static final String TEST_INPUT = "how big";
    // A 1x1 pixel PNG
    private static final String TEST_IMAGE_DATA_URL =
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
    private static final UnifiedCompletionRequest TEST_REQUEST = UnifiedCompletionRequest.streaming(
        UnifiedCompletionRequestBody.of(
            List.of(
                new Message(
                    new ContentObjects(
                        List.of(
                            new ContentObjectText(TEST_INPUT),
                            new ContentObjectImage(new ContentObjectImageUrl(TEST_IMAGE_DATA_URL, null))
                        )
                    ),
                    USER_ROLE,
                    null,
                    null,
                    null,
                    null
                )
            )
        )
    );

    @Override
    public void validate(InferenceService service, Model model, TimeValue timeout, ActionListener<InferenceServiceResults> listener) {
        service.unifiedCompletionInfer(model, TEST_REQUEST, timeout, ServiceIntegrationValidator.wrapListenerForValidation(listener));
    }
}
