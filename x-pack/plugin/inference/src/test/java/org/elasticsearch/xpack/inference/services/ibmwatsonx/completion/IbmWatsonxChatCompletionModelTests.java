/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class IbmWatsonxChatCompletionModelTests extends ESTestCase {
    private static final URI TEST_URI = URI.create("abc.com");

    public static IbmWatsonxChatCompletionModel createModel(URI uri, String apiVersion, String modelId, String projectId, String apiKey)
        throws URISyntaxException {
        return new IbmWatsonxChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            new IbmWatsonxChatCompletionServiceSettings(uri, apiVersion, modelId, projectId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesExistingModelId() throws URISyntaxException {
        var model = createModel(TEST_URI, "apiVersion", "modelId", "projectId", "apiKey");
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            "different_model",
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = IbmWatsonxChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("different_model"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesNullModelId() throws URISyntaxException {
        var model = createModel(TEST_URI, "apiVersion", null, "projectId", "apiKey");
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            "different_model",
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = IbmWatsonxChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("different_model"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_KeepsNullIfNoModelIdProvided() throws URISyntaxException {
        var model = createModel(TEST_URI, "apiVersion", null, "projectId", "apiKey");
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = IbmWatsonxChatCompletionModel.of(model, request);

        assertNull(overriddenModel.getServiceSettings().modelId());
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() throws URISyntaxException {
        var model = createModel(TEST_URI, "apiVersion", "modelId", "projectId", "apiKey");
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            null, // not overriding model
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = IbmWatsonxChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("modelId"));
    }
}
