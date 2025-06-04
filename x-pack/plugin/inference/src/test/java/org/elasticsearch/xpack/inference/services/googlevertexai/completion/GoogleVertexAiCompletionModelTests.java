/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils.GENERATE_CONTENT;
import static org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils.STREAM_GENERATE_CONTENT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiCompletionModelTests extends ESTestCase {

    private static final String DEFAULT_PROJECT_ID = "test-project";
    private static final String DEFAULT_LOCATION = "us-central1";
    private static final String DEFAULT_MODEL_ID = "gemini-pro";

    public void testCreateModel() throws URISyntaxException {
        var model = createCompletionModel(DEFAULT_PROJECT_ID, DEFAULT_LOCATION, DEFAULT_MODEL_ID);
        URI expectedUri = new URI(
            Strings.format(
                "https://%s-aiplatform.googleapis.com/v1/projects/%s" + "/locations/global/publishers/google/models/%s:generateContent",
                DEFAULT_LOCATION,
                DEFAULT_PROJECT_ID,
                DEFAULT_MODEL_ID

            )
        );
        assertThat(model.uri(), is(expectedUri));
    }

    public void testUpdateUri() throws URISyntaxException {
        var model = createCompletionModel(DEFAULT_PROJECT_ID, DEFAULT_LOCATION, DEFAULT_MODEL_ID);
        assertThat(model.uri().toString(), containsString(GENERATE_CONTENT));
        model.updateUri(true);
        assertThat(model.uri().toString(), containsString(STREAM_GENERATE_CONTENT));
        model.updateUri(false);
        assertThat(model.uri().toString(), containsString(GENERATE_CONTENT));
    }

    private static GoogleVertexAiCompletionModel createCompletionModel(String projectId, String location, String modelId) {
        return new GoogleVertexAiCompletionModel(
            "google-vertex-ai-chat-test-id",
            TaskType.CHAT_COMPLETION,
            "google_vertex_ai",
            new HashMap<>(Map.of("project_id", projectId, "location", location, "model_id", modelId)),
            new HashMap<>(),
            new HashMap<>(Map.of("service_account_json", "{}")),
            ConfigurationParseContext.PERSISTENT
        );
    }
}
