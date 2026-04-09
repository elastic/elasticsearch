/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class GoogleAiStudioCompletionModelTests extends ESTestCase {

    public void testCreateModel_AlwaysWithEmptyTaskSettings() {
        var model = new GoogleAiStudioCompletionModel(
            "inference entity id",
            TaskType.COMPLETION,
            "service",
            new HashMap<>(Map.of("model_id", "model")),
            new HashMap<>(Map.of()),
            null,
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(model.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
    }

    public void testBuildUri() throws URISyntaxException {
        assertThat(
            GoogleAiStudioCompletionModel.buildUri("model").toString(),
            is("https://generativelanguage.googleapis.com/v1/models/model:generateContent")
        );
    }

    public static GoogleAiStudioCompletionModel createModel(String model, String apiKey) {
        return new GoogleAiStudioCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            new GoogleAiStudioCompletionServiceSettings(model, null),
            EmptyTaskSettings.INSTANCE,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static GoogleAiStudioCompletionModel createModel(String model, String url, String apiKey) {
        var googleModel = spy(
            new GoogleAiStudioCompletionModel(
                "id",
                TaskType.COMPLETION,
                "service",
                new GoogleAiStudioCompletionServiceSettings(model, null),
                EmptyTaskSettings.INSTANCE,
                new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
            )
        );
        when(googleModel.uri(anyBoolean())).thenReturn(URI.create(url));
        return googleModel;
    }
}
