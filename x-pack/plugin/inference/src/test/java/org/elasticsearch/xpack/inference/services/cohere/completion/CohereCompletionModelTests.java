/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CohereCompletionModelTests extends ESTestCase {

    public void testCreateModel_AlwaysWithEmptyTaskSettings() {
        var model = new CohereCompletionModel(
            "inference_id",
            new HashMap<>(Map.of("model_id", "cohere completion model")),
            null,
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(model.getTaskSettings(), is(EmptyTaskSettings.INSTANCE));
        assertThat(model.getServiceSettings().modelId(), is("cohere completion model"));
    }

    public static CohereCompletionModel createModel(String url, String apiKey, @Nullable String model) {
        return new CohereCompletionModel(
            "id",
            new CohereCompletionServiceSettings(url, model, null),
            EmptyTaskSettings.INSTANCE,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

}
