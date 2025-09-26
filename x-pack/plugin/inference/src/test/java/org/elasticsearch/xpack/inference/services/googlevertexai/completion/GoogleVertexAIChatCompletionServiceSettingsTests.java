/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleModelGardenProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.googlevertexai.request.GoogleVertexAiUtils.ML_INFERENCE_GOOGLE_MODEL_GARDEN_ADDED;

public class GoogleVertexAIChatCompletionServiceSettingsTests extends InferenceSettingsTestCase<
    GoogleVertexAiChatCompletionServiceSettings> {
    @Override
    protected Writeable.Reader<GoogleVertexAiChatCompletionServiceSettings> instanceReader() {
        return GoogleVertexAiChatCompletionServiceSettings::new;
    }

    @Override
    protected GoogleVertexAiChatCompletionServiceSettings fromMutableMap(Map<String, Object> mutableMap) {
        return GoogleVertexAiChatCompletionServiceSettings.fromMap(mutableMap, ConfigurationParseContext.PERSISTENT);

    }

    @Override
    protected GoogleVertexAiChatCompletionServiceSettings mutateInstanceForVersion(
        GoogleVertexAiChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(ML_INFERENCE_GOOGLE_MODEL_GARDEN_ADDED)) {
            return instance;
        } else {
            return new GoogleVertexAiChatCompletionServiceSettings(
                instance.projectId(),
                instance.location(),
                instance.modelId(),
                null,
                null,
                null,
                instance.rateLimitSettings()
            );
        }
    }

    @Override
    protected GoogleVertexAiChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    private static GoogleVertexAiChatCompletionServiceSettings createRandom() {
        return new GoogleVertexAiChatCompletionServiceSettings(
            randomString(),
            randomString(),
            randomString(),
            createOptionalUri(randomOptionalString()),
            createOptionalUri(randomOptionalString()),
            randomFrom(GoogleModelGardenProvider.ANTHROPIC, null),
            new RateLimitSettings(randomIntBetween(1, 1000))
        );
    }
}
