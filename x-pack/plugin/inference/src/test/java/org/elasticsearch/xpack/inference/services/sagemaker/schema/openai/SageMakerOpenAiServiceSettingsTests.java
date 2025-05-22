/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.openai;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SageMakerOpenAiServiceSettingsTests extends InferenceSettingsTestCase<OpenAiTextEmbeddingPayload.ApiServiceSettings> {
    @Override
    protected OpenAiTextEmbeddingPayload.ApiServiceSettings fromMutableMap(Map<String, Object> mutableMap) {
        var validationException = new ValidationException();
        var settings = OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(mutableMap, validationException);
        validationException.throwIfValidationErrorsExist();
        return settings;
    }

    @Override
    protected Writeable.Reader<OpenAiTextEmbeddingPayload.ApiServiceSettings> instanceReader() {
        return OpenAiTextEmbeddingPayload.ApiServiceSettings::new;
    }

    @Override
    protected OpenAiTextEmbeddingPayload.ApiServiceSettings createTestInstance() {
        return randomApiServiceSettings();
    }

    static OpenAiTextEmbeddingPayload.ApiServiceSettings randomApiServiceSettings() {
        var dimensions = randomBoolean() ? randomIntBetween(1, 100) : null;
        return new OpenAiTextEmbeddingPayload.ApiServiceSettings(dimensions, dimensions != null);
    }

    public void testDimensionsSetByUser() {
        var expectedDimensions = randomIntBetween(1, 100);
        var dimensionlessSettings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false);
        var updatedSettings = dimensionlessSettings.updateModelWithEmbeddingDetails(expectedDimensions);
        assertThat(updatedSettings, not(sameInstance(dimensionlessSettings)));
        assertThat(updatedSettings.dimensions(), equalTo(expectedDimensions));
    }
}
