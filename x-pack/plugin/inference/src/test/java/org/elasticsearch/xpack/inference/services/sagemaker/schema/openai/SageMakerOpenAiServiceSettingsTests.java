/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.openai;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SageMakerOpenAiServiceSettingsTests extends InferenceSettingsTestCase<OpenAiTextEmbeddingPayload.ApiServiceSettings> {
    @Override
    protected OpenAiTextEmbeddingPayload.ApiServiceSettings fromMutableMap(Map<String, Object> mutableMap) {
        var validationException = new ValidationException();
        var settings = OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(
            mutableMap,
            ConfigurationParseContext.PERSISTENT,
            validationException
        );
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
        // When dimensions are present they may have been set by the user or auto-discovered, so exercise both.
        var dimensionsSetByUser = dimensions != null && randomBoolean();
        return new OpenAiTextEmbeddingPayload.ApiServiceSettings(dimensions, dimensionsSetByUser);
    }

    public void testDimensionsSetByUser() {
        var expectedDimensions = randomIntBetween(1, 100);
        var dimensionlessSettings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false);
        var updatedSettings = dimensionlessSettings.updateModelWithEmbeddingDetails(expectedDimensions);
        assertThat(updatedSettings, not(sameInstance(dimensionlessSettings)));
        assertThat(updatedSettings.dimensions(), equalTo(expectedDimensions));
    }

    public void testFromRequest_DimensionsSetByUserIsDerivedFromDimensions() {
        var validationException = new ValidationException();
        var withDimensions = OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(
            new HashMap<String, Object>(Map.of("dimensions", 123)),
            ConfigurationParseContext.REQUEST,
            validationException
        );
        validationException.throwIfValidationErrorsExist();
        assertThat(withDimensions.dimensions(), equalTo(123));
        assertThat(withDimensions.dimensionsSetByUser(), equalTo(true));

        var withoutDimensions = OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(
            new HashMap<String, Object>(),
            ConfigurationParseContext.REQUEST,
            validationException
        );
        validationException.throwIfValidationErrorsExist();
        assertThat(withoutDimensions.dimensions(), nullValue());
        assertThat(withoutDimensions.dimensionsSetByUser(), equalTo(false));
    }

    public void testFromRequest_DoesNotConsumeDimensionsSetByUser() {
        // In a request, dimensions_set_by_user is not parsed, so it remains in the map for the service to reject as unknown.
        var validationException = new ValidationException();
        var map = new HashMap<String, Object>(Map.of("dimensions", 123, "dimensions_set_by_user", false));
        OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST, validationException);
        validationException.throwIfValidationErrorsExist();
        assertThat(map, hasKey("dimensions_set_by_user"));
    }

    public void testFromStorage_MissingDimensionsSetByUser_DefaultsToFalse() {
        // Configs persisted before the field existed treat their dimensions as auto-discovered.
        var validationException = new ValidationException();
        var settings = OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(
            new HashMap<String, Object>(Map.of("dimensions", 123)),
            ConfigurationParseContext.PERSISTENT,
            validationException
        );
        validationException.throwIfValidationErrorsExist();
        assertThat(settings.dimensions(), equalTo(123));
        assertThat(settings.dimensionsSetByUser(), equalTo(false));
    }

    public void testFromStorage_ReadsDimensionsSetByUser() {
        var validationException = new ValidationException();
        var map = new HashMap<String, Object>(Map.of("dimensions", 123, "dimensions_set_by_user", false));
        var settings = OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(
            map,
            ConfigurationParseContext.PERSISTENT,
            validationException
        );
        validationException.throwIfValidationErrorsExist();
        assertThat(settings.dimensionsSetByUser(), equalTo(false));
        assertThat(map, not(hasKey("dimensions_set_by_user")));
    }

    public void testFilteredXContentObjectOmitsDimensionsSetByUser() throws IOException {
        var settings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(randomIntBetween(1, 100), randomBoolean());
        // The persisted form keeps the internal flag so it survives a round-trip...
        assertThat(toMap(settings), hasKey("dimensions_set_by_user"));
        // ...but the filtered form returned in the GET response must not expose it.
        assertThat(toMap(settings.getFilteredXContentObject()), not(hasKey("dimensions_set_by_user")));
    }
}
