/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.openai;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemasTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.sagemaker.schema.openai.OpenAiTextEmbeddingPayload.SIMILARITY_UNSUPPORTED_MESSAGE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
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

    @Override
    protected OpenAiTextEmbeddingPayload.ApiServiceSettings mutateInstanceForVersion(
        OpenAiTextEmbeddingPayload.ApiServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(OpenAiTextEmbeddingPayload.ApiServiceSettings.INFERENCE_SAGEMAKER_OPENAI_SIMILARITY) == false) {
            // Older nodes always used dot_product; there was no stored similarity field.
            return new OpenAiTextEmbeddingPayload.ApiServiceSettings(
                instance.dimensions(),
                instance.dimensionsSetByUser(),
                SimilarityMeasure.DOT_PRODUCT
            );
        }
        return instance;
    }

    static OpenAiTextEmbeddingPayload.ApiServiceSettings randomApiServiceSettings() {
        var dimensions = randomBoolean() ? randomIntBetween(1, 100) : null;
        // When dimensions are present they may have been set by the user or auto-discovered, so exercise both.
        var dimensionsSetByUser = dimensions != null && randomBoolean();
        // Use non-null values only: the XContent round-trip (PERSISTENT) turns null into DOT_PRODUCT.
        var similarity = randomFrom(SimilarityMeasure.values());
        return new OpenAiTextEmbeddingPayload.ApiServiceSettings(dimensions, dimensionsSetByUser, similarity);
    }

    // --- similarity parsing tests ---

    public void testFromStorage_MissingSimilarity_DefaultsToDotProduct() {
        // Endpoints persisted before the similarity field was added always used dot_product.
        var validationException = new ValidationException();
        var settings = OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(
            new HashMap<String, Object>(Map.of("dimensions", 123)),
            ConfigurationParseContext.PERSISTENT,
            validationException
        );
        validationException.throwIfValidationErrorsExist();
        assertThat(settings.similarity(), is(SimilarityMeasure.DOT_PRODUCT));
    }

    public void testFromStorage_ReadsSimilarity() {
        for (var expected : SimilarityMeasure.values()) {
            var validationException = new ValidationException();
            var map = new HashMap<String, Object>(Map.of("dimensions", 123, SIMILARITY, expected.toString()));
            var settings = OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(
                map,
                ConfigurationParseContext.PERSISTENT,
                validationException
            );
            validationException.throwIfValidationErrorsExist();
            assertThat(settings.similarity(), is(expected));
            assertThat(map, not(hasKey(SIMILARITY)));
        }
    }

    public void testFromRequest_MissingSimilarity_IsNull() {
        var validationException = new ValidationException();
        var settings = OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(
            new HashMap<String, Object>(),
            ConfigurationParseContext.REQUEST,
            validationException
        );
        validationException.throwIfValidationErrorsExist();
        assertNull(settings.similarity());
    }

    public void testFromRequest_ReadsSimilarity() {
        var validationException = new ValidationException();
        var map = new HashMap<String, Object>(Map.of(SIMILARITY, SimilarityMeasure.COSINE.toString()));
        var settings = OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST, validationException);
        validationException.throwIfValidationErrorsExist();
        assertThat(settings.similarity(), is(SimilarityMeasure.COSINE));
        assertThat(map, not(hasKey(SIMILARITY)));
    }

    public void testFromRequest_InvalidSimilarity_AddsValidationError() {
        var validationException = new ValidationException();
        OpenAiTextEmbeddingPayload.ApiServiceSettings.fromMap(
            new HashMap<String, Object>(Map.of(SIMILARITY, "invalid_value")),
            ConfigurationParseContext.REQUEST,
            validationException
        );
        var exception = expectThrows(ValidationException.class, validationException::throwIfValidationErrorsExist);
        assertThat(exception.getMessage(), containsString("[service_settings]"));
        assertThat(exception.getMessage(), containsString("[" + SIMILARITY + "]"));
    }

    // --- filtered GET output tests ---

    public void testFilteredXContentObjectIncludesSimilarity() throws IOException {
        var settings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false, SimilarityMeasure.COSINE);
        assertThat(toMap(settings.getFilteredXContentObject()), hasKey(SIMILARITY));
        assertThat(toMap(settings.getFilteredXContentObject()).get(SIMILARITY), is(SimilarityMeasure.COSINE.toString()));
    }

    // --- updateModelWithEmbeddingDetails tests ---

    public void testDimensionsSetByUser() {
        var expectedDimensions = randomIntBetween(1, 100);
        var dimensionlessSettings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false, SimilarityMeasure.COSINE);
        var updatedSettings = dimensionlessSettings.updateModelWithEmbeddingDetails(expectedDimensions);
        assertThat(updatedSettings, not(sameInstance(dimensionlessSettings)));
        assertThat(updatedSettings.dimensions(), equalTo(expectedDimensions));
    }

    public void testUpdateModelWithEmbeddingDetails_PreservesSimilarity() {
        for (var similarity : SimilarityMeasure.values()) {
            var settings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false, similarity);
            var updated = settings.updateModelWithEmbeddingDetails(42);
            assertThat(updated.similarity(), is(similarity));
        }
    }

    // --- resolveCreateRequestDefaults hook tests ---

    public void testResolveCreateRequestDefaults_FeatureSupported_MissingSimilarity_DefaultsToCosine() {
        var settings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false, null);
        var resolved = settings.resolveCreateRequestDefaults(SageMakerSchemasTests.mockInferenceFeatureService(true));
        assertThat(resolved.similarity(), is(SimilarityMeasure.COSINE));
    }

    public void testResolveCreateRequestDefaults_FeatureSupported_UserSimilarity_IsKept() {
        var settings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false, SimilarityMeasure.DOT_PRODUCT);
        var resolved = settings.resolveCreateRequestDefaults(SageMakerSchemasTests.mockInferenceFeatureService(true));
        assertThat(resolved, sameInstance(settings));
    }

    public void testResolveCreateRequestDefaults_FeatureUnsupported_MissingSimilarity_DefaultsToDotProduct() {
        var settings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false, null);
        var resolved = settings.resolveCreateRequestDefaults(SageMakerSchemasTests.mockInferenceFeatureService(false));
        assertThat(resolved.similarity(), is(SimilarityMeasure.DOT_PRODUCT));
    }

    public void testResolveCreateRequestDefaults_FeatureUnsupported_UserSimilarity_Throws() {
        var settings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(null, false, SimilarityMeasure.COSINE);
        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> settings.resolveCreateRequestDefaults(SageMakerSchemasTests.mockInferenceFeatureService(false))
        );
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is(SIMILARITY_UNSUPPORTED_MESSAGE));
    }

    // --- existing parsing tests (updated for new constructor) ---

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
        var settings = new OpenAiTextEmbeddingPayload.ApiServiceSettings(
            randomIntBetween(1, 100),
            randomBoolean(),
            SimilarityMeasure.COSINE
        );
        // The persisted form keeps the internal flag so it survives a round-trip...
        assertThat(toMap(settings), hasKey("dimensions_set_by_user"));
        // ...but the filtered form returned in the GET response must not expose it.
        assertThat(toMap(settings.getFilteredXContentObject()), not(hasKey("dimensions_set_by_user")));
    }
}
