/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceDocumentExtractionServiceSettingsTests extends AbstractBWCSerializationTestCase<
    ElasticInferenceServiceDocumentExtractionServiceSettings> {

    @Override
    protected Writeable.Reader<ElasticInferenceServiceDocumentExtractionServiceSettings> instanceReader() {
        return ElasticInferenceServiceDocumentExtractionServiceSettings::new;
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionServiceSettings mutateInstance(
        ElasticInferenceServiceDocumentExtractionServiceSettings instance
    ) throws IOException {
        String modelId = randomValueOtherThan(instance.modelId(), () -> randomAlphaOfLength(10));
        return new ElasticInferenceServiceDocumentExtractionServiceSettings(modelId);
    }

    public void testFromMap_MissingModelId_ThrowsValidationException() {
        var exception = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceDocumentExtractionServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("does not contain the required setting [model_id]"));
    }

    public void testFromMap_DoesNotRemoveRateLimitField_DoesNotThrowValidationException_PersistentContext() {
        var modelId = "my-model-id";

        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                modelId,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
            )
        );

        var serviceSettings = ElasticInferenceServiceDocumentExtractionServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings, is(new ElasticInferenceServiceDocumentExtractionServiceSettings(modelId)));
        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_DoesNotThrowValidationException_WhenRateLimitFieldDoesNotExist() {
        var modelId = "my-model-id";

        var map = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, modelId));

        var serviceSettings = ElasticInferenceServiceDocumentExtractionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new ElasticInferenceServiceDocumentExtractionServiceSettings(modelId)));
        assertThat(map, anEmptyMap());
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_DoesThrowValidationException_WhenRateLimitFieldDoesExist_RequestContext() {
        var modelId = "my-model-id";

        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                modelId,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
            )
        );

        var exception = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceDocumentExtractionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            exception.getMessage(),
            containsString(
                "[service_settings] rate limit settings are not permitted for service [elastic] and task type [document_extraction]"
            )
        );
        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))));
    }

    public static ElasticInferenceServiceDocumentExtractionServiceSettings createRandom() {
        return new ElasticInferenceServiceDocumentExtractionServiceSettings(randomAlphaOfLength(10));
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionServiceSettings mutateInstanceForVersion(
        ElasticInferenceServiceDocumentExtractionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected ElasticInferenceServiceDocumentExtractionServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return ElasticInferenceServiceDocumentExtractionServiceSettings.fromMap(parser.map(), ConfigurationParseContext.PERSISTENT);
    }
}
