/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    ElasticInferenceServiceCompletionServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;

    @Override
    protected Writeable.Reader<ElasticInferenceServiceCompletionServiceSettings> instanceReader() {
        return ElasticInferenceServiceCompletionServiceSettings::new;
    }

    @Override
    protected ElasticInferenceServiceCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElasticInferenceServiceCompletionServiceSettings mutateInstance(ElasticInferenceServiceCompletionServiceSettings instance)
        throws IOException {
        return new ElasticInferenceServiceCompletionServiceSettings(randomValueOtherThan(instance.modelId(), () -> randomAlphaOfLength(4)));
    }

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID));

        var serviceSettings = new ElasticInferenceServiceCompletionServiceSettings(INITIAL_TEST_MODEL_ID).updateServiceSettings(
            settingsMap,
            TaskType.COMPLETION
        );

        MatcherAssert.assertThat(serviceSettings, is(new ElasticInferenceServiceCompletionServiceSettings(TEST_MODEL_ID)));
    }

    public void testUpdateServiceSettings_WithRateLimit_ThrowsException() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var exception = expectThrows(
            ValidationException.class,
            () -> new ElasticInferenceServiceCompletionServiceSettings(INITIAL_TEST_MODEL_ID).updateServiceSettings(
                settingsMap,
                TaskType.COMPLETION
            )
        );

        assertThat(
            exception.getMessage(),
            containsString("[service_settings] rate limit settings are not permitted for service [elastic] and task type [chat_completion]")
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new ElasticInferenceServiceCompletionServiceSettings(INITIAL_TEST_MODEL_ID).updateServiceSettings(
            new HashMap<>(),
            TaskType.COMPLETION
        );

        MatcherAssert.assertThat(serviceSettings, is(new ElasticInferenceServiceCompletionServiceSettings(INITIAL_TEST_MODEL_ID)));
    }

    public void testFromMap() {
        var serviceSettings = ElasticInferenceServiceCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, TEST_MODEL_ID)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new ElasticInferenceServiceCompletionServiceSettings(TEST_MODEL_ID)));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_ThrowsValidationError_IfRateLimitFieldExists_ForRequestContext() {
        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );
        var exception = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            exception.getMessage(),
            containsString("[service_settings] rate limit settings are not permitted for service [elastic] and task type [chat_completion]")
        );
    }

    public void testFromMap_DoesNotThrowValidationError_IfRateLimitFieldExists_ForPersistentContext() {
        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var serviceSettings = ElasticInferenceServiceCompletionServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))));
        assertThat(serviceSettings, is(new ElasticInferenceServiceCompletionServiceSettings(TEST_MODEL_ID)));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_MissingModelId_ThrowsException() {
        ValidationException validationException = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceCompletionServiceSettings.fromMap(new HashMap<>(Map.of()), ConfigurationParseContext.REQUEST)
        );

        assertThat(validationException.getMessage(), containsString("does not contain the required setting [model_id]"));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var serviceSettings = new ElasticInferenceServiceCompletionServiceSettings(TEST_MODEL_ID);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id":"%s"
            }""", TEST_MODEL_ID))));
    }

    public static ElasticInferenceServiceCompletionServiceSettings createRandom() {
        return new ElasticInferenceServiceCompletionServiceSettings(randomAlphaOfLength(4));
    }

    @Override
    protected ElasticInferenceServiceCompletionServiceSettings mutateInstanceForVersion(
        ElasticInferenceServiceCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
