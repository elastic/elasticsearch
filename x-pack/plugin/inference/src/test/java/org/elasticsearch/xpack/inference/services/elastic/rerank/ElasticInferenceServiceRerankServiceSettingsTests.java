/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
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

public class ElasticInferenceServiceRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    ElasticInferenceServiceRerankServiceSettings> {

    @Override
    protected Writeable.Reader<ElasticInferenceServiceRerankServiceSettings> instanceReader() {
        return ElasticInferenceServiceRerankServiceSettings::new;
    }

    @Override
    protected ElasticInferenceServiceRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElasticInferenceServiceRerankServiceSettings mutateInstance(ElasticInferenceServiceRerankServiceSettings instance)
        throws IOException {
        String modelId = randomValueOtherThan(instance.modelId(), ElasticInferenceServiceRerankServiceSettingsTests::randomRerankModel);
        return new ElasticInferenceServiceRerankServiceSettings(modelId);
    }

    public void testFromMap() {
        var modelId = "my-model-id";

        var serviceSettings = ElasticInferenceServiceRerankServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new ElasticInferenceServiceRerankServiceSettings(modelId)));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
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

        var serviceSettings = ElasticInferenceServiceRerankServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings, is(new ElasticInferenceServiceRerankServiceSettings(modelId)));
        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_DoesNotThrowValidationException_WhenRateLimitFieldDoesNotExist() {
        var modelId = "my-model-id";

        var map = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, modelId));

        var serviceSettings = ElasticInferenceServiceRerankServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new ElasticInferenceServiceRerankServiceSettings(modelId)));
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
            () -> ElasticInferenceServiceRerankServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            exception.getMessage(),
            containsString("[service_settings] rate limit settings are not permitted for service [elastic] and task type [rerank]")
        );
        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var modelId = ".rerank-v1";
        var serviceSettings = new ElasticInferenceServiceRerankServiceSettings(modelId);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {"model_id":"%s"}""", modelId))));
    }

    public static ElasticInferenceServiceRerankServiceSettings createRandom() {
        return new ElasticInferenceServiceRerankServiceSettings(randomRerankModel());
    }

    private static String randomRerankModel() {
        return randomFrom(".rerank-v1", ".rerank-v2");
    }

    @Override
    protected ElasticInferenceServiceRerankServiceSettings mutateInstanceForVersion(
        ElasticInferenceServiceRerankServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
