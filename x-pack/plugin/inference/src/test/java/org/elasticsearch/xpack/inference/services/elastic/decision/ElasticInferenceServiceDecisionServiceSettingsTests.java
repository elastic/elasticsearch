/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.decision;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elastic.decision.ElasticInferenceServiceDecisionServiceSettings.INFERENCE_API_EIS_DECISION;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceDecisionServiceSettingsTests extends AbstractWireSerializingTestCase<
    ElasticInferenceServiceDecisionServiceSettings> {

    @Override
    protected Writeable.Reader<ElasticInferenceServiceDecisionServiceSettings> instanceReader() {
        return ElasticInferenceServiceDecisionServiceSettings::new;
    }

    @Override
    protected ElasticInferenceServiceDecisionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElasticInferenceServiceDecisionServiceSettings mutateInstance(ElasticInferenceServiceDecisionServiceSettings instance) {
        return new ElasticInferenceServiceDecisionServiceSettings(randomValueOtherThan(instance.modelId(), () -> randomAlphaOfLength(8)));
    }

    public void testFromMap() {
        var map = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, "my-model-id"));

        var serviceSettings = ElasticInferenceServiceDecisionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new ElasticInferenceServiceDecisionServiceSettings("my-model-id")));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
        assertThat(map, anEmptyMap());
    }

    public void testFromMap_ThrowsWhenModelIdIsMissing() {
        var exception = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceDecisionServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("[service_settings] does not contain the required setting [model_id]"));
    }

    public void testFromMap_ThrowsWhenRateLimitFieldExists_RequestContext() {
        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                "my-model-id",
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
            )
        );

        var exception = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceDecisionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            exception.getMessage(),
            containsString("[service_settings] rate limit settings are not permitted for service [elastic] and task type [decision]")
        );
    }

    public void testFromMap_IgnoresRateLimitField_PersistentContext() {
        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                "my-model-id",
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
            )
        );

        var serviceSettings = ElasticInferenceServiceDecisionServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings, is(new ElasticInferenceServiceDecisionServiceSettings("my-model-id")));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var serviceSettings = new ElasticInferenceServiceDecisionServiceSettings("my-model-id");

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);

        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            {"model_id":"my-model-id"}""")));
    }

    public void testSupportsVersion() {
        var serviceSettings = createRandom();

        assertTrue(serviceSettings.supportsVersion(INFERENCE_API_EIS_DECISION));
        assertTrue(serviceSettings.supportsVersion(TransportVersion.current()));
        assertFalse(serviceSettings.supportsVersion(TransportVersion.minimumCompatible()));
    }

    public static ElasticInferenceServiceDecisionServiceSettings createRandom() {
        return new ElasticInferenceServiceDecisionServiceSettings(randomAlphaOfLength(8));
    }
}
