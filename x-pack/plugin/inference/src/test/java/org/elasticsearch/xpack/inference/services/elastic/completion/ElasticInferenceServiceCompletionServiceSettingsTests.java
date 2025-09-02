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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    ElasticInferenceServiceCompletionServiceSettings> {

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
        return randomValueOtherThan(instance, ElasticInferenceServiceCompletionServiceSettingsTests::createRandom);
    }

    public void testFromMap() {
        var modelId = "model_id";

        var serviceSettings = ElasticInferenceServiceCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId))
        );

        assertThat(serviceSettings, is(new ElasticInferenceServiceCompletionServiceSettings(modelId)));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_DoesNotRemoveRateLimitField() {
        var modelId = "my-model-id";

        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                modelId,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
            )
        );
        var serviceSettings = ElasticInferenceServiceRerankServiceSettings.fromMap(map);

        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))));
        assertThat(serviceSettings, is(new ElasticInferenceServiceRerankServiceSettings(modelId)));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_MissingModelId_ThrowsException() {
        ValidationException validationException = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceCompletionServiceSettings.fromMap(new HashMap<>(Map.of()))
        );

        assertThat(validationException.getMessage(), containsString("does not contain the required setting [model_id]"));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var modelId = "model_id";
        var serviceSettings = new ElasticInferenceServiceCompletionServiceSettings(modelId);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id":"%s"
            }""", modelId))));
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
