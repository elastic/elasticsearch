/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<
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
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new ElasticInferenceServiceCompletionServiceSettings(modelId, new RateLimitSettings(720L))));
    }

    public void testFromMap_MissingModelId_ThrowsException() {
        ValidationException validationException = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceCompletionServiceSettings.fromMap(new HashMap<>(Map.of()), ConfigurationParseContext.REQUEST)
        );

        assertThat(validationException.getMessage(), containsString("does not contain the required setting [model_id]"));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var modelId = "model_id";
        var serviceSettings = new ElasticInferenceServiceCompletionServiceSettings(modelId, new RateLimitSettings(1000));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":1000}}""", modelId)));
    }

    public static ElasticInferenceServiceCompletionServiceSettings createRandom() {
        return new ElasticInferenceServiceCompletionServiceSettings(randomAlphaOfLength(4), RateLimitSettingsTests.createRandom());
    }
}
