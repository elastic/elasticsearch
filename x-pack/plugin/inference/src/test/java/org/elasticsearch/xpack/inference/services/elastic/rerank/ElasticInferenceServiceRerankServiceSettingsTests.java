/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.rerank;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
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

import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceRerankServiceSettingsTests extends AbstractWireSerializingTestCase<
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
        return randomValueOtherThan(instance, ElasticInferenceServiceRerankServiceSettingsTests::createRandom);
    }

    public void testFromMap() {
        var modelId = "my-model-id";

        var serviceSettings = ElasticInferenceServiceRerankServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new ElasticInferenceServiceRerankServiceSettings(modelId, null)));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var modelId = ".rerank-v1";
        var rateLimitSettings = new RateLimitSettings(100L);
        var serviceSettings = new ElasticInferenceServiceRerankServiceSettings(modelId, rateLimitSettings);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":%d}}""", modelId, rateLimitSettings.requestsPerTimeUnit())));
    }

    public static ElasticInferenceServiceRerankServiceSettings createRandom() {
        return new ElasticInferenceServiceRerankServiceSettings(randomRerankModel(), null);
    }

    private static String randomRerankModel() {
        return randomFrom(".rerank-v1", ".rerank-v2");
    }
}
