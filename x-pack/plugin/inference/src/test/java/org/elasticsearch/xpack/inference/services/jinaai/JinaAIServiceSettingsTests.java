/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIService.JINA_AI_EMBEDDING_REFACTOR;
import static org.hamcrest.Matchers.is;

public class JinaAIServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAIServiceSettings> {

    public static JinaAIServiceSettings createRandom() {
        var model = randomAlphaOfLength(15);

        return new JinaAIServiceSettings(model, RateLimitSettingsTests.createRandom());
    }

    public void testFromMap() {
        var model = "model";
        var serviceSettings = JinaAIServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, model)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new JinaAIServiceSettings(model, null)));
    }

    public void testFromMap_WithRateLimit() {
        var model = "model";
        var serviceSettings = JinaAIServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    model,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3))
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new JinaAIServiceSettings(model, new RateLimitSettings(3))));
    }

    public void testFromMap_WhenUsingModelId() {
        var model = "model";
        var serviceSettings = JinaAIServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, model)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new JinaAIServiceSettings(model, null)));
    }

    public void testXContent_WritesAllFields() throws IOException {
        var entity = new JinaAIServiceSettings("model", new RateLimitSettings(1));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"model","rate_limit":{"requests_per_minute":1}}"""));
    }

    @Override
    protected Writeable.Reader<JinaAIServiceSettings> instanceReader() {
        return JinaAIServiceSettings::new;
    }

    @Override
    protected JinaAIServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected JinaAIServiceSettings mutateInstance(JinaAIServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(1)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(15));
            case 1 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new JinaAIServiceSettings(modelId, rateLimitSettings);
    }

    public static Map<String, Object> getServiceSettingsMap(String model) {
        var map = new HashMap<String, Object>();

        map.put(ServiceFields.MODEL_ID, model);

        return map;
    }

    @Override
    protected JinaAIServiceSettings mutateInstanceForVersion(JinaAIServiceSettings instance, TransportVersion version) {
        if (version.supports(JINA_AI_EMBEDDING_REFACTOR)) {
            return instance;
        } else {
            return instance;
        }
    }
}
