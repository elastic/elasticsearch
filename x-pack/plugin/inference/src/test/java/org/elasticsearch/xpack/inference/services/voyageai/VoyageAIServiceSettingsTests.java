/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class VoyageAIServiceSettingsTests extends AbstractWireSerializingTestCase<VoyageAIServiceSettings> {

    public static VoyageAIServiceSettings createRandomWithNonNullUrl() {
        return createRandom();
    }

    /**
     * The created settings can have a url set to null.
     */
    public static VoyageAIServiceSettings createRandom() {
        var model = randomAlphaOfLength(15);

        return new VoyageAIServiceSettings(model, RateLimitSettingsTests.createRandom());
    }

    public void testFromMap() {
        var model = "model";
        var serviceSettings = VoyageAIServiceSettings.fromMap(
            new HashMap<>(Map.of(VoyageAIServiceSettings.MODEL_ID, model)),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(serviceSettings, is(new VoyageAIServiceSettings(model, null)));
    }

    public void testFromMap_WithRateLimit() {
        var model = "model";
        var serviceSettings = VoyageAIServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    VoyageAIServiceSettings.MODEL_ID,
                    model,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3))
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(serviceSettings, is(new VoyageAIServiceSettings(model, new RateLimitSettings(3))));
    }

    public void testFromMap_WhenUsingModelId() {
        var model = "model";
        var serviceSettings = VoyageAIServiceSettings.fromMap(
            new HashMap<>(Map.of(VoyageAIServiceSettings.MODEL_ID, model)),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(serviceSettings, is(new VoyageAIServiceSettings(model, null)));
    }

    public void testXContent_WritesModelId() throws IOException {
        var entity = new VoyageAIServiceSettings("model", new RateLimitSettings(1));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"model","rate_limit":{"requests_per_minute":1}}"""));
    }

    @Override
    protected Writeable.Reader<VoyageAIServiceSettings> instanceReader() {
        return VoyageAIServiceSettings::new;
    }

    @Override
    protected VoyageAIServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected VoyageAIServiceSettings mutateInstance(VoyageAIServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, VoyageAIServiceSettingsTests::createRandom);
    }

    public static Map<String, Object> getServiceSettingsMap(String model) {
        var map = new HashMap<String, Object>();

        map.put(VoyageAIServiceSettings.MODEL_ID, model);

        return map;
    }
}
