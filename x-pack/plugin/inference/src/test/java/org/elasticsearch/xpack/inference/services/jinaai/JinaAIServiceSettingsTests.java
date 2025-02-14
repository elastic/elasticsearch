/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class JinaAIServiceSettingsTests extends AbstractWireSerializingTestCase<JinaAIServiceSettings> {

    public static JinaAIServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    /**
     * The created settings can have a url set to null.
     */
    public static JinaAIServiceSettings createRandom() {
        var url = randomBoolean() ? randomAlphaOfLength(15) : null;
        return createRandom(url);
    }

    private static JinaAIServiceSettings createRandom(String url) {
        var model = randomAlphaOfLength(15);

        return new JinaAIServiceSettings(ServiceUtils.createOptionalUri(url), model, RateLimitSettingsTests.createRandom());
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var model = "model";
        var serviceSettings = JinaAIServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.URL, url, JinaAIServiceSettings.MODEL_ID, model)),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(serviceSettings, is(new JinaAIServiceSettings(ServiceUtils.createUri(url), model, null)));
    }

    public void testFromMap_WithRateLimit() {
        var url = "https://www.abc.com";
        var model = "model";
        var serviceSettings = JinaAIServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    JinaAIServiceSettings.MODEL_ID,
                    model,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3))
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(new JinaAIServiceSettings(ServiceUtils.createUri(url), model, new RateLimitSettings(3)))
        );
    }

    public void testFromMap_WhenUsingModelId() {
        var url = "https://www.abc.com";
        var model = "model";
        var serviceSettings = JinaAIServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.URL, url, JinaAIServiceSettings.MODEL_ID, model)),
            ConfigurationParseContext.PERSISTENT
        );

        MatcherAssert.assertThat(serviceSettings, is(new JinaAIServiceSettings(ServiceUtils.createUri(url), model, null)));
    }

    public void testFromMap_MissingUrl_DoesNotThrowException() {
        var serviceSettings = JinaAIServiceSettings.fromMap(
            new HashMap<>(Map.of(JinaAIServiceSettings.MODEL_ID, "model")),
            ConfigurationParseContext.PERSISTENT
        );
        assertNull(serviceSettings.uri());
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, "")), ConfigurationParseContext.PERSISTENT)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    ServiceFields.URL
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> JinaAIServiceSettings.fromMap(new HashMap<>(Map.of(ServiceFields.URL, url)), ConfigurationParseContext.PERSISTENT)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s]", url, ServiceFields.URL)
            )
        );
    }

    public void testXContent_WritesModelId() throws IOException {
        var entity = new JinaAIServiceSettings((String) null, "model", new RateLimitSettings(1));

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
        return createRandomWithNonNullUrl();
    }

    @Override
    protected JinaAIServiceSettings mutateInstance(JinaAIServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, JinaAIServiceSettingsTests::createRandom);
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, String model) {
        var map = new HashMap<String, Object>();

        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        map.put(JinaAIServiceSettings.MODEL_ID, model);

        return map;
    }
}
