/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class OpenShiftAiRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<OpenShiftAiRerankServiceSettings> {

    private static OpenShiftAiRerankServiceSettings createRandom() {
        var modelId = randomAlphaOfLengthOrNull(8);
        var url = randomAlphaOfLength(15);
        return new OpenShiftAiRerankServiceSettings(modelId, ServiceUtils.createUri(url), RateLimitSettingsTests.createRandom());
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var url = "http://www.abc.com";
        var model = "model";
        var rateLimitSettings = new RateLimitSettings(100);

        assertXContentEquals(new OpenShiftAiRerankServiceSettings(model, url, rateLimitSettings), """
            {
                "model_id":"model",
                "url":"http://www.abc.com",
                "rate_limit": {
                    "requests_per_minute": 100
                }
            }
            """);
    }

    public void testToXContent_WritesDefaultRateLimitAndOmitsModelIdIfNotSet() throws IOException {
        var url = "http://www.abc.com";

        assertXContentEquals(new OpenShiftAiRerankServiceSettings(null, url, null), """
            {
                "url":"http://www.abc.com",
                "rate_limit": {
                    "requests_per_minute": 3000
                }
            }
            """);
    }

    private static void assertXContentEquals(OpenShiftAiRerankServiceSettings serviceSettings, String expectedString) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(expectedString));
    }

    @Override
    protected Writeable.Reader<OpenShiftAiRerankServiceSettings> instanceReader() {
        return OpenShiftAiRerankServiceSettings::new;
    }

    @Override
    protected OpenShiftAiRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenShiftAiRerankServiceSettings mutateInstance(OpenShiftAiRerankServiceSettings instance) throws IOException {
        String modelId = instance.modelId();
        URI uri = instance.uri();
        RateLimitSettings rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> ServiceUtils.createUri(randomAlphaOfLength(15)));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new OpenShiftAiRerankServiceSettings(modelId, uri, rateLimitSettings);
    }

    @Override
    protected OpenShiftAiRerankServiceSettings mutateInstanceForVersion(
        OpenShiftAiRerankServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        return new HashMap<>(OpenShiftAiChatCompletionServiceSettingsTests.getServiceSettingsMap(url, model));
    }

}
