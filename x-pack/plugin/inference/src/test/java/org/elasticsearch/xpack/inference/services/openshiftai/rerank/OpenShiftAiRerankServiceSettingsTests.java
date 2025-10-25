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
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class OpenShiftAiRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<OpenShiftAiRerankServiceSettings> {

    private static OpenShiftAiRerankServiceSettings createRandom() {
        var modelId = randomAlphaOfLengthOrNull(8);
        var url = randomAlphaOfLength(15);
        return new OpenShiftAiRerankServiceSettings(modelId, ServiceUtils.createUri(url), RateLimitSettingsTests.createRandom());
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var url = "http://www.abc.com";
        var model = "model";

        var serviceSettings = new OpenShiftAiRerankServiceSettings(model, url, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model",
                "url":"http://www.abc.com",
                "rate_limit": {
                    "requests_per_minute": 3000
                }
            }
            """));
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
        return randomValueOtherThan(instance, OpenShiftAiRerankServiceSettingsTests::createRandom);
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
