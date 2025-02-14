/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class JinaAIRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAIRerankServiceSettings> {
    public static JinaAIRerankServiceSettings createRandom() {
        return new JinaAIRerankServiceSettings(
            new JinaAIServiceSettings(
                randomFrom(new String[] { null, Strings.format("http://%s.com", randomAlphaOfLength(8)) }),
                randomAlphaOfLength(10),
                RateLimitSettingsTests.createRandom()
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var url = "http://www.abc.com";
        var model = "model";

        var serviceSettings = new JinaAIRerankServiceSettings(new JinaAIServiceSettings(url, model, null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "url":"http://www.abc.com",
                "model_id":"model",
                "rate_limit": {
                    "requests_per_minute": 2000
                }
            }
            """));
    }

    @Override
    protected Writeable.Reader<JinaAIRerankServiceSettings> instanceReader() {
        return JinaAIRerankServiceSettings::new;
    }

    @Override
    protected JinaAIRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected JinaAIRerankServiceSettings mutateInstance(JinaAIRerankServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, JinaAIRerankServiceSettingsTests::createRandom);
    }

    @Override
    protected JinaAIRerankServiceSettings mutateInstanceForVersion(JinaAIRerankServiceSettings instance, TransportVersion version) {
        return instance;
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        return new HashMap<>(JinaAIServiceSettingsTests.getServiceSettingsMap(url, model));
    }
}
