/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class VoyageAIRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<VoyageAIRerankServiceSettings> {
    public static VoyageAIRerankServiceSettings createRandom() {
        return new VoyageAIRerankServiceSettings(
            new VoyageAIServiceSettings(randomAlphaOfLength(10), RateLimitSettingsTests.createRandom())
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var url = "http://www.abc.com";
        var model = "model";

        var serviceSettings = new VoyageAIRerankServiceSettings(new VoyageAIServiceSettings(model, null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model",
                "rate_limit": {
                    "requests_per_minute": 2000
                }
            }
            """));
    }

    @Override
    protected Writeable.Reader<VoyageAIRerankServiceSettings> instanceReader() {
        return VoyageAIRerankServiceSettings::new;
    }

    @Override
    protected VoyageAIRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected VoyageAIRerankServiceSettings mutateInstance(VoyageAIRerankServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, VoyageAIRerankServiceSettingsTests::createRandom);
    }

    @Override
    protected VoyageAIRerankServiceSettings mutateInstanceForVersion(VoyageAIRerankServiceSettings instance, TransportVersion version) {
        return instance;
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String model) {
        return new HashMap<>(VoyageAIServiceSettingsTests.getServiceSettingsMap(model));
    }
}
