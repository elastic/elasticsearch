/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class GoogleAiStudioCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<GoogleAiStudioCompletionServiceSettings> {

    public static GoogleAiStudioCompletionServiceSettings createRandom() {
        return new GoogleAiStudioCompletionServiceSettings(randomAlphaOfLength(8), randomFrom(RateLimitSettingsTests.createRandom(), null));
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        var model = "some model";

        var serviceSettings = GoogleAiStudioCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, model)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new GoogleAiStudioCompletionServiceSettings(model, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GoogleAiStudioCompletionServiceSettings("model", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"model","rate_limit":{"requests_per_minute":360}}"""));
    }

    @Override
    protected Writeable.Reader<GoogleAiStudioCompletionServiceSettings> instanceReader() {
        return GoogleAiStudioCompletionServiceSettings::new;
    }

    @Override
    protected GoogleAiStudioCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleAiStudioCompletionServiceSettings mutateInstance(GoogleAiStudioCompletionServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, GoogleAiStudioCompletionServiceSettingsTests::createRandom);
    }
}
