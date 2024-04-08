/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.hamcrest.Matchers.is;

public class OpenAiChatCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<OpenAiChatCompletionServiceSettings> {

    public void testFromMap_CreatesSettingsCorrectly() {
        var modelId = "some model";
        var url = "https://www.elastic.co";
        var org = "organization";
        var maxInputTokens = 8192;

        var serviceSettings = OpenAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.URL,
                    url,
                    OpenAiServiceFields.ORGANIZATION,
                    org,
                    MAX_INPUT_TOKENS,
                    maxInputTokens
                )
            )
        );

        assertThat(
            serviceSettings,
            is(
                new OpenAiChatCompletionServiceSettings(
                    new OpenAiServiceSettings(modelId, ServiceUtils.createUri(url), org),
                    maxInputTokens
                )
            )
        );
    }

    public void testToXContent_WritesMaxInputTokens() throws IOException {
        var serviceSettings = new OpenAiChatCompletionServiceSettings(
            new OpenAiServiceSettings("model", ServiceUtils.createUri("url"), "org"),
            1024
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = org.elasticsearch.common.Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"model_id":"model","url":"url","organization_id":"org","max_input_tokens":1024}"""));
    }

    public void testToXContent_DoesNotWriteOptionalMaxInputToken() throws IOException {
        var serviceSettings = new OpenAiChatCompletionServiceSettings(
            OpenAiServiceSettingsTests.createRandom(randomAlphaOfLength(8)),
            null
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertFalse(xContentResult.contains(MAX_INPUT_TOKENS));
    }

    @Override
    protected Writeable.Reader<OpenAiChatCompletionServiceSettings> instanceReader() {
        return OpenAiChatCompletionServiceSettings::new;
    }

    @Override
    protected OpenAiChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenAiChatCompletionServiceSettings mutateInstance(OpenAiChatCompletionServiceSettings instance) throws IOException {
        return createRandom();
    }

    private static OpenAiChatCompletionServiceSettings createRandom() {
        var commonSettings = OpenAiServiceSettingsTests.createRandomWithNonNullUrl();
        var maxInputTokens = randomFrom(randomIntBetween(128, 4096), null);

        return new OpenAiChatCompletionServiceSettings(commonSettings, maxInputTokens);
    }

}
