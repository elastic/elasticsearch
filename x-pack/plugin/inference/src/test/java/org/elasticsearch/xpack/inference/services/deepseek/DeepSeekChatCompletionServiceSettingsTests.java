/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class DeepSeekChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    DeepSeekChatCompletionModel.DeepSeekServiceSettings> {

    static final String TEST_URL = "https://www.test.com";
    private static final String INITIAL_TEST_URL = "https://www.initial-test.com";

    static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    public static DeepSeekChatCompletionModel.DeepSeekServiceSettings createRandom() {
        return new DeepSeekChatCompletionModel.DeepSeekServiceSettings(
            randomAlphaOfLength(8),
            ServiceUtils.createOptionalUri(randomAlphaOfLengthOrNull(8))
        );
    }

    public void testUpdateServiceSettings_AllFields_ReturnsOriginalInstance() {
        var settingsMap = buildServiceSettingsMap(TEST_URL, TEST_MODEL_ID);

        var originalServiceSettings = new DeepSeekChatCompletionModel.DeepSeekServiceSettings(
            INITIAL_TEST_MODEL_ID,
            ServiceUtils.createUri(INITIAL_TEST_URL)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(updatedServiceSettings, sameInstance(originalServiceSettings));
    }

    public void testUpdateServiceSettings_EmptyMap_ReturnsOriginalInstance() {
        var originalServiceSettings = new DeepSeekChatCompletionModel.DeepSeekServiceSettings(
            INITIAL_TEST_MODEL_ID,
            ServiceUtils.createUri(INITIAL_TEST_URL)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, sameInstance(originalServiceSettings));
    }

    public void testToXContent_AllFields_WritesAllValues() throws IOException {
        var serviceSettings = new DeepSeekChatCompletionModel.DeepSeekServiceSettings(TEST_MODEL_ID, ServiceUtils.createUri(TEST_URL));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
              "model_id": "%s",
              "url": "%s"
            }
            """, TEST_MODEL_ID, TEST_URL))));
    }

    public void testToXContent_OnlyModelId_WritesModelIdValue() throws IOException {
        var serviceSettings = new DeepSeekChatCompletionModel.DeepSeekServiceSettings(TEST_MODEL_ID, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
              "model_id": "%s"
            }
            """, TEST_MODEL_ID, TEST_URL))));
    }

    public static HashMap<String, Object> buildServiceSettingsMap(@Nullable String url, @Nullable String modelId) {
        var result = new HashMap<String, Object>();
        if (url != null) {
            result.put(ServiceFields.URL, url);
        }
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        return result;
    }

    @Override
    protected Writeable.Reader<DeepSeekChatCompletionModel.DeepSeekServiceSettings> instanceReader() {
        return DeepSeekChatCompletionModel.DeepSeekServiceSettings::new;
    }

    @Override
    protected DeepSeekChatCompletionModel.DeepSeekServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected DeepSeekChatCompletionModel.DeepSeekServiceSettings mutateInstance(
        DeepSeekChatCompletionModel.DeepSeekServiceSettings instance
    ) throws IOException {
        var modelId = instance.modelId();
        URI uri = instance.uri();
        var uriString = uri == null ? null : uri.toString();
        switch (randomInt(1)) {
            case 0 -> uriString = randomValueOtherThan(uriString, () -> randomAlphaOfLengthOrNull(8));
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new DeepSeekChatCompletionModel.DeepSeekServiceSettings(modelId, ServiceUtils.createOptionalUri(uriString));
    }

    @Override
    protected DeepSeekChatCompletionModel.DeepSeekServiceSettings mutateInstanceForVersion(
        DeepSeekChatCompletionModel.DeepSeekServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
