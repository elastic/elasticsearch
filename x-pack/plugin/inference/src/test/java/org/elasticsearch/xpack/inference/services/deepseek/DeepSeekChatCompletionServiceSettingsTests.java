/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DeepSeekChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<DeepSeekServiceSettings> {

    static final String TEST_URL = "https://www.test.com";
    private static final String INITIAL_TEST_URL = "https://www.initial-test.com";

    static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    public static DeepSeekServiceSettings createRandom() {
        return new DeepSeekServiceSettings(randomAlphaOfLength(8), ServiceUtils.createOptionalUri(randomAlphaOfLengthOrNull(8)));
    }

    public void testFromMap_AllFields() {
        var serviceSettingsMap = buildServiceSettingsMap(TEST_URL, TEST_MODEL_ID);
        var settings = DeepSeekServiceSettings.fromMap(serviceSettingsMap);
        assertThat(settings.modelId(), is(TEST_MODEL_ID));
        assertThat(settings.uri(), is(ServiceUtils.createUri(TEST_URL)));
    }

    public void testFromMap_RequiredFieldsOnly() {
        var serviceSettingsMap = buildServiceSettingsMap(null, TEST_MODEL_ID);
        var settings = DeepSeekServiceSettings.fromMap(serviceSettingsMap);
        assertThat(settings.modelId(), is(TEST_MODEL_ID));
        assertThat(settings.uri(), is(nullValue()));
    }

    public void testFromMap_NoModelId_ThrowsValidationException() {
        var serviceSettingsMap = buildServiceSettingsMap(TEST_URL, null);
        var exception = expectThrows(ValidationException.class, () -> DeepSeekServiceSettings.fromMap(serviceSettingsMap));
        assertThat(exception.getMessage(), containsString("[service_settings] does not contain the required setting [model_id]"));
    }

    public void testUpdateServiceSettings_AllFields_ReturnsOriginalInstance() {
        var settingsMap = buildServiceSettingsMap(TEST_URL, TEST_MODEL_ID);

        var originalServiceSettings = new DeepSeekServiceSettings(INITIAL_TEST_MODEL_ID, ServiceUtils.createUri(INITIAL_TEST_URL));
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(updatedServiceSettings, sameInstance(originalServiceSettings));
    }

    public void testUpdateServiceSettings_EmptyMap_ReturnsOriginalInstance() {
        var originalServiceSettings = new DeepSeekServiceSettings(INITIAL_TEST_MODEL_ID, ServiceUtils.createUri(INITIAL_TEST_URL));
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, sameInstance(originalServiceSettings));
    }

    public void testToXContent_AllFields_WritesAllValues() throws IOException {
        var serviceSettings = new DeepSeekServiceSettings(TEST_MODEL_ID, ServiceUtils.createUri(TEST_URL));

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
        var serviceSettings = new DeepSeekServiceSettings(TEST_MODEL_ID, null);

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
    protected Writeable.Reader<DeepSeekServiceSettings> instanceReader() {
        return DeepSeekServiceSettings::new;
    }

    @Override
    protected DeepSeekServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected DeepSeekServiceSettings mutateInstance(DeepSeekServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        URI uri = instance.uri();
        var uriString = uri == null ? null : uri.toString();
        switch (randomInt(1)) {
            case 0 -> uriString = randomValueOtherThan(uriString, () -> randomAlphaOfLengthOrNull(8));
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new DeepSeekServiceSettings(modelId, ServiceUtils.createOptionalUri(uriString));
    }

    @Override
    protected DeepSeekServiceSettings mutateInstanceForVersion(DeepSeekServiceSettings instance, TransportVersion version) {
        return instance;
    }
}
