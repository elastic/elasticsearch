/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiChatApiFormat;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiSecretSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.REGION_VALUE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OciGenAiChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    OciGenAiChatCompletionServiceSettings> {

    public static OciGenAiChatCompletionServiceSettings createRandom() {
        return new OciGenAiChatCompletionServiceSettings(OciGenAiTestUtils.randomCommonSettings());
    }

    public void testFromMap_Request_ParsesFields() {
        var map = OciGenAiTestUtils.serviceSettingsMap("meta.llama-3.3-70b-instruct");
        RateLimitSettingsTests.addRateLimitSettingsToMap(map, 7);

        var settings = OciGenAiChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(settings.region(), is(REGION_VALUE));
        assertThat(settings.compartmentId(), is(COMPARTMENT_ID));
        assertThat(settings.modelId(), is("meta.llama-3.3-70b-instruct"));
        assertThat(settings.apiVersion(), is(OciGenAiUtils.DEFAULT_API_VERSION));
        assertThat(settings.apiFormat(), is(OciGenAiChatApiFormat.GENERIC));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(7)));
    }

    public void testFromMap_Request_ParsesApiVersion() {
        var map = OciGenAiTestUtils.serviceSettingsMap("meta.llama-3.3-70b-instruct");
        map.put(OciGenAiServiceFields.API_VERSION, "20260101");

        var settings = OciGenAiChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(settings.apiVersion(), is("20260101"));
    }

    public void testFromMap_Request_IgnoresTheSigningKeyFields() {
        // in requests the signing key shares the service_settings block and is extracted separately by the secret settings
        var map = OciGenAiTestUtils.serviceSettingsMap("meta.llama-3.3-70b-instruct");
        map.putAll(OciGenAiTestUtils.secretSettingsMap());

        var settings = OciGenAiChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(settings.modelId(), is("meta.llama-3.3-70b-instruct"));
        assertThat(map.get(OciGenAiSecretSettings.PRIVATE_KEY), is(OciGenAiTestUtils.privateKeyPem()));
    }

    public void testFromMap_Request_ThrowsOnUnknownField() {
        var map = OciGenAiTestUtils.serviceSettingsMap("meta.llama-3.3-70b-instruct");
        map.put("extra_key", "value");

        var exception = expectThrows(
            XContentParseException.class,
            () -> OciGenAiChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("[service_settings] unknown field [extra_key]"));
    }

    public void testFromMap_Persistent_IgnoresUnknownFields() {
        var map = OciGenAiTestUtils.serviceSettingsMap("meta.llama-3.3-70b-instruct");
        map.put("extra_key", "value");

        var settings = OciGenAiChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(settings.modelId(), is("meta.llama-3.3-70b-instruct"));
    }

    public void testApiFormat_IsCohereForCohereModels() {
        var settings = OciGenAiChatCompletionServiceSettings.fromMap(
            OciGenAiTestUtils.serviceSettingsMap("cohere.command-a-03-2025"),
            ConfigurationParseContext.REQUEST
        );

        assertThat(settings.apiFormat(), is(OciGenAiChatApiFormat.COHERE));
    }

    public void testFromMap_ThrowsWhenModelIdIsMissing() {
        var map = OciGenAiTestUtils.serviceSettingsMap("model");
        map.remove(ServiceFields.MODEL_ID);

        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> OciGenAiChatCompletionServiceSettings.fromMap(map, randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(exception.getMessage(), is("[service_settings] does not contain the required setting [model_id]"));
    }

    public void testFromMap_ThrowsWhenModelIdIsEmpty() {
        var map = OciGenAiTestUtils.serviceSettingsMap("");

        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> OciGenAiChatCompletionServiceSettings.fromMap(map, randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(exception.getMessage(), is("[service_settings] Invalid value empty string. [model_id] must be a non-empty string"));
    }

    public void testFromMap_ThrowsWhenApiVersionIsEmpty() {
        var map = OciGenAiTestUtils.serviceSettingsMap("meta.llama-3.3-70b-instruct");
        map.put(OciGenAiServiceFields.API_VERSION, "");

        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> OciGenAiChatCompletionServiceSettings.fromMap(map, randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(exception.getMessage(), is("[service_settings] Invalid value empty string. [api_version] must be a non-empty string"));
    }

    public void testUpdateServiceSettings_UpdatesRateLimitOnly() {
        var settings = OciGenAiChatCompletionServiceSettings.fromMap(
            OciGenAiTestUtils.serviceSettingsMap("meta.llama-3.3-70b-instruct"),
            ConfigurationParseContext.REQUEST
        );

        var updated = settings.updateServiceSettings(RateLimitSettingsTests.addRateLimitSettingsToMap(new HashMap<>(), 99));

        assertThat(updated.rateLimitSettings(), is(new RateLimitSettings(99)));
        assertThat(updated.modelId(), is(settings.modelId()));
        assertThat(updated.region(), is(settings.region()));
    }

    public void testUpdateServiceSettings_ExplicitNullResetsTheRateLimit() {
        var settings = new OciGenAiChatCompletionServiceSettings(
            OciGenAiTestUtils.commonSettings(REGION_VALUE, COMPARTMENT_ID, "xai.grok-4", null, null, new RateLimitSettings(5))
        );

        var update = new HashMap<String, Object>();
        update.put(RateLimitSettings.FIELD_NAME, null);

        var updated = settings.updateServiceSettings(update);

        assertThat(updated.rateLimitSettings(), is(OciGenAiChatCompletionServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS));
    }

    public void testUpdateServiceSettings_IgnoresTheSigningKeyFields() {
        var settings = createRandom();

        var updated = settings.updateServiceSettings(OciGenAiTestUtils.secretSettingsMap());

        assertThat(updated, is(settings));
    }

    public void testUpdateServiceSettings_RejectsImmutableFields() {
        var settings = createRandom();
        var update = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, "other-model"));

        var exception = expectThrows(XContentParseException.class, () -> settings.updateServiceSettings(update));

        assertThat(exception.getMessage(), containsString("[service_settings] unknown field [model_id]"));
    }

    public void testToXContent() throws IOException {
        var settings = new OciGenAiChatCompletionServiceSettings(
            OciGenAiTestUtils.commonSettings(REGION_VALUE, COMPARTMENT_ID, "xai.grok-4", null, null, new RateLimitSettings(5))
        );

        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);

        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "region": "us-chicago-1",
                "compartment_id": "%s",
                "model_id": "xai.grok-4",
                "api_version": "20231130",
                "rate_limit": { "requests_per_minute": 5 }
            }
            """, COMPARTMENT_ID))));
    }

    @Override
    protected Writeable.Reader<OciGenAiChatCompletionServiceSettings> instanceReader() {
        return OciGenAiChatCompletionServiceSettings::new;
    }

    @Override
    protected OciGenAiChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OciGenAiChatCompletionServiceSettings mutateInstance(OciGenAiChatCompletionServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, OciGenAiChatCompletionServiceSettingsTests::createRandom);
    }

    @Override
    protected OciGenAiChatCompletionServiceSettings mutateInstanceForVersion(
        OciGenAiChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
