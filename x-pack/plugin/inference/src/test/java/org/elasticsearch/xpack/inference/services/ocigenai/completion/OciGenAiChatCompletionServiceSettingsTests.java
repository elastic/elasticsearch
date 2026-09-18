/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiChatApiFormat;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.REGION_VALUE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OciGenAiChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    OciGenAiChatCompletionServiceSettings> {

    public static OciGenAiChatCompletionServiceSettings createRandom() {
        var useUrl = randomBoolean();
        return new OciGenAiChatCompletionServiceSettings(
            new OciGenAiServiceSettings.CommonSettings(
                useUrl && randomBoolean() ? null : randomAlphaOfLength(8),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomBoolean() ? null : randomAlphaOfLength(10),
                useUrl ? URI.create("https://" + randomAlphaOfLength(8) + ".example.com") : null,
                RateLimitSettingsTests.createRandom()
            )
        );
    }

    public void testFromMap_Request_ParsesFields() {
        var map = OciGenAiTestUtils.serviceSettingsMap("meta.llama-3.3-70b-instruct");
        RateLimitSettingsTests.addRateLimitSettingsToMap(map, 7);

        var settings = OciGenAiChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(settings.region(), is(REGION_VALUE));
        assertThat(settings.compartmentId(), is(COMPARTMENT_ID));
        assertThat(settings.modelId(), is("meta.llama-3.3-70b-instruct"));
        assertThat(settings.apiFormat(), is(OciGenAiChatApiFormat.GENERIC));
        assertThat(settings.rateLimitSettings(), is(new RateLimitSettings(7)));
        assertTrue(map.isEmpty());
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
        map.remove("model_id");

        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("[service_settings] does not contain the required setting [model_id]"));
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
