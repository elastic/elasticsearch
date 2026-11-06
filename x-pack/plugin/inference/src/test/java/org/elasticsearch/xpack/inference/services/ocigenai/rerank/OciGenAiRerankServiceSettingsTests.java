/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OciGenAiRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<OciGenAiRerankServiceSettings> {

    public static OciGenAiRerankServiceSettings createRandom() {
        return new OciGenAiRerankServiceSettings(OciGenAiTestUtils.randomCommonSettings());
    }

    public void testFromMap_Request_ParsesFields() {
        var settings = OciGenAiRerankServiceSettings.fromMap(
            OciGenAiTestUtils.serviceSettingsMap("cohere.rerank-v3.5"),
            ConfigurationParseContext.REQUEST
        );

        assertThat(settings.modelId(), is("cohere.rerank-v3.5"));
        assertThat(settings.region(), is(OciGenAiTestUtils.REGION_VALUE));
        assertThat(settings.compartmentId(), is(OciGenAiTestUtils.COMPARTMENT_ID));
        assertThat(settings.apiVersion(), is(OciGenAiUtils.DEFAULT_API_VERSION));
        assertThat(settings.rateLimitSettings(), is(OciGenAiRerankServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS));
    }

    public void testFromMap_Request_ParsesApiVersion() {
        var map = OciGenAiTestUtils.serviceSettingsMap("cohere.rerank-v3.5");
        map.put(OciGenAiServiceFields.API_VERSION, "20260101");

        var settings = OciGenAiRerankServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(settings.apiVersion(), is("20260101"));
    }

    public void testFromMap_Request_ThrowsOnUnknownField() {
        var map = OciGenAiTestUtils.serviceSettingsMap("cohere.rerank-v3.5");
        map.put("extra_key", "value");

        var exception = expectThrows(
            XContentParseException.class,
            () -> OciGenAiRerankServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), containsString("[service_settings] unknown field [extra_key]"));
    }

    public void testFromMap_ThrowsWhenCompartmentIdIsMissing() {
        var map = OciGenAiTestUtils.serviceSettingsMap("cohere.rerank-v3.5");
        map.remove(OciGenAiServiceFields.COMPARTMENT_ID);

        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> OciGenAiRerankServiceSettings.fromMap(map, randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(exception.getMessage(), is("[service_settings] does not contain the required setting [compartment_id]"));
    }

    public void testUpdateServiceSettings_UpdatesRateLimit() {
        var settings = createRandom();

        var updated = settings.updateServiceSettings(RateLimitSettingsTests.addRateLimitSettingsToMap(new HashMap<>(), 12));

        assertThat(updated.rateLimitSettings(), is(new RateLimitSettings(12)));
        assertThat(updated.common().withRateLimitSettings(settings.rateLimitSettings()), is(settings.common()));
    }

    public void testUpdateServiceSettings_RejectsImmutableFields() {
        var settings = createRandom();
        var update = new HashMap<String, Object>(Map.of(OciGenAiServiceFields.REGION, "us-ashburn-1"));

        var exception = expectThrows(XContentParseException.class, () -> settings.updateServiceSettings(update));

        assertThat(exception.getMessage(), containsString("[service_settings] unknown field [region]"));
    }

    @Override
    protected Writeable.Reader<OciGenAiRerankServiceSettings> instanceReader() {
        return OciGenAiRerankServiceSettings::new;
    }

    @Override
    protected OciGenAiRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OciGenAiRerankServiceSettings mutateInstance(OciGenAiRerankServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, OciGenAiRerankServiceSettingsTests::createRandom);
    }

    @Override
    protected OciGenAiRerankServiceSettings mutateInstanceForVersion(OciGenAiRerankServiceSettings instance, TransportVersion version) {
        return instance;
    }
}
