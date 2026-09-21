/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import static org.hamcrest.Matchers.is;

public class OciGenAiRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<OciGenAiRerankServiceSettings> {

    public static OciGenAiRerankServiceSettings createRandom() {
        var useUrl = randomBoolean();
        return new OciGenAiRerankServiceSettings(
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
        var settings = OciGenAiRerankServiceSettings.fromMap(
            OciGenAiTestUtils.serviceSettingsMap("cohere.rerank-v3.5"),
            ConfigurationParseContext.REQUEST
        );

        assertThat(settings.modelId(), is("cohere.rerank-v3.5"));
        assertThat(settings.region(), is(OciGenAiTestUtils.REGION_VALUE));
        assertThat(settings.compartmentId(), is(OciGenAiTestUtils.COMPARTMENT_ID));
    }

    public void testUpdateServiceSettings_UpdatesRateLimit() {
        var settings = createRandom();

        var updated = settings.updateServiceSettings(RateLimitSettingsTests.addRateLimitSettingsToMap(new HashMap<>(), 12));

        assertThat(updated.rateLimitSettings(), is(new RateLimitSettings(12)));
        assertThat(updated.common().withRateLimitSettings(settings.rateLimitSettings()), is(settings.common()));
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
