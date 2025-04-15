/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.rerank;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class HuggingFaceRerankServiceSettingsTests extends AbstractWireSerializingTestCase<HuggingFaceRerankServiceSettings> {

    public static HuggingFaceRerankServiceSettings createRandom() {
        return new HuggingFaceRerankServiceSettings(randomAlphaOfLength(15));
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var serviceSettings = HuggingFaceRerankServiceSettings.fromMap(
            new HashMap<>(Map.of(HuggingFaceRerankServiceSettings.URL, url)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(new HuggingFaceRerankServiceSettings(url), is(serviceSettings));
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceRerankServiceSettings.fromMap(
                new HashMap<>(Map.of(HuggingFaceRerankServiceSettings.URL, "")),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    HuggingFaceRerankServiceSettings.URL
                )
            )
        );
    }

    public void testFromMap_MissingUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceRerankServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT)
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] does not contain the required setting [%s];",
                    HuggingFaceRerankServiceSettings.URL
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceRerankServiceSettings.fromMap(
                new HashMap<>(Map.of(HuggingFaceRerankServiceSettings.URL, url)),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s]",
                    url,
                    HuggingFaceRerankServiceSettings.URL
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new HuggingFaceRerankServiceSettings(ServiceUtils.createUri("url"), new RateLimitSettings(3));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = org.elasticsearch.common.Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"url":"url","max_input_tokens":512,"rate_limit":{"requests_per_minute":3}}"""));
    }

    @Override
    protected Writeable.Reader<HuggingFaceRerankServiceSettings> instanceReader() {
        return HuggingFaceRerankServiceSettings::new;
    }

    @Override
    protected HuggingFaceRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected HuggingFaceRerankServiceSettings mutateInstance(HuggingFaceRerankServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, HuggingFaceRerankServiceSettingsTests::createRandom);
    }
}
