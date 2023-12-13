/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class HuggingFaceServiceSettingsTests extends AbstractWireSerializingTestCase<HuggingFaceServiceSettings> {

    public static HuggingFaceServiceSettings createRandom() {
        return new HuggingFaceServiceSettings(randomAlphaOfLength(15));
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var serviceSettings = HuggingFaceServiceSettings.fromMap(new HashMap<>(Map.of(HuggingFaceServiceSettings.URL, url)));

        assertThat(serviceSettings, is(new HuggingFaceServiceSettings(url)));
    }

    public void testFromMap_MissingUrl_ThrowsError() {
        var thrownException = expectThrows(ValidationException.class, () -> HuggingFaceServiceSettings.fromMap(new HashMap<>()));

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] does not contain the required setting [%s];",
                    HuggingFaceServiceSettings.URL
                )
            )
        );
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceServiceSettings.fromMap(new HashMap<>(Map.of(HuggingFaceServiceSettings.URL, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    HuggingFaceServiceSettings.URL
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceServiceSettings.fromMap(new HashMap<>(Map.of(HuggingFaceServiceSettings.URL, url)))
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s];",
                    url,
                    HuggingFaceServiceSettings.URL
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<HuggingFaceServiceSettings> instanceReader() {
        return HuggingFaceServiceSettings::new;
    }

    @Override
    protected HuggingFaceServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected HuggingFaceServiceSettings mutateInstance(HuggingFaceServiceSettings instance) throws IOException {
        return createRandom();
    }

    public static Map<String, Object> getServiceSettingsMap(String url) {
        var map = new HashMap<String, Object>();

        map.put(HuggingFaceServiceSettings.URL, url);

        return map;
    }
}
