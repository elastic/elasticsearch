/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class HuggingFaceElserServiceSettingsTests extends AbstractWireSerializingTestCase<HuggingFaceElserServiceSettings> {

    public static HuggingFaceElserServiceSettings createRandom() {
        return new HuggingFaceElserServiceSettings(randomAlphaOfLength(15));
    }

    public void testFromMap() {
        var url = "https://www.abc.com";
        var serviceSettings = HuggingFaceElserServiceSettings.fromMap(new HashMap<>(Map.of(HuggingFaceElserServiceSettings.URL, url)));

        assertThat(new HuggingFaceElserServiceSettings(url), is(serviceSettings));
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceElserServiceSettings.fromMap(new HashMap<>(Map.of(HuggingFaceElserServiceSettings.URL, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    HuggingFaceElserServiceSettings.URL
                )
            )
        );
    }

    public void testFromMap_MissingUrl_ThrowsError() {
        var thrownException = expectThrows(ValidationException.class, () -> HuggingFaceElserServiceSettings.fromMap(new HashMap<>()));

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] does not contain the required setting [%s];",
                    HuggingFaceElserServiceSettings.URL
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceElserServiceSettings.fromMap(new HashMap<>(Map.of(HuggingFaceElserServiceSettings.URL, url)))
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s];",
                    url,
                    HuggingFaceElserServiceSettings.URL
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<HuggingFaceElserServiceSettings> instanceReader() {
        return HuggingFaceElserServiceSettings::new;
    }

    @Override
    protected HuggingFaceElserServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected HuggingFaceElserServiceSettings mutateInstance(HuggingFaceElserServiceSettings instance) throws IOException {
        return createRandom();
    }
}
