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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class HuggingFaceElserSecretSettingsTests extends AbstractWireSerializingTestCase<HuggingFaceElserSecretSettings> {

    public static HuggingFaceElserSecretSettings createRandom() {
        return new HuggingFaceElserSecretSettings(new SecureString(randomAlphaOfLength(15).toCharArray()));
    }

    public void testFromMap() {
        var apiKey = "abc";
        var serviceSettings = HuggingFaceElserSecretSettings.fromMap(new HashMap<>(Map.of(HuggingFaceElserSecretSettings.API_KEY, apiKey)));

        assertThat(new HuggingFaceElserSecretSettings(new SecureString(apiKey.toCharArray())), is(serviceSettings));
    }

    public void testFromMap_MissingApiKey_ThrowsError() {
        var thrownException = expectThrows(ValidationException.class, () -> HuggingFaceElserSecretSettings.fromMap(new HashMap<>()));

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("[secret_settings] does not contain the required setting [%s]", HuggingFaceElserSecretSettings.API_KEY)
            )
        );
    }

    public void testFromMap_EmptyApiKey_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceElserSecretSettings.fromMap(new HashMap<>(Map.of(HuggingFaceElserSecretSettings.API_KEY, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[secret_settings] Invalid value empty string. [%s] must be a non-empty string",
                    HuggingFaceElserSecretSettings.API_KEY
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<HuggingFaceElserSecretSettings> instanceReader() {
        return HuggingFaceElserSecretSettings::new;
    }

    @Override
    protected HuggingFaceElserSecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected HuggingFaceElserSecretSettings mutateInstance(HuggingFaceElserSecretSettings instance) throws IOException {
        return createRandom();
    }
}
