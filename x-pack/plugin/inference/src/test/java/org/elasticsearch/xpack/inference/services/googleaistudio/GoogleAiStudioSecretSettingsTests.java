/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class GoogleAiStudioSecretSettingsTests extends AbstractWireSerializingTestCase<GoogleAiStudioSecretSettings> {

    public static GoogleAiStudioSecretSettings createRandom() {
        return new GoogleAiStudioSecretSettings(randomSecureStringOfLength(15));
    }

    public void testFromMap() {
        var apiKey = "abc";
        var secretSettings = GoogleAiStudioSecretSettings.fromMap(new HashMap<>(Map.of(GoogleAiStudioSecretSettings.API_KEY, apiKey)));

        assertThat(new GoogleAiStudioSecretSettings(new SecureString(apiKey.toCharArray())), is(secretSettings));
    }

    public void testFromMap_ReturnsNull_WhenMapIsNull() {
        assertNull(GoogleAiStudioSecretSettings.fromMap(null));
    }

    public void testFromMap_ThrowsError_WhenApiKeyIsNull() {
        var throwException = expectThrows(ValidationException.class, () -> GoogleAiStudioSecretSettings.fromMap(new HashMap<>()));

        assertThat(throwException.getMessage(), containsString("[secret_settings] must have [api_key] set"));
    }

    public void testFromMap_ThrowsError_WhenApiKeyIsEmpty() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> GoogleAiStudioSecretSettings.fromMap(new HashMap<>(Map.of(GoogleAiStudioSecretSettings.API_KEY, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString("[secret_settings] Invalid value empty string. [api_key] must be a non-empty string")
        );
    }

    @Override
    protected Writeable.Reader<GoogleAiStudioSecretSettings> instanceReader() {
        return GoogleAiStudioSecretSettings::new;
    }

    @Override
    protected GoogleAiStudioSecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleAiStudioSecretSettings mutateInstance(GoogleAiStudioSecretSettings instance) throws IOException {
        return randomValueOtherThan(instance, GoogleAiStudioSecretSettingsTests::createRandom);
    }
}
