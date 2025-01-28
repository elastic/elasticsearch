/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

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

public class DefaultSecretSettingsTests extends AbstractWireSerializingTestCase<DefaultSecretSettings> {

    public static DefaultSecretSettings createRandom() {
        return new DefaultSecretSettings(new SecureString(randomAlphaOfLength(15).toCharArray()));
    }

    public void testNewSecretSettings() {
        DefaultSecretSettings initialSettings = createRandom();
        DefaultSecretSettings newSettings = createRandom();
        DefaultSecretSettings finalSettings = (DefaultSecretSettings) initialSettings.newSecretSettings(
            Map.of(DefaultSecretSettings.API_KEY, newSettings.apiKey().toString())
        );
        assertEquals(newSettings, finalSettings);
    }

    public void testFromMap() {
        var apiKey = "abc";
        var serviceSettings = DefaultSecretSettings.fromMap(new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, apiKey)));

        assertThat(new DefaultSecretSettings(new SecureString(apiKey.toCharArray())), is(serviceSettings));
    }

    public void testFromMap_ReturnsNull_WhenMapIsNull() {
        assertNull(DefaultSecretSettings.fromMap(null));
    }

    public void testFromMap_MissingApiKey_ThrowsError() {
        var thrownException = expectThrows(ValidationException.class, () -> DefaultSecretSettings.fromMap(new HashMap<>()));

        assertThat(
            thrownException.getMessage(),
            containsString(Strings.format("[secret_settings] does not contain the required setting [%s]", DefaultSecretSettings.API_KEY))
        );
    }

    public void testFromMap_EmptyApiKey_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> DefaultSecretSettings.fromMap(new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, "")))
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "[secret_settings] Invalid value empty string. [%s] must be a non-empty string",
                    DefaultSecretSettings.API_KEY
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<DefaultSecretSettings> instanceReader() {
        return DefaultSecretSettings::new;
    }

    @Override
    protected DefaultSecretSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected DefaultSecretSettings mutateInstance(DefaultSecretSettings instance) throws IOException {
        return randomValueOtherThan(instance, DefaultSecretSettingsTests::createRandom);
    }

    public static Map<String, Object> getSecretSettingsMap(String apiKey) {
        return new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, apiKey));
    }
}
