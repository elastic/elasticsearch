/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.example.customsettings;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.example.customsettings.ExampleCustomSettingsConfig.VALIDATED_SETTING;

/**
 * {@link ExampleCustomSettingsConfigTests} is a unit test class for {@link ExampleCustomSettingsConfig}.
 * <p>
 * It's a JUnit test class that extends {@link ESTestCase} which provides useful methods for testing.
 * <p>
 * The tests can be executed in the IDE or using the command: ./gradlew :example-plugins:custom-settings:test
 */
public class ExampleCustomSettingsConfigTests extends ESTestCase {

    public void testValidatedSetting() {
        final String expected = randomAlphaOfLengthBetween(1, 5);
        final String actual = VALIDATED_SETTING.get(Settings.builder().put(VALIDATED_SETTING.getKey(), expected).build());
        assertEquals(expected, actual);

        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            VALIDATED_SETTING.get(Settings.builder().put("custom.validated", "it's forbidden").build()));
        assertEquals("Setting must not contain [forbidden]", exception.getMessage());
    }
}
