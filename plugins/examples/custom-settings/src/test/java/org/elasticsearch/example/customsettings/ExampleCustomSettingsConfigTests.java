/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
