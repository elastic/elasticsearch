/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.settings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

public class MarvelSettingTests extends ESTestCase {
    public void testBooleanMarvelSetting() {
        String name = randomAsciiOfLength(10);
        String description = randomAsciiOfLength(20);
        Boolean defaultValue = null;
        if (randomBoolean()) {
            defaultValue = randomBoolean();
        }

        MarvelSetting.BooleanSetting setting = MarvelSetting.booleanSetting(name, defaultValue, description);
        assertThat(setting.getName(), equalTo(name));
        assertThat(setting.getDescription(), equalTo(description));
        assertThat(setting.getValue(), equalTo(defaultValue));

        setting.onRefresh(settingsBuilder().put(name, Boolean.FALSE).build());
        assertFalse(setting.getValue());

        setting.onRefresh(settingsBuilder().put(name, Boolean.TRUE).build());
        assertTrue(setting.getValue());
    }

    public void testTimeValueMarvelSetting() {
        String name = randomAsciiOfLength(10);
        String description = randomAsciiOfLength(20);
        TimeValue defaultValue = null;
        if (randomBoolean()) {
            defaultValue = newRandomTimeValue();
        }

        MarvelSetting.TimeValueSetting setting = MarvelSetting.timeSetting(name, defaultValue, description);
        assertThat(setting.getName(), equalTo(name));
        assertThat(setting.getDescription(), equalTo(description));
        if (defaultValue == null) {
            assertNull(setting.getValue());
        } else {
            assertThat(setting.getValue().millis(), equalTo(defaultValue.millis()));
        }

        setting.onRefresh(settingsBuilder().put(name, 15000L).build());
        assertThat(setting.getValue().millis(), equalTo(15000L));

        TimeValue updated = newRandomTimeValue();
        setting.onRefresh(settingsBuilder().put(name, updated.toString()).build());
        assertThat(setting.getValue().millis(), equalTo(updated.millis()));

        updated = newRandomTimeValue();
        setting.onRefresh(settingsBuilder().put(name, updated.toString()).build());
        assertThat(setting.getValue().millis(), equalTo(updated.millis()));
    }

    public void testStringMarvelSetting() {
        String name = randomAsciiOfLength(10);
        String description = randomAsciiOfLength(20);
        String defaultValue = null;
        if (randomBoolean()) {
            defaultValue = randomAsciiOfLength(15);
        }

        MarvelSetting.StringSetting setting = MarvelSetting.stringSetting(name, defaultValue, description);
        assertThat(setting.getName(), equalTo(name));
        assertThat(setting.getDescription(), equalTo(description));
        if (defaultValue == null) {
            assertNull(setting.getValue());
        } else {
            assertThat(setting.getValue(), equalTo(defaultValue));
        }

        setting.onRefresh(settingsBuilder().build());
        assertThat(setting.getValue(), equalTo(defaultValue));

        String updated = randomAsciiOfLength(15);
        setting.onRefresh(settingsBuilder().put(name, updated).build());
        assertThat(setting.getValue(), equalTo(updated));

        updated = randomAsciiOfLength(15);
        setting.onRefresh(settingsBuilder().put(name, updated).build());
        assertThat(setting.getValue(), equalTo(updated));
    }

    public void testStringArrayMarvelSetting() {
        String name = randomAsciiOfLength(10);
        String description = randomAsciiOfLength(20);
        String[] defaultValue = null;
        if (randomBoolean()) {
            defaultValue = randomStringArray();
        }

        MarvelSetting.StringArraySetting setting = MarvelSetting.arraySetting(name, defaultValue, description);
        assertThat(setting.getName(), equalTo(name));
        assertThat(setting.getDescription(), equalTo(description));
        if (defaultValue == null) {
            assertNull(setting.getValue());
        } else {
            assertArrayEquals(setting.getValue(), defaultValue);
        }

        setting.onRefresh(settingsBuilder().build());
        assertArrayEquals(setting.getValue(), defaultValue);

        String[] updated = randomStringArray();
        setting.onRefresh(settingsBuilder().put(name, Strings.arrayToCommaDelimitedString(updated)).build());
        assertArrayEquals(setting.getValue(), updated);

        updated = randomStringArray();
        setting.onRefresh(settingsBuilder().put(name, Strings.arrayToCommaDelimitedString(updated)).build());
        assertArrayEquals(setting.getValue(), updated);
    }

    private TimeValue newRandomTimeValue() {
        return TimeValue.parseTimeValue(randomFrom("10ms", "1.5s", "1.5m", "1.5h", "1.5d", "1000d"), null, getClass().getSimpleName() + ".unit");
    }

    private String[] randomStringArray() {
        int n = randomIntBetween(1, 5);
        String[] values = new String[n];
        for (int i = 0; i < n; i++) {
            values[i] = randomAsciiOfLength(5);
        }
        return values;
    }
}
