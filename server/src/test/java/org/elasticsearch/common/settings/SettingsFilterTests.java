/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.settings;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;

public class SettingsFilterTests extends ESTestCase {
    public void testAddingAndRemovingFilters() {
        HashSet<String> hashSet = new HashSet<>(Arrays.asList("foo", "bar", "baz"));
        SettingsFilter settingsFilter = new SettingsFilter(hashSet);
        assertEquals(settingsFilter.getPatterns(), hashSet);
    }

    public void testSettingsFiltering() throws IOException {

        testFiltering(
            Settings.builder()
                .put("foo", "foo_test")
                .put("foo1", "foo1_test")
                .put("bar", "bar_test")
                .put("bar1", "bar1_test")
                .put("bar.2", "bar2_test")
                .build(),
            Settings.builder().put("foo1", "foo1_test").build(),
            "foo",
            "bar*"
        );

        testFiltering(
            Settings.builder()
                .put("foo", "foo_test")
                .put("foo1", "foo1_test")
                .put("bar", "bar_test")
                .put("bar1", "bar1_test")
                .put("bar.2", "bar2_test")
                .build(),
            Settings.builder().put("foo", "foo_test").put("foo1", "foo1_test").build(),
            "bar*"
        );

        testFiltering(
            Settings.builder()
                .put("foo", "foo_test")
                .put("foo1", "foo1_test")
                .put("bar", "bar_test")
                .put("bar1", "bar1_test")
                .put("bar.2", "bar2_test")
                .build(),
            Settings.builder().build(),
            "foo",
            "bar*",
            "foo*"
        );

        testFiltering(
            Settings.builder().put("foo", "foo_test").put("bar", "bar_test").put("baz", "baz_test").build(),
            Settings.builder().put("foo", "foo_test").put("bar", "bar_test").put("baz", "baz_test").build()
        );

        testFiltering(
            Settings.builder().put("a.b.something.d", "foo_test").put("a.b.something.c", "foo1_test").build(),
            Settings.builder().put("a.b.something.c", "foo1_test").build(),
            "a.b.*.d"
        );
    }

    public void testFilteredSettingIsNotLogged() throws Exception {
        Settings oldSettings = Settings.builder().put("key", "old").build();
        Settings newSettings = Settings.builder().put("key", "new").build();

        Setting<String> filteredSetting = Setting.simpleString("key", Property.Filtered);
        assertExpectedLogMessages(
            (testLogger) -> Setting.logSettingUpdate(filteredSetting, newSettings, oldSettings, testLogger),
            new MockLog.SeenEventExpectation("secure logging", "org.elasticsearch.test", Level.INFO, "updating [key]"),
            new MockLog.UnseenEventExpectation("unwanted old setting name", "org.elasticsearch.test", Level.INFO, "*old*"),
            new MockLog.UnseenEventExpectation("unwanted new setting name", "org.elasticsearch.test", Level.INFO, "*new*")
        );
    }

    public void testIndexScopeSettingUpdateLoggedAsDebug() throws Exception {
        Settings oldSettings = Settings.builder().put("key", "old").build();
        Settings newSettings = Settings.builder().put("key", "new").build();

        // With INFO log level nothing gets logged.
        Setting<String> filteredSetting = Setting.simpleString("key", Property.IndexScope);
        assertExpectedLogMessages((testLogger) -> Setting.logSettingUpdate(filteredSetting, newSettings, oldSettings, testLogger));

        try {
            // With DEBUG log level something gets logged
            Configurator.setLevel("org.elasticsearch.test", Level.DEBUG);
            assertExpectedLogMessages(
                (logger) -> Setting.logSettingUpdate(filteredSetting, newSettings, oldSettings, logger),
                new MockLog.SeenEventExpectation(
                    "regular logging",
                    "org.elasticsearch.test",
                    Level.DEBUG,
                    "updating [key] from [old] to [new]"
                )
            );
        } finally {
            Configurator.setLevel("org.elasticsearch.test", Level.INFO);
        }
    }

    public void testRegularSettingUpdateIsFullyLogged() throws Exception {
        Settings oldSettings = Settings.builder().put("key", "old").build();
        Settings newSettings = Settings.builder().put("key", "new").build();

        Setting<String> regularSetting = Setting.simpleString("key");
        assertExpectedLogMessages(
            (testLogger) -> Setting.logSettingUpdate(regularSetting, newSettings, oldSettings, testLogger),
            new MockLog.SeenEventExpectation("regular logging", "org.elasticsearch.test", Level.INFO, "updating [key] from [old] to [new]")
        );
    }

    private void assertExpectedLogMessages(Consumer<Logger> consumer, MockLog.LoggingExpectation... expectations) {
        Logger testLogger = LogManager.getLogger("org.elasticsearch.test");
        try (var mockLog = MockLog.capture("org.elasticsearch.test")) {
            Arrays.stream(expectations).forEach(mockLog::addExpectation);
            consumer.accept(testLogger);
            mockLog.assertAllExpectationsMatched();
        }
    }

    private void testFiltering(Settings source, Settings filtered, String... patterns) throws IOException {
        SettingsFilter settingsFilter = new SettingsFilter(Arrays.asList(patterns));

        // Test using direct filtering
        Settings filteredSettings = settingsFilter.filter(source);
        assertThat(filteredSettings, equalTo(filtered));

        // Test using toXContent filtering
        RestRequest request = new FakeRestRequest();
        settingsFilter.addFilterSettingParams(request);
        XContentBuilder xContentBuilder = XContentBuilder.builder(JsonXContent.jsonXContent);
        xContentBuilder.startObject();
        source.toXContent(xContentBuilder, request);
        xContentBuilder.endObject();
        String filteredSettingsString = Strings.toString(xContentBuilder);
        filteredSettings = Settings.builder().loadFromSource(filteredSettingsString, xContentBuilder.contentType()).build();
        assertThat(filteredSettings, equalTo(filtered));
    }
}
