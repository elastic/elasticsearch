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
package org.elasticsearch.common.settings;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.rest.RestRequest;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;

public class SettingsFilterTests extends ESTestCase {

    @Test
    public void testAddingAndRemovingFilters() {
        SettingsFilter settingsFilter = new SettingsFilter(Settings.EMPTY);
        settingsFilter.addFilter("foo");
        settingsFilter.addFilter("bar");
        settingsFilter.addFilter("baz");
        assertThat(settingsFilter.getPatterns(), equalTo("foo,bar,baz"));

        settingsFilter.removeFilter("bar");
        assertThat(settingsFilter.getPatterns(), equalTo("foo,baz"));

        settingsFilter.removeFilter("bar");
        settingsFilter.removeFilter("foo");
        settingsFilter.removeFilter("baz");

        assertThat(settingsFilter.getPatterns(), equalTo(""));
    }

    @Test
    public void testSettingsFiltering() throws IOException {

        testFiltering(Settings.builder()
                        .put("foo", "foo_test")
                        .put("foo1", "foo1_test")
                        .put("bar", "bar_test")
                        .put("bar1", "bar1_test")
                        .put("bar.2", "bar2_test")
                        .build(),
                Settings.builder()
                        .put("foo1", "foo1_test")
                        .build(),
                "foo,bar*"
        );

        testFiltering(Settings.builder()
                        .put("foo", "foo_test")
                        .put("foo1", "foo1_test")
                        .put("bar", "bar_test")
                        .put("bar1", "bar1_test")
                        .put("bar.2", "bar2_test")
                        .build(),
                Settings.builder()
                        .put("foo", "foo_test")
                        .put("foo1", "foo1_test")
                        .build(),
                "bar*"
        );

        testFiltering(Settings.builder()
                        .put("foo", "foo_test")
                        .put("foo1", "foo1_test")
                        .put("bar", "bar_test")
                        .put("bar1", "bar1_test")
                        .put("bar.2", "bar2_test")
                        .build(),
                Settings.builder()
                        .build(),
                "foo,bar*,foo*"
        );

        testFiltering(Settings.builder()
                        .put("foo", "foo_test")
                        .put("bar", "bar_test")
                        .put("baz", "baz_test")
                        .build(),
                Settings.builder()
                        .put("foo", "foo_test")
                        .put("bar", "bar_test")
                        .put("baz", "baz_test")
                        .build()
        );
    }

    private void testFiltering(Settings source, Settings filtered, String... patterns) throws IOException {
        SettingsFilter settingsFilter = new SettingsFilter(Settings.EMPTY);
        for (String pattern : patterns) {
            settingsFilter.addFilter(pattern);
        }

        // Test using direct filtering
        Settings filteredSettings = SettingsFilter.filterSettings(settingsFilter.getPatterns(), source);
        assertThat(filteredSettings.getAsMap().entrySet(), equalTo(filtered.getAsMap().entrySet()));

        // Test using toXContent filtering
        RestRequest request = new FakeRestRequest();
        settingsFilter.addFilterSettingParams(request);
        XContentBuilder xContentBuilder = XContentBuilder.builder(JsonXContent.jsonXContent);
        xContentBuilder.startObject();
        source.toXContent(xContentBuilder, request);
        xContentBuilder.endObject();
        String filteredSettingsString = xContentBuilder.string();
        filteredSettings = Settings.builder().loadFromSource(filteredSettingsString).build();
        assertThat(filteredSettings.getAsMap().entrySet(), equalTo(filtered.getAsMap().entrySet()));
    }
}
