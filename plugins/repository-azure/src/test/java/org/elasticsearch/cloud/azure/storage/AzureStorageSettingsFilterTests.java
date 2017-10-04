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

package org.elasticsearch.cloud.azure.storage;

import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.plugin.repository.azure.AzureRepositoryPlugin;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;

import static org.hamcrest.Matchers.contains;

/**
 * TODO as we moved credentials to secure settings, we don't need anymore to keep this test in 7.x
 */
public class AzureStorageSettingsFilterTests extends ESTestCase {
    static final Settings settings = Settings.builder()
            .put("cloud.azure.storage.azure1.account", "myaccount1")
            .put("cloud.azure.storage.azure1.key", "mykey1")
            .put("cloud.azure.storage.azure1.default", true)
            .put("cloud.azure.storage.azure2.account", "myaccount2")
            .put("cloud.azure.storage.azure2.key", "mykey2")
            .put("cloud.azure.storage.azure3.account", "myaccount3")
            .put("cloud.azure.storage.azure3.key", "mykey3")
            .build();

    public void testSettingsFiltering() throws IOException {
        AzureRepositoryPlugin p = new AzureRepositoryPlugin(settings);
        SettingsModule module = new SettingsModule(Settings.EMPTY, p.getSettings(), p.getSettingsFilter());
        SettingsFilter settingsFilter = ModuleTestCase.bindAndGetInstance(module, SettingsFilter.class);

        // Test using direct filtering
        Settings filteredSettings = settingsFilter.filter(settings);
        assertThat(filteredSettings.keySet(), contains("cloud.azure.storage.azure1.default"));

        // Test using toXContent filtering
        RestRequest request = new FakeRestRequest();
        settingsFilter.addFilterSettingParams(request);
        XContentBuilder xContentBuilder = XContentBuilder.builder(JsonXContent.jsonXContent);
        xContentBuilder.startObject();
        settings.toXContent(xContentBuilder, request);
        xContentBuilder.endObject();
        String filteredSettingsString = xContentBuilder.string();
        filteredSettings = Settings.builder().loadFromSource(filteredSettingsString, xContentBuilder.contentType()).build();
        assertThat(filteredSettings.keySet(), contains("cloud.azure.storage.azure1.default"));
    }

}
