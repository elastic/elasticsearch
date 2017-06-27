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

package org.elasticsearch.repositories.azure;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.cloud.azure.storage.AzureStorageSettings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;

import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AzureSettingsParserTests extends LuceneTestCase {

    public void testParseTwoSettingsExplicitDefault() {
        Settings settings = Settings.builder()
                .put("cloud.azure.storage.azure1.account", "myaccount1")
                .put("cloud.azure.storage.azure1.key", "mykey1")
                .put("cloud.azure.storage.azure1.default", true)
                .put("cloud.azure.storage.azure2.account", "myaccount2")
                .put("cloud.azure.storage.azure2.key", "mykey2")
                .build();

        Tuple<AzureStorageSettings, Map<String, AzureStorageSettings>> tuple = AzureStorageSettings.parse(settings);
        assertThat(tuple.v1(), notNullValue());
        assertThat(tuple.v1().getAccount(), is("myaccount1"));
        assertThat(tuple.v1().getKey(), is("mykey1"));
        assertThat(tuple.v2().keySet(), hasSize(1));
        assertThat(tuple.v2().get("azure2"), notNullValue());
        assertThat(tuple.v2().get("azure2").getAccount(), is("myaccount2"));
        assertThat(tuple.v2().get("azure2").getKey(), is("mykey2"));
    }

    public void testParseUniqueSettings() {
        Settings settings = Settings.builder()
                .put("cloud.azure.storage.azure1.account", "myaccount1")
                .put("cloud.azure.storage.azure1.key", "mykey1")
                .build();

        Tuple<AzureStorageSettings, Map<String, AzureStorageSettings>> tuple = AzureStorageSettings.parse(settings);
        assertThat(tuple.v1(), notNullValue());
        assertThat(tuple.v1().getAccount(), is("myaccount1"));
        assertThat(tuple.v1().getKey(), is("mykey1"));
        assertThat(tuple.v2().keySet(), hasSize(0));
    }

    public void testParseTwoSettingsNoDefault() {
        Settings settings = Settings.builder()
                .put("cloud.azure.storage.azure1.account", "myaccount1")
                .put("cloud.azure.storage.azure1.key", "mykey1")
                .put("cloud.azure.storage.azure2.account", "myaccount2")
                .put("cloud.azure.storage.azure2.key", "mykey2")
                .build();

        try {
            AzureStorageSettings.parse(settings);
            fail("Should have failed with a SettingsException (no default data store)");
        } catch (SettingsException ex) {
            assertEquals(ex.getMessage(), "No default Azure data store configured");
        }
    }

    public void testParseTwoSettingsTooManyDefaultSet() {
        Settings settings = Settings.builder()
                .put("cloud.azure.storage.azure1.account", "myaccount1")
                .put("cloud.azure.storage.azure1.key", "mykey1")
                .put("cloud.azure.storage.azure1.default", true)
                .put("cloud.azure.storage.azure2.account", "myaccount2")
                .put("cloud.azure.storage.azure2.key", "mykey2")
                .put("cloud.azure.storage.azure2.default", true)
                .build();

        try {
            AzureStorageSettings.parse(settings);
            fail("Should have failed with a SettingsException (multiple default data stores)");
        } catch (SettingsException ex) {
            assertEquals(ex.getMessage(), "Multiple default Azure data stores configured: [azure1] and [azure2]");
        }

    }

    public void testParseEmptySettings() {
        Tuple<AzureStorageSettings, Map<String, AzureStorageSettings>> tuple = AzureStorageSettings.parse(Settings.EMPTY);
        assertThat(tuple.v1(), nullValue());
        assertThat(tuple.v2().keySet(), hasSize(0));
    }
}
