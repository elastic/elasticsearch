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

package org.elasticsearch.common.settings.loader;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class JsonSettingsLoaderTests extends ESTestCase {

    public void testSimpleJsonSettings() throws Exception {
        final String json = "/org/elasticsearch/common/settings/loader/test-settings.json";
        final Settings settings = Settings.builder()
                .loadFromStream(json, getClass().getResourceAsStream(json))
                .build();

        assertThat(settings.get("test1.value1"), equalTo("value1"));
        assertThat(settings.get("test1.test2.value2"), equalTo("value2"));
        assertThat(settings.getAsInt("test1.test2.value3", -1), equalTo(2));

        // check array
        assertThat(settings.get("test1.test3.0"), equalTo("test3-1"));
        assertThat(settings.get("test1.test3.1"), equalTo("test3-2"));
        assertThat(settings.getAsArray("test1.test3").length, equalTo(2));
        assertThat(settings.getAsArray("test1.test3")[0], equalTo("test3-1"));
        assertThat(settings.getAsArray("test1.test3")[1], equalTo("test3-2"));
    }

    public void testDuplicateKeysThrowsException() {
        final String json = "{\"foo\":\"bar\",\"foo\":\"baz\"}";
        final SettingsException e = expectThrows(SettingsException.class, () -> Settings.builder().loadFromSource(json).build());
        assertEquals(e.getCause().getClass(), ElasticsearchParseException.class);
        assertThat(
                e.toString(),
                containsString("duplicate settings key [foo] " +
                        "found at line number [1], " +
                        "column number [20], " +
                        "previous value [bar], " +
                        "current value [baz]"));
    }

    public void testNullValuedSettingThrowsException() {
        final String json = "{\"foo\":null}";
        final ElasticsearchParseException e =
                expectThrows(ElasticsearchParseException.class, () -> new JsonSettingsLoader(false).load(json));
        assertThat(e.toString(), containsString("null-valued setting found for key [foo] found at line number [1], column number [8]"));
    }

}
