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
import org.junit.Test;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class YamlSettingsLoaderTests extends ESTestCase {
    @Test
    public void testSimpleYamlSettings() throws Exception {
        String yaml = "/org/elasticsearch/common/settings/loader/test-settings.yml";
        Settings settings = settingsBuilder()
                .loadFromStream(yaml, getClass().getResourceAsStream(yaml))
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

    @Test(expected = SettingsException.class)
    public void testIndentation() {
        String yaml = "/org/elasticsearch/common/settings/loader/indentation-settings.yml";
        settingsBuilder()
            .loadFromStream(yaml, getClass().getResourceAsStream(yaml))
            .build();
    }

    @Test(expected = SettingsException.class)
    public void testIndentationWithExplicitDocumentStart() {
        String yaml = "/org/elasticsearch/common/settings/loader/indentation-with-explicit-document-start-settings.yml";
        settingsBuilder()
                .loadFromStream(yaml, getClass().getResourceAsStream(yaml))
                .build();
    }

    public void testDuplicateKeysThrowsException() {
        String yaml = "foo: bar\nfoo: baz";
        try {
            settingsBuilder()
                    .loadFromSource(yaml)
                    .build();
            fail("expected exception");
        } catch (SettingsException e) {
            assertEquals(e.getCause().getClass(), ElasticsearchParseException.class);
            assertTrue(e.toString().contains("duplicate settings key [foo] found at line number [2], column number [6], previous value [bar], current value [baz]"));
        }
    }
}
