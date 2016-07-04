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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class YamlSettingsLoaderTests extends ESTestCase {

    public void testSimpleYamlSettings() throws Exception {
        final String yaml = "/org/elasticsearch/common/settings/loader/test-settings.yml";
        final Settings settings = Settings.builder()
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

    public void testIndentation() throws Exception {
        String yaml = "/org/elasticsearch/common/settings/loader/indentation-settings.yml";
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> {
            Settings.builder().loadFromStream(yaml, getClass().getResourceAsStream(yaml));
        });
        assertTrue(e.getMessage(), e.getMessage().contains("malformed"));
    }

    public void testIndentationWithExplicitDocumentStart() throws Exception {
        String yaml = "/org/elasticsearch/common/settings/loader/indentation-with-explicit-document-start-settings.yml";
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> {
            Settings.builder().loadFromStream(yaml, getClass().getResourceAsStream(yaml));
        });
        assertTrue(e.getMessage(), e.getMessage().contains("malformed"));
    }

    public void testDuplicateKeysThrowsException() {
        String yaml = "foo: bar\nfoo: baz";
        SettingsException e = expectThrows(SettingsException.class, () -> {
            Settings.builder().loadFromSource(yaml);
        });
        assertEquals(e.getCause().getClass(), ElasticsearchParseException.class);
        String msg = e.getCause().getMessage();
        assertTrue(
            msg,
            msg.contains("duplicate settings key [foo] found at line number [2], column number [6], " +
                "previous value [bar], current value [baz]"));
    }

    public void testMissingValue() throws Exception {
        Path tmp = createTempFile("test", ".yaml");
        Files.write(tmp, Collections.singletonList("foo: # missing value\n"), StandardCharsets.UTF_8);
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> {
            Settings.builder().loadFromPath(tmp);
        });
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("null-valued setting found for key [foo] found at line number [1], column number [5]"));
    }
}
