/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.common.settings;

import org.elasticsearch.common.settings.NoClassSettingsException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.unit.common.settings.bar.BarTest;
import org.elasticsearch.test.unit.common.settings.foo.FooTest;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public class ImmutableSettingsTests {

    @Test
    public void testGetAsClass() {
        Settings settings = settingsBuilder()
                .put("test.class", "bar")
                .put("test.class.package", "org.elasticsearch.test.unit.common.settings.bar")
                .build();

        // Assert that defaultClazz is loaded if setting is not specified
        assertThat(settings.getAsClass("no.settings", FooTest.class, "org.elasticsearch.test.unit.common.settings.", "Test").getName(),
                equalTo(FooTest.class.getName()));

        // Assert that correct class is loaded if setting contain name without package
        assertThat(settings.getAsClass("test.class", FooTest.class, "org.elasticsearch.test.unit.common.settings.", "Test").getName(),
                equalTo(BarTest.class.getName()));

        // Assert that class cannot be loaded if wrong packagePrefix is specified
        try {
            settings.getAsClass("test.class", FooTest.class, "com.example.elasticsearch.test.unit..common.settings.", "Test");
            assertThat("Class with wrong package name shouldn't be loaded", false);
        } catch (NoClassSettingsException ex) {
            // Ignore
        }

        // Assert that package name in settings is getting correctly applied
        assertThat(settings.getAsClass("test.class.package", FooTest.class, "com.example.elasticsearch.test.unit.common.settings.", "Test").getName(),
                equalTo(BarTest.class.getName()));

    }

    @Test
    public void testLoadFromDelimitedString() {
        Settings settings = settingsBuilder()
                .loadFromDelimitedString("key1=value1;key2=value2", ';')
                .build();
        assertThat(settings.get("key1"), equalTo("value1"));
        assertThat(settings.get("key2"), equalTo("value2"));
        assertThat(settings.getAsMap().size(), equalTo(2));
        assertThat(settings.toDelimitedString(';'), equalTo("key1=value1;key2=value2;"));

        settings = settingsBuilder()
                .loadFromDelimitedString("key1=value1;key2=value2;", ';')
                .build();
        assertThat(settings.get("key1"), equalTo("value1"));
        assertThat(settings.get("key2"), equalTo("value2"));
        assertThat(settings.getAsMap().size(), equalTo(2));
        assertThat(settings.toDelimitedString(';'), equalTo("key1=value1;key2=value2;"));
    }

    @Test(expected = NoClassSettingsException.class)
    public void testThatAllClassNotFoundExceptionsAreCaught() {
        // this should be nGram in order to really work, but for sure not not throw a NoClassDefFoundError
        Settings settings = settingsBuilder().put("type", "ngram").build();
        settings.getAsClass("type", null, "org.elasticsearch.index.analysis.", "TokenFilterFactory");
    }

    @Test
    public void testReplacePropertiesPlaceholderSystemProperty() {
        System.setProperty("sysProp1", "sysVal1");
        try {
            Settings settings = settingsBuilder()
                    .put("setting1", "${sysProp1}")
                    .replacePropertyPlaceholders()
                    .build();
            assertThat(settings.get("setting1"), equalTo("sysVal1"));
        } finally {
            System.clearProperty("sysProp1");
        }

        Settings settings = settingsBuilder()
                .put("setting1", "${sysProp1:defaultVal1}")
                .replacePropertyPlaceholders()
                .build();
        assertThat(settings.get("setting1"), equalTo("defaultVal1"));

        settings = settingsBuilder()
                .put("setting1", "${sysProp1:}")
                .replacePropertyPlaceholders()
                .build();
        assertThat(settings.get("setting1"), is(nullValue()));
    }

    @Test
    public void testReplacePropertiesPlaceholderIgnoreEnvUnset() {
        Settings settings = settingsBuilder()
                .put("setting1", "${env.UNSET_ENV_VAR}")
                .replacePropertyPlaceholders()
                .build();
        assertThat(settings.get("setting1"), is(nullValue()));
    }
}
