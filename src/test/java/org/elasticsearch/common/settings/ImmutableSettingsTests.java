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

import org.elasticsearch.common.settings.bar.BarTestClass;
import org.elasticsearch.common.settings.foo.FooTestClass;
import org.elasticsearch.common.settings.loader.YamlSettingsLoader;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 */
public class ImmutableSettingsTests extends ElasticsearchTestCase {

    @Test
    public void testCamelCaseSupport() {
        Settings settings = settingsBuilder()
                .put("test.camelCase", "bar")
                .build();
        assertThat(settings.get("test.camelCase"), equalTo("bar"));
        assertThat(settings.get("test.camel_case"), equalTo("bar"));
    }

    @Test
    public void testGetAsClass() {
        Settings settings = settingsBuilder()
                .put("test.class", "bar")
                .put("test.class.package", "org.elasticsearch.common.settings.bar")
                .build();

        // Assert that defaultClazz is loaded if setting is not specified
        assertThat(settings.getAsClass("no.settings", FooTestClass.class, "org.elasticsearch.common.settings.", "TestClass").getName(),
                equalTo(FooTestClass.class.getName()));

        // Assert that correct class is loaded if setting contain name without package
        assertThat(settings.getAsClass("test.class", FooTestClass.class, "org.elasticsearch.common.settings.", "TestClass").getName(),
                equalTo(BarTestClass.class.getName()));

        // Assert that class cannot be loaded if wrong packagePrefix is specified
        try {
            settings.getAsClass("test.class", FooTestClass.class, "com.example.elasticsearch.test.unit..common.settings.", "TestClass");
            fail("Class with wrong package name shouldn't be loaded");
        } catch (NoClassSettingsException ex) {
            // Ignore
        }

        // Assert that package name in settings is getting correctly applied
        assertThat(settings.getAsClass("test.class.package", FooTestClass.class, "com.example.elasticsearch.test.unit.common.settings.", "TestClass").getName(),
                equalTo(BarTestClass.class.getName()));

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

    @Test
    public void testUnFlattenedSettings() {
        Settings settings = settingsBuilder()
                .put("foo", "abc")
                .put("bar", "def")
                .put("baz.foo", "ghi")
                .put("baz.bar", "jkl")
                .putArray("baz.arr", "a", "b", "c")
                .build();
        Map<String, Object> map = settings.getAsStructuredMap();
        assertThat(map.keySet(), Matchers.<String>hasSize(3));
        assertThat(map, allOf(
                Matchers.<String, Object>hasEntry("foo", "abc"),
                Matchers.<String, Object>hasEntry("bar", "def")));

        @SuppressWarnings("unchecked") Map<String, Object> bazMap = (Map<String, Object>) map.get("baz");
        assertThat(bazMap.keySet(), Matchers.<String>hasSize(3));
        assertThat(bazMap, allOf(
                Matchers.<String, Object>hasEntry("foo", "ghi"),
                Matchers.<String, Object>hasEntry("bar", "jkl")));
        @SuppressWarnings("unchecked") List<String> bazArr = (List<String>) bazMap.get("arr");
        assertThat(bazArr, contains("a", "b", "c"));

    }

    @Test
    public void testFallbackToFlattenedSettings() {
        Settings settings = settingsBuilder()
                .put("foo", "abc")
                .put("foo.bar", "def")
                .put("foo.baz", "ghi").build();
        Map<String, Object> map = settings.getAsStructuredMap();
        assertThat(map.keySet(), Matchers.<String>hasSize(3));
        assertThat(map, allOf(
                Matchers.<String, Object>hasEntry("foo", "abc"),
                Matchers.<String, Object>hasEntry("foo.bar", "def"),
                Matchers.<String, Object>hasEntry("foo.baz", "ghi")));

        settings = settingsBuilder()
                .put("foo.bar", "def")
                .put("foo", "abc")
                .put("foo.baz", "ghi")
                .build();
        map = settings.getAsStructuredMap();
        assertThat(map.keySet(), Matchers.<String>hasSize(3));
        assertThat(map, allOf(
                Matchers.<String, Object>hasEntry("foo", "abc"),
                Matchers.<String, Object>hasEntry("foo.bar", "def"),
                Matchers.<String, Object>hasEntry("foo.baz", "ghi")));
    }

    @Test
    public void testGetAsSettings() {
        Settings settings = settingsBuilder()
                .put("foo", "abc")
                .put("foo.bar", "def")
                .put("foo.baz", "ghi").build();

        Settings fooSettings = settings.getAsSettings("foo");
        assertThat(fooSettings.get("bar"), equalTo("def"));
        assertThat(fooSettings.get("baz"), equalTo("ghi"));
    }

    @Test
    public void testNames() {
        Settings settings = settingsBuilder()
                .put("bar", "baz")
                .put("foo", "abc")
                .put("foo.bar", "def")
                .put("foo.baz", "ghi").build();

        Set<String> names = settings.names();
        assertThat(names.size(), equalTo(2));
        assertTrue(names.contains("bar"));
        assertTrue(names.contains("foo"));

        Settings fooSettings = settings.getAsSettings("foo");
        names = fooSettings.names();
        assertThat(names.size(), equalTo(2));
        assertTrue(names.contains("bar"));
        assertTrue(names.contains("baz"));
    }

    @Test
    public void testThatArraysAreOverriddenCorrectly() throws IOException {
        // overriding a single value with an array
        Settings settings = settingsBuilder()
                .put(settingsBuilder().putArray("value", "1").build())
                .put(settingsBuilder().putArray("value", "2", "3").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("2", "3"));

        settings = settingsBuilder()
                .put(settingsBuilder().put("value", "1").build())
                .put(settingsBuilder().putArray("value", "2", "3").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("2", "3"));

        settings = settingsBuilder()
                .put(new YamlSettingsLoader().load("value: 1"))
                .put(new YamlSettingsLoader().load("value: [ 2, 3 ]"))
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("2", "3"));

        settings = settingsBuilder()
                .put(settingsBuilder().put("value.with.deep.key", "1").build())
                .put(settingsBuilder().putArray("value.with.deep.key", "2", "3").build())
                .build();
        assertThat(settings.getAsArray("value.with.deep.key"), arrayContaining("2", "3"));

        // overriding an array with a shorter array
        settings = settingsBuilder()
                .put(settingsBuilder().putArray("value", "1", "2").build())
                .put(settingsBuilder().putArray("value", "3").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("3"));

        settings = settingsBuilder()
                .put(settingsBuilder().putArray("value", "1", "2", "3").build())
                .put(settingsBuilder().putArray("value", "4", "5").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("4", "5"));

        settings = settingsBuilder()
                .put(settingsBuilder().putArray("value.deep.key", "1", "2", "3").build())
                .put(settingsBuilder().putArray("value.deep.key", "4", "5").build())
                .build();
        assertThat(settings.getAsArray("value.deep.key"), arrayContaining("4", "5"));

        // overriding an array with a longer array
        settings = settingsBuilder()
                .put(settingsBuilder().putArray("value", "1", "2").build())
                .put(settingsBuilder().putArray("value", "3", "4", "5").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("3", "4", "5"));

        settings = settingsBuilder()
                .put(settingsBuilder().putArray("value.deep.key", "1", "2", "3").build())
                .put(settingsBuilder().putArray("value.deep.key", "4", "5").build())
                .build();
        assertThat(settings.getAsArray("value.deep.key"), arrayContaining("4", "5"));

        // overriding an array with a single value
        settings = settingsBuilder()
                .put(settingsBuilder().putArray("value", "1", "2").build())
                .put(settingsBuilder().put("value", "3").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("3"));

        settings = settingsBuilder()
                .put(settingsBuilder().putArray("value.deep.key", "1", "2").build())
                .put(settingsBuilder().put("value.deep.key", "3").build())
                .build();
        assertThat(settings.getAsArray("value.deep.key"), arrayContaining("3"));

        // test that other arrays are not overridden
        settings = settingsBuilder()
                .put(settingsBuilder().putArray("value", "1", "2", "3").putArray("a", "b", "c").build())
                .put(settingsBuilder().putArray("value", "4", "5").putArray("d", "e", "f").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("4", "5"));
        assertThat(settings.getAsArray("a"), arrayContaining("b", "c"));
        assertThat(settings.getAsArray("d"), arrayContaining("e", "f"));

        settings = settingsBuilder()
                .put(settingsBuilder().putArray("value.deep.key", "1", "2", "3").putArray("a", "b", "c").build())
                .put(settingsBuilder().putArray("value.deep.key", "4", "5").putArray("d", "e", "f").build())
                .build();
        assertThat(settings.getAsArray("value.deep.key"), arrayContaining("4", "5"));
        assertThat(settings.getAsArray("a"), notNullValue());
        assertThat(settings.getAsArray("d"), notNullValue());

        // overriding a deeper structure with an array
        settings = settingsBuilder()
                .put(settingsBuilder().put("value.data", "1").build())
                .put(settingsBuilder().putArray("value", "4", "5").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("4", "5"));

        // overriding an array with a deeper structure
        settings = settingsBuilder()
                .put(settingsBuilder().putArray("value", "4", "5").build())
                .put(settingsBuilder().put("value.data", "1").build())
                .build();
        assertThat(settings.get("value.data"), is("1"));
        assertThat(settings.get("value"), is(nullValue()));
    }

    @Test
    public void testPrefixNormalization() {

        Settings settings = settingsBuilder().normalizePrefix("foo.").build();

        assertThat(settings.names().size(), equalTo(0));

        settings = settingsBuilder()
                .put("bar", "baz")
                .normalizePrefix("foo.")
                .build();

        assertThat(settings.getAsMap().size(), equalTo(1));
        assertThat(settings.get("bar"), nullValue());
        assertThat(settings.get("foo.bar"), equalTo("baz"));


        settings = settingsBuilder()
                .put("bar", "baz")
                .put("foo.test", "test")
                .normalizePrefix("foo.")
                .build();

        assertThat(settings.getAsMap().size(), equalTo(2));
        assertThat(settings.get("bar"), nullValue());
        assertThat(settings.get("foo.bar"), equalTo("baz"));
        assertThat(settings.get("foo.test"), equalTo("test"));

        settings = settingsBuilder()
                .put("foo.test", "test")
                .normalizePrefix("foo.")
                .build();


        assertThat(settings.getAsMap().size(), equalTo(1));
        assertThat(settings.get("foo.test"), equalTo("test"));
    }


}
