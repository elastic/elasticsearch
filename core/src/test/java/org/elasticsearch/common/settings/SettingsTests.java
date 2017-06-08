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

import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.loader.YamlSettingsLoader;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SettingsTests extends ESTestCase {

    public void testReplacePropertiesPlaceholderSystemProperty() {
        String value = System.getProperty("java.home");
        assertFalse(value.isEmpty());
        Settings settings = Settings.builder()
                 .put("property.placeholder", value)
                 .put("setting1", "${property.placeholder}")
                 .replacePropertyPlaceholders()
                 .build();
        assertThat(settings.get("setting1"), equalTo(value));
    }

    public void testReplacePropertiesPlaceholderSystemVariablesHaveNoEffect() {
        final String value = System.getProperty("java.home");
        assertNotNull(value);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Settings.builder()
                .put("setting1", "${java.home}")
                .replacePropertyPlaceholders()
                .build());
        assertThat(e, hasToString(containsString("Could not resolve placeholder 'java.home'")));
    }

    public void testReplacePropertiesPlaceholderByEnvironmentVariables() {
        final String hostname = randomAlphaOfLength(16);
        final Settings implicitEnvSettings = Settings.builder()
            .put("setting1", "${HOSTNAME}")
            .replacePropertyPlaceholders(name -> "HOSTNAME".equals(name) ? hostname : null)
            .build();
        assertThat(implicitEnvSettings.get("setting1"), equalTo(hostname));
    }

    public void testReplacePropertiesPlaceholderIgnoresPrompt() {
        Settings settings = Settings.builder()
                .put("setting1", "${prompt.text}")
                .put("setting2", "${prompt.secret}")
                .replacePropertyPlaceholders()
                .build();
        assertThat(settings.get("setting1"), is("${prompt.text}"));
        assertThat(settings.get("setting2"), is("${prompt.secret}"));
    }

    public void testUnFlattenedSettings() {
        Settings settings = Settings.builder()
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

    public void testFallbackToFlattenedSettings() {
        Settings settings = Settings.builder()
                .put("foo", "abc")
                .put("foo.bar", "def")
                .put("foo.baz", "ghi").build();
        Map<String, Object> map = settings.getAsStructuredMap();
        assertThat(map.keySet(), Matchers.<String>hasSize(3));
        assertThat(map, allOf(
                Matchers.<String, Object>hasEntry("foo", "abc"),
                Matchers.<String, Object>hasEntry("foo.bar", "def"),
                Matchers.<String, Object>hasEntry("foo.baz", "ghi")));

        settings = Settings.builder()
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

    public void testGetAsSettings() {
        Settings settings = Settings.builder()
                .put("bar", "hello world")
                .put("foo", "abc")
                .put("foo.bar", "def")
                .put("foo.baz", "ghi").build();

        Settings fooSettings = settings.getAsSettings("foo");
        assertFalse(fooSettings.isEmpty());
        assertEquals(2, fooSettings.size());
        assertThat(fooSettings.get("bar"), equalTo("def"));
        assertThat(fooSettings.get("baz"), equalTo("ghi"));
    }

    @SuppressWarnings("deprecation") //#getAsBooleanLenientForPreEs6Indices is the test subject
    public void testLenientBooleanForPreEs6Index() throws IOException {
        // time to say goodbye?
        assertTrue(
            "It's time to implement #22298. Please delete this test and Settings#getAsBooleanLenientForPreEs6Indices().",
            Version.CURRENT.minimumCompatibilityVersion().before(Version.V_6_0_0_alpha1));


        String falsy = randomFrom("false", "off", "no", "0");
        String truthy = randomFrom("true", "on", "yes", "1");

        Settings settings = Settings.builder()
            .put("foo", falsy)
            .put("bar", truthy).build();

        final DeprecationLogger deprecationLogger = new DeprecationLogger(ESLoggerFactory.getLogger("testLenientBooleanForPreEs6Index"));

        assertFalse(settings.getAsBooleanLenientForPreEs6Indices(Version.V_5_0_0, "foo", null, deprecationLogger));
        assertTrue(settings.getAsBooleanLenientForPreEs6Indices(Version.V_5_0_0, "bar", null, deprecationLogger));
        assertTrue(settings.getAsBooleanLenientForPreEs6Indices(Version.V_5_0_0, "baz", true, deprecationLogger));

        List<String> expectedDeprecationWarnings = new ArrayList<>();
        if (Booleans.isBoolean(falsy) == false) {
            expectedDeprecationWarnings.add(
                "The value [" + falsy + "] of setting [foo] is not coerced into boolean anymore. Please change this value to [false].");
        }
        if (Booleans.isBoolean(truthy) == false) {
            expectedDeprecationWarnings.add(
                "The value [" + truthy + "] of setting [bar] is not coerced into boolean anymore. Please change this value to [true].");
        }

        if (expectedDeprecationWarnings.isEmpty() == false) {
            assertWarnings(expectedDeprecationWarnings.toArray(new String[1]));
        }
    }

    @SuppressWarnings("deprecation") //#getAsBooleanLenientForPreEs6Indices is the test subject
    public void testInvalidLenientBooleanForCurrentIndexVersion() {
        String falsy = randomFrom("off", "no", "0");
        String truthy = randomFrom("on", "yes", "1");

        Settings settings = Settings.builder()
            .put("foo", falsy)
            .put("bar", truthy).build();

        final DeprecationLogger deprecationLogger =
            new DeprecationLogger(ESLoggerFactory.getLogger("testInvalidLenientBooleanForCurrentIndexVersion"));
        expectThrows(IllegalArgumentException.class,
            () -> settings.getAsBooleanLenientForPreEs6Indices(Version.CURRENT, "foo", null, deprecationLogger));
        expectThrows(IllegalArgumentException.class,
            () -> settings.getAsBooleanLenientForPreEs6Indices(Version.CURRENT, "bar", null, deprecationLogger));
    }

    @SuppressWarnings("deprecation") //#getAsBooleanLenientForPreEs6Indices is the test subject
    public void testValidLenientBooleanForCurrentIndexVersion() {
        Settings settings = Settings.builder()
            .put("foo", "false")
            .put("bar", "true").build();

        final DeprecationLogger deprecationLogger =
            new DeprecationLogger(ESLoggerFactory.getLogger("testValidLenientBooleanForCurrentIndexVersion"));
        assertFalse(settings.getAsBooleanLenientForPreEs6Indices(Version.CURRENT, "foo", null, deprecationLogger));
        assertTrue(settings.getAsBooleanLenientForPreEs6Indices(Version.CURRENT, "bar", null, deprecationLogger));
        assertTrue(settings.getAsBooleanLenientForPreEs6Indices(Version.CURRENT, "baz", true, deprecationLogger));
    }

    public void testMultLevelGetPrefix() {
        Settings settings = Settings.builder()
            .put("1.2.3", "hello world")
            .put("1.2.3.4", "abc")
            .put("2.3.4", "def")
            .put("3.4", "ghi").build();

        Settings firstLevelSettings = settings.getByPrefix("1.");
        assertFalse(firstLevelSettings.isEmpty());
        assertEquals(2, firstLevelSettings.size());
        assertThat(firstLevelSettings.get("2.3.4"), equalTo("abc"));
        assertThat(firstLevelSettings.get("2.3"), equalTo("hello world"));

        Settings secondLevelSetting = firstLevelSettings.getByPrefix("2.");
        assertFalse(secondLevelSetting.isEmpty());
        assertEquals(2, secondLevelSetting.size());
        assertNull(secondLevelSetting.get("2.3.4"));
        assertNull(secondLevelSetting.get("1.2.3.4"));
        assertNull(secondLevelSetting.get("1.2.3"));
        assertThat(secondLevelSetting.get("3.4"), equalTo("abc"));
        assertThat(secondLevelSetting.get("3"), equalTo("hello world"));

        Settings thirdLevelSetting = secondLevelSetting.getByPrefix("3.");
        assertFalse(thirdLevelSetting.isEmpty());
        assertEquals(1, thirdLevelSetting.size());
        assertNull(thirdLevelSetting.get("2.3.4"));
        assertNull(thirdLevelSetting.get("3.4"));
        assertNull(thirdLevelSetting.get("1.2.3"));
        assertThat(thirdLevelSetting.get("4"), equalTo("abc"));
    }

    public void testNames() {
        Settings settings = Settings.builder()
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

    public void testThatArraysAreOverriddenCorrectly() throws IOException {
        // overriding a single value with an array
        Settings settings = Settings.builder()
                .put(Settings.builder().putArray("value", "1").build())
                .put(Settings.builder().putArray("value", "2", "3").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("2", "3"));

        settings = Settings.builder()
                .put(Settings.builder().put("value", "1").build())
                .put(Settings.builder().putArray("value", "2", "3").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("2", "3"));

        settings = Settings.builder()
                .put(new YamlSettingsLoader(false).load("value: 1"))
                .put(new YamlSettingsLoader(false).load("value: [ 2, 3 ]"))
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("2", "3"));

        settings = Settings.builder()
                .put(Settings.builder().put("value.with.deep.key", "1").build())
                .put(Settings.builder().putArray("value.with.deep.key", "2", "3").build())
                .build();
        assertThat(settings.getAsArray("value.with.deep.key"), arrayContaining("2", "3"));

        // overriding an array with a shorter array
        settings = Settings.builder()
                .put(Settings.builder().putArray("value", "1", "2").build())
                .put(Settings.builder().putArray("value", "3").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("3"));

        settings = Settings.builder()
                .put(Settings.builder().putArray("value", "1", "2", "3").build())
                .put(Settings.builder().putArray("value", "4", "5").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("4", "5"));

        settings = Settings.builder()
                .put(Settings.builder().putArray("value.deep.key", "1", "2", "3").build())
                .put(Settings.builder().putArray("value.deep.key", "4", "5").build())
                .build();
        assertThat(settings.getAsArray("value.deep.key"), arrayContaining("4", "5"));

        // overriding an array with a longer array
        settings = Settings.builder()
                .put(Settings.builder().putArray("value", "1", "2").build())
                .put(Settings.builder().putArray("value", "3", "4", "5").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("3", "4", "5"));

        settings = Settings.builder()
                .put(Settings.builder().putArray("value.deep.key", "1", "2", "3").build())
                .put(Settings.builder().putArray("value.deep.key", "4", "5").build())
                .build();
        assertThat(settings.getAsArray("value.deep.key"), arrayContaining("4", "5"));

        // overriding an array with a single value
        settings = Settings.builder()
                .put(Settings.builder().putArray("value", "1", "2").build())
                .put(Settings.builder().put("value", "3").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("3"));

        settings = Settings.builder()
                .put(Settings.builder().putArray("value.deep.key", "1", "2").build())
                .put(Settings.builder().put("value.deep.key", "3").build())
                .build();
        assertThat(settings.getAsArray("value.deep.key"), arrayContaining("3"));

        // test that other arrays are not overridden
        settings = Settings.builder()
                .put(Settings.builder().putArray("value", "1", "2", "3").putArray("a", "b", "c").build())
                .put(Settings.builder().putArray("value", "4", "5").putArray("d", "e", "f").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("4", "5"));
        assertThat(settings.getAsArray("a"), arrayContaining("b", "c"));
        assertThat(settings.getAsArray("d"), arrayContaining("e", "f"));

        settings = Settings.builder()
                .put(Settings.builder().putArray("value.deep.key", "1", "2", "3").putArray("a", "b", "c").build())
                .put(Settings.builder().putArray("value.deep.key", "4", "5").putArray("d", "e", "f").build())
                .build();
        assertThat(settings.getAsArray("value.deep.key"), arrayContaining("4", "5"));
        assertThat(settings.getAsArray("a"), notNullValue());
        assertThat(settings.getAsArray("d"), notNullValue());

        // overriding a deeper structure with an array
        settings = Settings.builder()
                .put(Settings.builder().put("value.data", "1").build())
                .put(Settings.builder().putArray("value", "4", "5").build())
                .build();
        assertThat(settings.getAsArray("value"), arrayContaining("4", "5"));

        // overriding an array with a deeper structure
        settings = Settings.builder()
                .put(Settings.builder().putArray("value", "4", "5").build())
                .put(Settings.builder().put("value.data", "1").build())
                .build();
        assertThat(settings.get("value.data"), is("1"));
        assertThat(settings.get("value"), is(nullValue()));
    }

    public void testPrefixNormalization() {
        Settings settings = Settings.builder().normalizePrefix("foo.").build();

        assertThat(settings.names().size(), equalTo(0));

        settings = Settings.builder()
                .put("bar", "baz")
                .normalizePrefix("foo.")
                .build();

        assertThat(settings.size(), equalTo(1));
        assertThat(settings.get("bar"), nullValue());
        assertThat(settings.get("foo.bar"), equalTo("baz"));


        settings = Settings.builder()
                .put("bar", "baz")
                .put("foo.test", "test")
                .normalizePrefix("foo.")
                .build();

        assertThat(settings.size(), equalTo(2));
        assertThat(settings.get("bar"), nullValue());
        assertThat(settings.get("foo.bar"), equalTo("baz"));
        assertThat(settings.get("foo.test"), equalTo("test"));

        settings = Settings.builder()
                .put("foo.test", "test")
                .normalizePrefix("foo.")
                .build();


        assertThat(settings.size(), equalTo(1));
        assertThat(settings.get("foo.test"), equalTo("test"));
    }

    public void testFilteredMap() {
        Settings.Builder builder = Settings.builder();
        builder.put("a", "a1");
        builder.put("a.b", "ab1");
        builder.put("a.b.c", "ab2");
        builder.put("a.c", "ac1");
        builder.put("a.b.c.d", "ab3");


        Map<String, String> fiteredMap = builder.build().filter((k) -> k.startsWith("a.b")).getAsMap();
        assertEquals(3, fiteredMap.size());
        int numKeys = 0;
        for (String k : fiteredMap.keySet()) {
            numKeys++;
            assertTrue(k.startsWith("a.b"));
        }

        assertEquals(3, numKeys);
        int numValues = 0;

        for (String v : fiteredMap.values()) {
            numValues++;
            assertTrue(v.startsWith("ab"));
        }
        assertEquals(3, numValues);
        assertFalse(fiteredMap.containsKey("a.c"));
        assertFalse(fiteredMap.containsKey("a"));
        assertTrue(fiteredMap.containsKey("a.b"));
        assertTrue(fiteredMap.containsKey("a.b.c"));
        assertTrue(fiteredMap.containsKey("a.b.c.d"));
        expectThrows(UnsupportedOperationException.class, () ->
            fiteredMap.remove("a.b"));
        assertEquals("ab1", fiteredMap.get("a.b"));
        assertEquals("ab2", fiteredMap.get("a.b.c"));
        assertEquals("ab3", fiteredMap.get("a.b.c.d"));

        Iterator<String> iterator = fiteredMap.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertTrue(iterator.hasNext());
        }
        assertEquals("a.b", iterator.next());
        if (randomBoolean()) {
            assertTrue(iterator.hasNext());
        }
        assertEquals("a.b.c", iterator.next());
        if (randomBoolean()) {
            assertTrue(iterator.hasNext());
        }
        assertEquals("a.b.c.d", iterator.next());
        assertFalse(iterator.hasNext());
        expectThrows(NoSuchElementException.class, () -> iterator.next());

    }

    public void testPrefixMap() {
        Settings.Builder builder = Settings.builder();
        builder.put("a", "a1");
        builder.put("a.b", "ab1");
        builder.put("a.b.c", "ab2");
        builder.put("a.c", "ac1");
        builder.put("a.b.c.d", "ab3");

        Map<String, String> prefixMap = builder.build().getByPrefix("a.").getAsMap();
        assertEquals(4, prefixMap.size());
        int numKeys = 0;
        for (String k : prefixMap.keySet()) {
            numKeys++;
            assertTrue(k, k.startsWith("b") || k.startsWith("c"));
        }

        assertEquals(4, numKeys);
        int numValues = 0;

        for (String v : prefixMap.values()) {
            numValues++;
            assertTrue(v, v.startsWith("ab") || v.startsWith("ac"));
        }
        assertEquals(4, numValues);
        assertFalse(prefixMap.containsKey("a"));
        assertTrue(prefixMap.containsKey("c"));
        assertTrue(prefixMap.containsKey("b"));
        assertTrue(prefixMap.containsKey("b.c"));
        assertTrue(prefixMap.containsKey("b.c.d"));
        expectThrows(UnsupportedOperationException.class, () ->
            prefixMap.remove("a.b"));
        assertEquals("ab1", prefixMap.get("b"));
        assertEquals("ab2", prefixMap.get("b.c"));
        assertEquals("ab3", prefixMap.get("b.c.d"));
        Iterator<String> prefixIterator = prefixMap.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertTrue(prefixIterator.hasNext());
        }
        assertEquals("b", prefixIterator.next());
        if (randomBoolean()) {
            assertTrue(prefixIterator.hasNext());
        }
        assertEquals("b.c", prefixIterator.next());
        if (randomBoolean()) {
            assertTrue(prefixIterator.hasNext());
        }
        assertEquals("b.c.d", prefixIterator.next());
        if (randomBoolean()) {
            assertTrue(prefixIterator.hasNext());
        }
        assertEquals("c", prefixIterator.next());
        assertFalse(prefixIterator.hasNext());
        expectThrows(NoSuchElementException.class, () -> prefixIterator.next());
    }

    public void testSecureSettingsPrefix() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("test.prefix.foo", "somethingsecure");
        Settings.Builder builder = Settings.builder();
        builder.setSecureSettings(secureSettings);
        Settings settings = builder.build();
        Settings prefixSettings = settings.getByPrefix("test.prefix.");
        assertTrue(prefixSettings.names().contains("foo"));
    }

    public void testGroupPrefix() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("test.key1.foo", "somethingsecure");
        secureSettings.setString("test.key1.bar", "somethingsecure");
        secureSettings.setString("test.key2.foo", "somethingsecure");
        secureSettings.setString("test.key2.bog", "somethingsecure");
        Settings.Builder builder = Settings.builder();
        builder.put("test.key1.baz", "blah1");
        builder.put("test.key1.other", "blah2");
        builder.put("test.key2.baz", "blah3");
        builder.put("test.key2.else", "blah4");
        builder.setSecureSettings(secureSettings);
        Settings settings = builder.build();
        Map<String, Settings> groups = settings.getGroups("test");
        assertEquals(2, groups.size());
        Settings key1 = groups.get("key1");
        assertNotNull(key1);
        assertThat(key1.names(), containsInAnyOrder("foo", "bar", "baz", "other"));
        Settings key2 = groups.get("key2");
        assertNotNull(key2);
        assertThat(key2.names(), containsInAnyOrder("foo", "bog", "baz", "else"));
    }

    public void testEmptyFilterMap() {
        Settings.Builder builder = Settings.builder();
        builder.put("a", "a1");
        builder.put("a.b", "ab1");
        builder.put("a.b.c", "ab2");
        builder.put("a.c", "ac1");
        builder.put("a.b.c.d", "ab3");

        Map<String, String> fiteredMap = builder.build().filter((k) -> false).getAsMap();
        assertEquals(0, fiteredMap.size());
        for (String k : fiteredMap.keySet()) {
            fail("no element");

        }
        for (String v : fiteredMap.values()) {
            fail("no element");
        }
        assertFalse(fiteredMap.containsKey("a.c"));
        assertFalse(fiteredMap.containsKey("a"));
        assertFalse(fiteredMap.containsKey("a.b"));
        assertFalse(fiteredMap.containsKey("a.b.c"));
        assertFalse(fiteredMap.containsKey("a.b.c.d"));
        expectThrows(UnsupportedOperationException.class, () ->
            fiteredMap.remove("a.b"));
        assertNull(fiteredMap.get("a.b"));
        assertNull(fiteredMap.get("a.b.c"));
        assertNull(fiteredMap.get("a.b.c.d"));

        Iterator<String> iterator = fiteredMap.keySet().iterator();
        for (int i = 0; i < 10; i++) {
            assertFalse(iterator.hasNext());
        }
        expectThrows(NoSuchElementException.class, () -> iterator.next());
    }

    public void testEmpty() {
        assertTrue(Settings.EMPTY.isEmpty());
        MockSecureSettings secureSettings = new MockSecureSettings();
        assertTrue(Settings.builder().setSecureSettings(secureSettings).build().isEmpty());
    }

    public void testSecureSettingConflict() {
        Setting<SecureString> setting = SecureSetting.secureString("something.secure", null);
        Settings settings = Settings.builder().put("something.secure", "notreallysecure").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertTrue(e.getMessage().contains("must be stored inside the Elasticsearch keystore"));
    }

    public void testGetAsArrayFailsOnDuplicates() {
        final Settings settings =
                Settings.builder()
                        .put("foobar.0", "bar")
                        .put("foobar.1", "baz")
                        .put("foobar", "foo")
                        .build();
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> settings.getAsArray("foobar"));
        assertThat(e, hasToString(containsString("settings object contains values for [foobar=foo] and [foobar.0=bar]")));
    }

}
