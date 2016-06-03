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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SettingTests extends ESTestCase {
    public void testGet() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope);
        assertFalse(booleanSetting.get(Settings.EMPTY));
        assertFalse(booleanSetting.get(Settings.builder().put("foo.bar", false).build()));
        assertTrue(booleanSetting.get(Settings.builder().put("foo.bar", true).build()));
    }

    public void testByteSize() {
        Setting<ByteSizeValue> byteSizeValueSetting =
            Setting.byteSizeSetting("a.byte.size", new ByteSizeValue(1024), Property.Dynamic, Property.NodeScope);
        assertFalse(byteSizeValueSetting.isGroupSetting());
        ByteSizeValue byteSizeValue = byteSizeValueSetting.get(Settings.EMPTY);
        assertEquals(byteSizeValue.bytes(), 1024);

        byteSizeValueSetting = Setting.byteSizeSetting("a.byte.size", s -> "2048b", Property.Dynamic, Property.NodeScope);
        byteSizeValue = byteSizeValueSetting.get(Settings.EMPTY);
        assertEquals(byteSizeValue.bytes(), 2048);


        AtomicReference<ByteSizeValue> value = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<ByteSizeValue> settingUpdater = byteSizeValueSetting.newUpdater(value::set, logger);
        try {
            settingUpdater.apply(Settings.builder().put("a.byte.size", 12).build(), Settings.EMPTY);
            fail("no unit");
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse setting [a.byte.size] with value [12] as a size in bytes: unit is missing or unrecognized",
                    ex.getMessage());
        }

        assertTrue(settingUpdater.apply(Settings.builder().put("a.byte.size", "12b").build(), Settings.EMPTY));
        assertEquals(new ByteSizeValue(12), value.get());
    }

    public void testSimpleUpdate() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope);
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<Boolean> settingUpdater = booleanSetting.newUpdater(atomicBoolean::set, logger);
        Settings build = Settings.builder().put("foo.bar", false).build();
        settingUpdater.apply(build, Settings.EMPTY);
        assertNull(atomicBoolean.get());
        build = Settings.builder().put("foo.bar", true).build();
        settingUpdater.apply(build, Settings.EMPTY);
        assertTrue(atomicBoolean.get());

        // try update bogus value
        build = Settings.builder().put("foo.bar", "I am not a boolean").build();
        try {
            settingUpdater.apply(build, Settings.EMPTY);
            fail("not a boolean");
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [I am not a boolean] cannot be parsed to boolean [ true/1/on/yes OR false/0/off/no ]",
                    ex.getMessage());
        }
    }

    public void testUpdateNotDynamic() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.NodeScope);
        assertFalse(booleanSetting.isGroupSetting());
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        try {
            booleanSetting.newUpdater(atomicBoolean::set, logger);
            fail("not dynamic");
        } catch (IllegalStateException ex) {
            assertEquals("setting [foo.bar] is not dynamic", ex.getMessage());
        }
    }

    public void testUpdaterIsIsolated() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope);
        AtomicReference<Boolean> ab1 = new AtomicReference<>(null);
        AtomicReference<Boolean> ab2 = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<Boolean> settingUpdater = booleanSetting.newUpdater(ab1::set, logger);
        settingUpdater.apply(Settings.builder().put("foo.bar", true).build(), Settings.EMPTY);
        assertTrue(ab1.get());
        assertNull(ab2.get());
    }

    public void testDefault() {
        TimeValue defaultValue = TimeValue.timeValueMillis(randomIntBetween(0, 1000000));
        Setting<TimeValue> setting =
            Setting.positiveTimeSetting("my.time.value", defaultValue, Property.NodeScope);
        assertFalse(setting.isGroupSetting());
        String aDefault = setting.getDefaultRaw(Settings.EMPTY);
        assertEquals(defaultValue.millis() + "ms", aDefault);
        assertEquals(defaultValue.millis(), setting.get(Settings.EMPTY).millis());
        assertEquals(defaultValue, setting.getDefault(Settings.EMPTY));

        Setting<String> secondaryDefault =
            new Setting<>("foo.bar", (s) -> s.get("old.foo.bar", "some_default"), Function.identity(), Property.NodeScope);
        assertEquals("some_default", secondaryDefault.get(Settings.EMPTY));
        assertEquals("42", secondaryDefault.get(Settings.builder().put("old.foo.bar", 42).build()));

        Setting<String> secondaryDefaultViaSettings =
            new Setting<>("foo.bar", secondaryDefault, Function.identity(), Property.NodeScope);
        assertEquals("some_default", secondaryDefaultViaSettings.get(Settings.EMPTY));
        assertEquals("42", secondaryDefaultViaSettings.get(Settings.builder().put("old.foo.bar", 42).build()));

        // It gets more complicated when there are two settings objects....
        Settings hasFallback = Settings.builder().put("foo.bar", "o").build();
        Setting<String> fallsback =
                new Setting<>("foo.baz", secondaryDefault, Function.identity(), Property.NodeScope);
        assertEquals("o", fallsback.get(hasFallback));
        assertEquals("some_default", fallsback.get(Settings.EMPTY));
        assertEquals("some_default", fallsback.get(Settings.EMPTY, Settings.EMPTY));
        assertEquals("o", fallsback.get(Settings.EMPTY, hasFallback));
        assertEquals("o", fallsback.get(hasFallback, Settings.EMPTY));
        assertEquals("a", fallsback.get(
                Settings.builder().put("foo.bar", "a").build(),
                Settings.builder().put("foo.bar", "b").build()));
    }

    public void testComplexType() {
        AtomicReference<ComplexType> ref = new AtomicReference<>(null);
        Setting<ComplexType> setting = new Setting<>("foo.bar", (s) -> "", (s) -> new ComplexType(s),
            Property.Dynamic, Property.NodeScope);
        assertFalse(setting.isGroupSetting());
        ref.set(setting.get(Settings.EMPTY));
        ComplexType type = ref.get();
        ClusterSettings.SettingUpdater<ComplexType> settingUpdater = setting.newUpdater(ref::set, logger);
        assertFalse(settingUpdater.apply(Settings.EMPTY, Settings.EMPTY));
        assertSame("no update - type has not changed", type, ref.get());

        // change from default
        assertTrue(settingUpdater.apply(Settings.builder().put("foo.bar", "2").build(), Settings.EMPTY));
        assertNotSame("update - type has changed", type, ref.get());
        assertEquals("2", ref.get().foo);


        // change back to default...
        assertTrue(settingUpdater.apply(Settings.EMPTY, Settings.builder().put("foo.bar", "2").build()));
        assertNotSame("update - type has changed", type, ref.get());
        assertEquals("", ref.get().foo);
    }

    public void testType() {
        Setting<Integer> integerSetting = Setting.intSetting("foo.int.bar", 1, Property.Dynamic, Property.NodeScope);
        assertThat(integerSetting.hasNodeScope(), is(true));
        assertThat(integerSetting.hasIndexScope(), is(false));
        integerSetting = Setting.intSetting("foo.int.bar", 1, Property.Dynamic, Property.IndexScope);
        assertThat(integerSetting.hasIndexScope(), is(true));
        assertThat(integerSetting.hasNodeScope(), is(false));
    }

    public void testGroups() {
        AtomicReference<Settings> ref = new AtomicReference<>(null);
        Setting<Settings> setting = Setting.groupSetting("foo.bar.", Property.Dynamic, Property.NodeScope);
        assertTrue(setting.isGroupSetting());
        ClusterSettings.SettingUpdater<Settings> settingUpdater = setting.newUpdater(ref::set, logger);

        Settings currentInput = Settings.builder()
                .put("foo.bar.1.value", "1")
                .put("foo.bar.2.value", "2")
                .put("foo.bar.3.value", "3").build();
        Settings previousInput = Settings.EMPTY;
        assertTrue(settingUpdater.apply(currentInput, previousInput));
        assertNotNull(ref.get());
        Settings settings = ref.get();
        Map<String, Settings> asMap = settings.getAsGroups();
        assertEquals(3, asMap.size());
        assertEquals(asMap.get("1").get("value"), "1");
        assertEquals(asMap.get("2").get("value"), "2");
        assertEquals(asMap.get("3").get("value"), "3");

        previousInput = currentInput;
        currentInput = Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").put("foo.bar.3.value", "3").build();
        Settings current = ref.get();
        assertFalse(settingUpdater.apply(currentInput, previousInput));
        assertSame(current, ref.get());

        previousInput = currentInput;
        currentInput = Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build();
        // now update and check that we got it
        assertTrue(settingUpdater.apply(currentInput, previousInput));
        assertNotSame(current, ref.get());

        asMap = ref.get().getAsGroups();
        assertEquals(2, asMap.size());
        assertEquals(asMap.get("1").get("value"), "1");
        assertEquals(asMap.get("2").get("value"), "2");

        previousInput = currentInput;
        currentInput = Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "4").build();
        // now update and check that we got it
        assertTrue(settingUpdater.apply(currentInput, previousInput));
        assertNotSame(current, ref.get());

        asMap = ref.get().getAsGroups();
        assertEquals(2, asMap.size());
        assertEquals(asMap.get("1").get("value"), "1");
        assertEquals(asMap.get("2").get("value"), "4");

        assertTrue(setting.match("foo.bar.baz"));
        assertFalse(setting.match("foo.baz.bar"));

        ClusterSettings.SettingUpdater<Settings> predicateSettingUpdater = setting.newUpdater(ref::set, logger,(s) -> assertFalse(true));
        try {
            predicateSettingUpdater.apply(Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build(),
                    Settings.EMPTY);
            fail("not accepted");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "illegal value can't update [foo.bar.] from [{}] to [{1.value=1, 2.value=2}]");
        }
    }

    public static class ComplexType {

        final String foo;

        public ComplexType(String foo) {
            this.foo = foo;
        }
    }

    public static class Composite {

        private Integer b;
        private Integer a;

        public void set(Integer a, Integer b) {
            this.a = a;
            this.b = b;
        }
    }


    public void testComposite() {
        Composite c = new Composite();
        Setting<Integer> a = Setting.intSetting("foo.int.bar.a", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> b = Setting.intSetting("foo.int.bar.b", 1, Property.Dynamic, Property.NodeScope);
        ClusterSettings.SettingUpdater<Tuple<Integer, Integer>> settingUpdater = Setting.compoundUpdater(c::set, a, b, logger);
        assertFalse(settingUpdater.apply(Settings.EMPTY, Settings.EMPTY));
        assertNull(c.a);
        assertNull(c.b);

        Settings build = Settings.builder().put("foo.int.bar.a", 2).build();
        assertTrue(settingUpdater.apply(build, Settings.EMPTY));
        assertEquals(2, c.a.intValue());
        assertEquals(1, c.b.intValue());

        Integer aValue = c.a;
        assertFalse(settingUpdater.apply(build, build));
        assertSame(aValue, c.a);
        Settings previous = build;
        build = Settings.builder().put("foo.int.bar.a", 2).put("foo.int.bar.b", 5).build();
        assertTrue(settingUpdater.apply(build, previous));
        assertEquals(2, c.a.intValue());
        assertEquals(5, c.b.intValue());

        // reset to default
        assertTrue(settingUpdater.apply(Settings.EMPTY, build));
        assertEquals(1, c.a.intValue());
        assertEquals(1, c.b.intValue());

    }

    public void testListSettings() {
        Setting<List<String>> listSetting = Setting.listSetting("foo.bar", Arrays.asList("foo,bar"), (s) -> s.toString(),
            Property.Dynamic, Property.NodeScope);
        List<String> value = listSetting.get(Settings.EMPTY);
        assertFalse(listSetting.exists(Settings.EMPTY));
        assertEquals(1, value.size());
        assertEquals("foo,bar", value.get(0));

        List<String> input = Arrays.asList("test", "test1, test2", "test", ",,,,");
        Settings.Builder builder = Settings.builder().putArray("foo.bar", input.toArray(new String[0]));
        assertTrue(listSetting.exists(builder.build()));
        value = listSetting.get(builder.build());
        assertEquals(input.size(), value.size());
        assertArrayEquals(value.toArray(new String[0]), input.toArray(new String[0]));

        // try to parse this really annoying format
        builder = Settings.builder();
        for (int i = 0; i < input.size(); i++) {
            builder.put("foo.bar." + i, input.get(i));
        }
        value = listSetting.get(builder.build());
        assertEquals(input.size(), value.size());
        assertArrayEquals(value.toArray(new String[0]), input.toArray(new String[0]));
        assertTrue(listSetting.exists(builder.build()));

        AtomicReference<List<String>> ref = new AtomicReference<>();
        AbstractScopedSettings.SettingUpdater<List<String>> settingUpdater = listSetting.newUpdater(ref::set, logger);
        assertTrue(settingUpdater.hasChanged(builder.build(), Settings.EMPTY));
        settingUpdater.apply(builder.build(), Settings.EMPTY);
        assertEquals(input.size(), ref.get().size());
        assertArrayEquals(ref.get().toArray(new String[0]), input.toArray(new String[0]));

        settingUpdater.apply(Settings.builder().putArray("foo.bar", "123").build(), builder.build());
        assertEquals(1, ref.get().size());
        assertArrayEquals(ref.get().toArray(new String[0]), new String[] {"123"});

        settingUpdater.apply(Settings.builder().put("foo.bar", "1,2,3").build(), Settings.builder().putArray("foo.bar", "123").build());
        assertEquals(3, ref.get().size());
        assertArrayEquals(ref.get().toArray(new String[0]), new String[] {"1", "2", "3"});

        settingUpdater.apply(Settings.EMPTY, Settings.builder().put("foo.bar", "1,2,3").build());
        assertEquals(1, ref.get().size());
        assertEquals("foo,bar", ref.get().get(0));

        Setting<List<Integer>> otherSettings = Setting.listSetting("foo.bar", Collections.emptyList(), Integer::parseInt,
            Property.Dynamic, Property.NodeScope);
        List<Integer> defaultValue = otherSettings.get(Settings.EMPTY);
        assertEquals(0, defaultValue.size());
        List<Integer> intValues = otherSettings.get(Settings.builder().put("foo.bar", "0,1,2,3").build());
        assertEquals(4, intValues.size());
        for (int i = 0; i < intValues.size(); i++) {
            assertEquals(i, intValues.get(i).intValue());
        }

        Setting<List<String>> settingWithFallback = Setting.listSetting("foo.baz", listSetting, Function.identity(),
            Property.Dynamic, Property.NodeScope);
        value = settingWithFallback.get(Settings.EMPTY);
        assertEquals(1, value.size());
        assertEquals("foo,bar", value.get(0));

        value = settingWithFallback.get(Settings.builder().putArray("foo.bar", "1", "2").build());
        assertEquals(2, value.size());
        assertEquals("1", value.get(0));
        assertEquals("2", value.get(1));

        value = settingWithFallback.get(Settings.builder().putArray("foo.baz", "3", "4").build());
        assertEquals(2, value.size());
        assertEquals("3", value.get(0));
        assertEquals("4", value.get(1));

        value = settingWithFallback.get(Settings.builder().putArray("foo.baz", "3", "4").putArray("foo.bar", "1", "2").build());
        assertEquals(2, value.size());
        assertEquals("3", value.get(0));
        assertEquals("4", value.get(1));
    }

    public void testListSettingAcceptsNumberSyntax() {
        Setting<List<String>> listSetting = Setting.listSetting("foo.bar", Arrays.asList("foo,bar"), (s) -> s.toString(),
            Property.Dynamic, Property.NodeScope);
        List<String> input = Arrays.asList("test", "test1, test2", "test", ",,,,");
        Settings.Builder builder = Settings.builder().putArray("foo.bar", input.toArray(new String[0]));
        // try to parse this really annoying format
        for (String key : builder.internalMap().keySet()) {
            assertTrue("key: " + key + " doesn't match", listSetting.match(key));
        }
        builder = Settings.builder().put("foo.bar", "1,2,3");
        for (String key : builder.internalMap().keySet()) {
            assertTrue("key: " + key + " doesn't match", listSetting.match(key));
        }
        assertFalse(listSetting.match("foo_bar"));
        assertFalse(listSetting.match("foo_bar.1"));
        assertTrue(listSetting.match("foo.bar"));
        assertTrue(listSetting.match("foo.bar." + randomIntBetween(0,10000)));
    }

    public void testDynamicKeySetting() {
        Setting<Boolean> setting = Setting.prefixKeySetting("foo.", "false", Boolean::parseBoolean, Property.NodeScope);
        assertTrue(setting.hasComplexMatcher());
        assertTrue(setting.match("foo.bar"));
        assertFalse(setting.match("foo"));
        Setting<Boolean> concreteSetting = setting.getConcreteSetting("foo.bar");
        assertTrue(concreteSetting.get(Settings.builder().put("foo.bar", "true").build()));
        assertFalse(concreteSetting.get(Settings.builder().put("foo.baz", "true").build()));

        try {
            setting.getConcreteSetting("foo");
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("key [foo] must match [foo.] but didn't.", ex.getMessage());
        }
    }

    public void testAdfixKeySetting() {
        Setting<Boolean> setting =
            Setting.adfixKeySetting("foo", "enable", "false", Boolean::parseBoolean, Property.NodeScope);
        assertTrue(setting.hasComplexMatcher());
        assertTrue(setting.match("foo.bar.enable"));
        assertTrue(setting.match("foo.baz.enable"));
        assertTrue(setting.match("foo.bar.baz.enable"));
        assertFalse(setting.match("foo.bar"));
        assertFalse(setting.match("foo.bar.baz.enabled"));
        assertFalse(setting.match("foo"));
        Setting<Boolean> concreteSetting = setting.getConcreteSetting("foo.bar.enable");
        assertTrue(concreteSetting.get(Settings.builder().put("foo.bar.enable", "true").build()));
        assertFalse(concreteSetting.get(Settings.builder().put("foo.baz.enable", "true").build()));

        try {
            setting.getConcreteSetting("foo");
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("key [foo] must match [foo*enable.] but didn't.", ex.getMessage());
        }
    }

    public void testMinMaxInt() {
        Setting<Integer> integerSetting = Setting.intSetting("foo.bar", 1, 0, 10, Property.NodeScope);
        try {
            integerSetting.get(Settings.builder().put("foo.bar", 11).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [11] for setting [foo.bar] must be <= 10", ex.getMessage());
        }

        try {
            integerSetting.get(Settings.builder().put("foo.bar", -1).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [-1] for setting [foo.bar] must be >= 0", ex.getMessage());
        }

        assertEquals(5, integerSetting.get(Settings.builder().put("foo.bar", 5).build()).intValue());
        assertEquals(1, integerSetting.get(Settings.EMPTY).intValue());
    }

    /**
     * Only one single scope can be added to any setting
     */
    public void testMutuallyExclusiveScopes() {
        // Those should pass
        Setting<String> setting = Setting.simpleString("foo.bar", Property.NodeScope);
        assertThat(setting.hasNodeScope(), is(true));
        assertThat(setting.hasIndexScope(), is(false));
        setting = Setting.simpleString("foo.bar", Property.IndexScope);
        assertThat(setting.hasIndexScope(), is(true));
        assertThat(setting.hasNodeScope(), is(false));

        // We accept settings with no scope but they will be rejected when we register with SettingsModule.registerSetting
        setting = Setting.simpleString("foo.bar");
        assertThat(setting.hasIndexScope(), is(false));
        assertThat(setting.hasNodeScope(), is(false));

        // We accept settings with multiple scopes but they will be rejected when we register with SettingsModule.registerSetting
        setting = Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope);
        assertThat(setting.hasIndexScope(), is(true));
        assertThat(setting.hasNodeScope(), is(true));
    }

    /**
     * We can't have Null properties
     */
    public void testRejectNullProperties() {
        try {
            Setting.simpleString("foo.bar", (Property[]) null);
            fail();
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), containsString("properties cannot be null for setting"));
        }
    }
}
