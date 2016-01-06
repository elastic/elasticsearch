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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class SettingTests extends ESTestCase {


    public void testGet() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, true, Setting.Scope.CLUSTER);
        assertFalse(booleanSetting.get(Settings.EMPTY));
        assertFalse(booleanSetting.get(Settings.builder().put("foo.bar", false).build()));
        assertTrue(booleanSetting.get(Settings.builder().put("foo.bar", true).build()));
    }

    public void testByteSize() {
        Setting<ByteSizeValue> byteSizeValueSetting = Setting.byteSizeSetting("a.byte.size", new ByteSizeValue(1024), true, Setting.Scope.CLUSTER);
        assertFalse(byteSizeValueSetting.isGroupSetting());
        ByteSizeValue byteSizeValue = byteSizeValueSetting.get(Settings.EMPTY);
        assertEquals(byteSizeValue.bytes(), 1024);
        AtomicReference<ByteSizeValue> value = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater settingUpdater = byteSizeValueSetting.newUpdater(value::set, logger);
        try {
            settingUpdater.apply(Settings.builder().put("a.byte.size", 12).build(), Settings.EMPTY);
            fail("no unit");
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse setting [a.byte.size] with value [12] as a size in bytes: unit is missing or unrecognized", ex.getMessage());
        }

        assertTrue(settingUpdater.apply(Settings.builder().put("a.byte.size", "12b").build(), Settings.EMPTY));
        assertEquals(new ByteSizeValue(12), value.get());
    }

    public void testSimpleUpdate() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, true, Setting.Scope.CLUSTER);
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater settingUpdater = booleanSetting.newUpdater(atomicBoolean::set, logger);
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
            assertEquals("Failed to parse value [I am not a boolean] cannot be parsed to boolean [ true/1/on/yes OR false/0/off/no ]", ex.getMessage());
        }
    }

    public void testUpdateNotDynamic() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, false, Setting.Scope.CLUSTER);
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
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, true, Setting.Scope.CLUSTER);
        AtomicReference<Boolean> ab1 = new AtomicReference<>(null);
        AtomicReference<Boolean> ab2 = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater settingUpdater = booleanSetting.newUpdater(ab1::set, logger);
        ClusterSettings.SettingUpdater settingUpdater2 = booleanSetting.newUpdater(ab2::set, logger);
        settingUpdater.apply(Settings.builder().put("foo.bar", true).build(), Settings.EMPTY);
        assertTrue(ab1.get());
        assertNull(ab2.get());
    }

    public void testDefault() {
        TimeValue defautlValue = TimeValue.timeValueMillis(randomIntBetween(0, 1000000));
        Setting<TimeValue> setting = Setting.positiveTimeSetting("my.time.value", defautlValue, randomBoolean(), Setting.Scope.CLUSTER);
        assertFalse(setting.isGroupSetting());
        String aDefault = setting.getDefault(Settings.EMPTY);
        assertEquals(defautlValue.millis() + "ms", aDefault);
        assertEquals(defautlValue.millis(), setting.get(Settings.EMPTY).millis());

        Setting<String> secondaryDefault = new Setting<>("foo.bar", (s) -> s.get("old.foo.bar", "some_default"), (s) -> s, randomBoolean(), Setting.Scope.CLUSTER);
        assertEquals("some_default", secondaryDefault.get(Settings.EMPTY));
        assertEquals("42", secondaryDefault.get(Settings.builder().put("old.foo.bar", 42).build()));
    }

    public void testComplexType() {
        AtomicReference<ComplexType> ref = new AtomicReference<>(null);
        Setting<ComplexType> setting = new Setting<>("foo.bar", (s) -> "", (s) -> new ComplexType(s), true, Setting.Scope.CLUSTER);
        assertFalse(setting.isGroupSetting());
        ref.set(setting.get(Settings.EMPTY));
        ComplexType type = ref.get();
        ClusterSettings.SettingUpdater settingUpdater = setting.newUpdater(ref::set, logger);
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
        Setting<Integer> integerSetting = Setting.intSetting("foo.int.bar", 1, true, Setting.Scope.CLUSTER);
        assertEquals(integerSetting.getScope(), Setting.Scope.CLUSTER);
        integerSetting = Setting.intSetting("foo.int.bar", 1, true, Setting.Scope.INDEX);
        assertEquals(integerSetting.getScope(), Setting.Scope.INDEX);
    }

    public void testGroups() {
        AtomicReference<Settings> ref = new AtomicReference<>(null);
        Setting<Settings> setting = Setting.groupSetting("foo.bar.", true, Setting.Scope.CLUSTER);
        assertTrue(setting.isGroupSetting());
        ClusterSettings.SettingUpdater settingUpdater = setting.newUpdater(ref::set, logger);

        Settings currentInput = Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").put("foo.bar.3.value", "3").build();
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

        ClusterSettings.SettingUpdater predicateSettingUpdater = setting.newUpdater(ref::set, logger,(s) -> assertFalse(true));
        try {
            predicateSettingUpdater.apply(Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build(), Settings.EMPTY);
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
        Setting<Integer> a = Setting.intSetting("foo.int.bar.a", 1, true, Setting.Scope.CLUSTER);
        Setting<Integer> b = Setting.intSetting("foo.int.bar.b", 1, true, Setting.Scope.CLUSTER);
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
        Setting<List<String>> listSetting = Setting.listSetting("foo.bar", Arrays.asList("foo,bar"), (s) -> s.toString(), true, Setting.Scope.CLUSTER);
        List<String> value = listSetting.get(Settings.EMPTY);
        assertEquals(1, value.size());
        assertEquals("foo,bar", value.get(0));

        List<String> input = Arrays.asList("test", "test1, test2", "test", ",,,,");
        Settings.Builder builder = Settings.builder().putArray("foo.bar", input.toArray(new String[0]));
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

        AtomicReference<List<String>> ref = new AtomicReference<>();
        AbstractScopedSettings.SettingUpdater settingUpdater = listSetting.newUpdater(ref::set, logger);
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

        Setting<List<Integer>> otherSettings = Setting.listSetting("foo.bar", Collections.emptyList(), Integer::parseInt, true, Setting.Scope.CLUSTER);
        List<Integer> defaultValue = otherSettings.get(Settings.EMPTY);
        assertEquals(0, defaultValue.size());
        List<Integer> intValues = otherSettings.get(Settings.builder().put("foo.bar", "0,1,2,3").build());
        assertEquals(4, intValues.size());
        for (int i = 0; i < intValues.size(); i++) {
            assertEquals(i, intValues.get(i).intValue());
        }
    }

    public void testListSettingAcceptsNumberSyntax() {
        Setting<List<String>> listSetting = Setting.listSetting("foo.bar", Arrays.asList("foo,bar"), (s) -> s.toString(), true, Setting.Scope.CLUSTER);
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
}
