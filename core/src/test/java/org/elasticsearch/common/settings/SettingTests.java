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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class SettingTests extends ESTestCase {


    public void testGet() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, true, Setting.Scope.Cluster);
        assertFalse(booleanSetting.get(Settings.EMPTY));
        assertFalse(booleanSetting.get(Settings.builder().put("foo.bar", false).build()));
        assertTrue(booleanSetting.get(Settings.builder().put("foo.bar", true).build()));
    }

    public void testByteSize() {
        Setting<ByteSizeValue> byteSizeValueSetting = Setting.byteSizeSetting("a.byte.size", new ByteSizeValue(1024), true, Setting.Scope.Cluster);
        assertFalse(byteSizeValueSetting.isGroupSetting());
        ByteSizeValue byteSizeValue = byteSizeValueSetting.get(Settings.EMPTY);
        assertEquals(byteSizeValue.bytes(), 1024);
        AtomicReference<ByteSizeValue> value = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater settingUpdater = byteSizeValueSetting.newUpdater(value::set, logger, Settings.EMPTY);
        try {
            settingUpdater.prepareApply(Settings.builder().put("a.byte.size", 12).build());
            fail("no unit");
        } catch (ElasticsearchParseException ex) {
            assertEquals("failed to parse setting [a.byte.size] with value [12] as a size in bytes: unit is missing or unrecognized", ex.getMessage());
        }

        assertTrue(settingUpdater.prepareApply(Settings.builder().put("a.byte.size", "12b").build()));
        settingUpdater.apply();
        assertEquals(new ByteSizeValue(12), value.get());
    }

    public void testSimpleUpdate() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, true, Setting.Scope.Cluster);
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater settingUpdater = booleanSetting.newUpdater(atomicBoolean::set, logger, Settings.EMPTY);
        Settings build = Settings.builder().put("foo.bar", false).build();
        settingUpdater.prepareApply(build);
        assertNull(atomicBoolean.get());
        settingUpdater.rollback();
        assertNull(atomicBoolean.get());
        build = Settings.builder().put("foo.bar", true).build();
        settingUpdater.prepareApply(build);
        assertNull(atomicBoolean.get());
        settingUpdater.apply();
        assertTrue(atomicBoolean.get());

        // try update bogus value
        build = Settings.builder().put("foo.bar", "I am not a boolean").build();
        try {
            settingUpdater.prepareApply(build);
            fail("not a boolean");
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [I am not a boolean] for setting [foo.bar]", ex.getMessage());
        }
    }

    public void testUpdateNotDynamic() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, false, Setting.Scope.Cluster);
        assertFalse(booleanSetting.isGroupSetting());
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        try {
            booleanSetting.newUpdater(atomicBoolean::set, logger, Settings.EMPTY);
            fail("not dynamic");
        } catch (IllegalStateException ex) {
            assertEquals("setting [foo.bar] is not dynamic", ex.getMessage());
        }
    }

    public void testUpdaterIsIsolated() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, true, Setting.Scope.Cluster);
        AtomicReference<Boolean> ab1 = new AtomicReference<>(null);
        AtomicReference<Boolean> ab2 = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater settingUpdater = booleanSetting.newUpdater(ab1::set, logger, Settings.EMPTY);
        settingUpdater.prepareApply(Settings.builder().put("foo.bar", true).build());
        assertNull(ab1.get());
        assertNull(ab2.get());
        settingUpdater.apply();
        assertTrue(ab1.get());
        assertNull(ab2.get());
    }

    public void testDefault() {
        TimeValue defautlValue = TimeValue.timeValueMillis(randomIntBetween(0, 1000000));
        Setting<TimeValue> setting = Setting.positiveTimeSetting("my.time.value", defautlValue, randomBoolean(), Setting.Scope.Cluster);
        assertFalse(setting.isGroupSetting());
        String aDefault = setting.getDefault(Settings.EMPTY);
        assertEquals(defautlValue.millis() + "ms", aDefault);
        assertEquals(defautlValue.millis(), setting.get(Settings.EMPTY).millis());

        Setting<String> secondaryDefault = new Setting<>("foo.bar", (s) -> s.get("old.foo.bar", "some_default"), (s) -> s, randomBoolean(), Setting.Scope.Cluster);
        assertEquals("some_default", secondaryDefault.get(Settings.EMPTY));
        assertEquals("42", secondaryDefault.get(Settings.builder().put("old.foo.bar", 42).build()));
    }

    public void testComplexType() {
        AtomicReference<ComplexType> ref = new AtomicReference<>(null);
        Setting<ComplexType> setting = new Setting<>("foo.bar", (s) -> "", (s) -> new ComplexType(s), true, Setting.Scope.Cluster);
        assertFalse(setting.isGroupSetting());
        ref.set(setting.get(Settings.EMPTY));
        ComplexType type = ref.get();
        ClusterSettings.SettingUpdater settingUpdater = setting.newUpdater(ref::set, logger, Settings.EMPTY);
        assertFalse(settingUpdater.prepareApply(Settings.EMPTY));
        settingUpdater.apply();
        assertSame("no update - type has not changed", type, ref.get());

        // change from default
        assertTrue(settingUpdater.prepareApply(Settings.builder().put("foo.bar", "2").build()));
        settingUpdater.apply();
        assertNotSame("update - type has changed", type, ref.get());
        assertEquals("2", ref.get().foo);


        // change back to default...
        assertTrue(settingUpdater.prepareApply(Settings.builder().put("foo.bar.baz", "2").build()));
        settingUpdater.apply();
        assertNotSame("update - type has changed", type, ref.get());
        assertEquals("", ref.get().foo);
    }

    public void testRollback() {
        Setting<Integer> integerSetting = Setting.intSetting("foo.int.bar", 1, true, Setting.Scope.Cluster);
        assertFalse(integerSetting.isGroupSetting());
        AtomicReference<Integer> ref = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater settingUpdater = integerSetting.newUpdater(ref::set, logger, Settings.EMPTY);
        assertNull(ref.get());
        assertTrue(settingUpdater.prepareApply(Settings.builder().put("foo.int.bar", "2").build()));
        settingUpdater.rollback();
        settingUpdater.apply();
        assertNull(ref.get());
        assertTrue(settingUpdater.prepareApply(Settings.builder().put("foo.int.bar", "2").build()));
        settingUpdater.apply();
        assertEquals(2, ref.get().intValue());
    }

    public void testType() {
        Setting<Integer> integerSetting = Setting.intSetting("foo.int.bar", 1, true, Setting.Scope.Cluster);
        assertEquals(integerSetting.getScope(), Setting.Scope.Cluster);
        integerSetting = Setting.intSetting("foo.int.bar", 1, true, Setting.Scope.Index);
        assertEquals(integerSetting.getScope(), Setting.Scope.Index);
    }

    public void testGroups() {
        AtomicReference<Settings> ref = new AtomicReference<>(null);
        Setting<Settings> setting = Setting.groupSetting("foo.bar.", true, Setting.Scope.Cluster);
        assertTrue(setting.isGroupSetting());
        ClusterSettings.SettingUpdater settingUpdater = setting.newUpdater(ref::set, logger, Settings.EMPTY);

        assertTrue(settingUpdater.prepareApply(Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").put("foo.bar.3.value", "3").build()));
        settingUpdater.apply();
        assertNotNull(ref.get());
        Settings settings = ref.get();
        Map<String, Settings> asMap = settings.getAsGroups();
        assertEquals(3, asMap.size());
        assertEquals(asMap.get("1").get("value"), "1");
        assertEquals(asMap.get("2").get("value"), "2");
        assertEquals(asMap.get("3").get("value"), "3");

        Settings current = ref.get();
        assertFalse(settingUpdater.prepareApply(Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").put("foo.bar.3.value", "3").build()));
        settingUpdater.apply();
        assertSame(current, ref.get());

        // now update and check that we got it
        assertTrue(settingUpdater.prepareApply(Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build()));
        settingUpdater.apply();
        assertNotSame(current, ref.get());

        asMap = ref.get().getAsGroups();
        assertEquals(2, asMap.size());
        assertEquals(asMap.get("1").get("value"), "1");
        assertEquals(asMap.get("2").get("value"), "2");

        // now update and check that we got it
        assertTrue(settingUpdater.prepareApply(Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "4").build()));
        settingUpdater.apply();
        assertNotSame(current, ref.get());

        asMap = ref.get().getAsGroups();
        assertEquals(2, asMap.size());
        assertEquals(asMap.get("1").get("value"), "1");
        assertEquals(asMap.get("2").get("value"), "4");

        assertTrue(setting.match("foo.bar.baz"));
        assertFalse(setting.match("foo.baz.bar"));

        ClusterSettings.SettingUpdater predicateSettingUpdater = setting.newUpdater(ref::set, logger, Settings.EMPTY, (s) -> assertFalse(true));
        try {
            predicateSettingUpdater.prepareApply(Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build());
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
        Setting<Integer> a = Setting.intSetting("foo.int.bar.a", 1, true, Setting.Scope.Cluster);
        Setting<Integer> b = Setting.intSetting("foo.int.bar.b", 1, true, Setting.Scope.Cluster);
        ClusterSettings.SettingUpdater settingUpdater = Setting.compoundUpdater(c::set, a, b, logger, Settings.EMPTY);
        assertFalse(settingUpdater.prepareApply(Settings.EMPTY));
        settingUpdater.apply();
        assertNull(c.a);
        assertNull(c.b);

        Settings build = Settings.builder().put("foo.int.bar.a", 2).build();
        assertTrue(settingUpdater.prepareApply(build));
        settingUpdater.apply();
        assertEquals(2, c.a.intValue());
        assertNull(c.b);

        Integer aValue = c.a;
        assertFalse(settingUpdater.prepareApply(build));
        settingUpdater.apply();
        assertSame(aValue, c.a);

        build = Settings.builder().put("foo.int.bar.a", 2).put("foo.int.bar.b", 5).build();
        assertTrue(settingUpdater.prepareApply(build));
        settingUpdater.apply();
        assertEquals(2, c.a.intValue());
        assertEquals(5, c.b.intValue());

        // reset to default
        assertTrue(settingUpdater.prepareApply(Settings.EMPTY));
        settingUpdater.apply();
        assertEquals(1, c.a.intValue());
        assertEquals(1, c.b.intValue());

    }
}
