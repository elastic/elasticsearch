/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.settings;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.AbstractScopedSettings.SettingUpdater;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SettingTests extends ESTestCase {

    public void testGet() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope);
        assertFalse(booleanSetting.get(Settings.EMPTY));
        assertFalse(booleanSetting.get(Settings.builder().put("foo.bar", false).build()));
        assertTrue(booleanSetting.get(Settings.builder().put("foo.bar", true).build()));
    }

    public void testByteSizeSetting() {
        final Setting<ByteSizeValue> byteSizeValueSetting = Setting.byteSizeSetting(
            "a.byte.size",
            ByteSizeValue.ofBytes(1024),
            Property.Dynamic,
            Property.NodeScope
        );
        assertFalse(byteSizeValueSetting.isGroupSetting());
        final ByteSizeValue byteSizeValue = byteSizeValueSetting.get(Settings.EMPTY);
        assertThat(byteSizeValue.getBytes(), equalTo(1024L));
    }

    public void testByteSizeSettingMinValue() {
        final Setting<ByteSizeValue> byteSizeValueSetting = Setting.byteSizeSetting(
            "a.byte.size",
            ByteSizeValue.of(100, ByteSizeUnit.MB),
            ByteSizeValue.of(20_000_000, ByteSizeUnit.BYTES),
            ByteSizeValue.ofBytes(Integer.MAX_VALUE)
        );
        final long value = 20_000_000 - randomIntBetween(1, 1024);
        final Settings settings = Settings.builder().put("a.byte.size", value + "b").build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> byteSizeValueSetting.get(settings));
        final String expectedMessage = "failed to parse value [" + value + "b] for setting [a.byte.size], must be >= [20000000b]";
        assertThat(e, hasToString(containsString(expectedMessage)));
    }

    public void testByteSizeSettingMaxValue() {
        final Setting<ByteSizeValue> byteSizeValueSetting = Setting.byteSizeSetting(
            "a.byte.size",
            ByteSizeValue.of(100, ByteSizeUnit.MB),
            ByteSizeValue.of(16, ByteSizeUnit.MB),
            ByteSizeValue.ofBytes(Integer.MAX_VALUE)
        );
        final long value = (1L << 31) - 1 + randomIntBetween(1, 1024);
        final Settings settings = Settings.builder().put("a.byte.size", value + "b").build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> byteSizeValueSetting.get(settings));
        final String expectedMessage = "failed to parse value [" + value + "b] for setting [a.byte.size], must be <= [2147483647b]";
        assertThat(e, hasToString(containsString(expectedMessage)));
    }

    public void testByteSizeSettingValidation() {
        final Setting<ByteSizeValue> byteSizeValueSetting = Setting.byteSizeSetting(
            "a.byte.size",
            s -> "2048b",
            Property.Dynamic,
            Property.NodeScope
        );
        final ByteSizeValue byteSizeValue = byteSizeValueSetting.get(Settings.EMPTY);
        assertThat(byteSizeValue.getBytes(), equalTo(2048L));
        AtomicReference<ByteSizeValue> value = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<ByteSizeValue> settingUpdater = byteSizeValueSetting.newUpdater(value::set, logger);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> settingUpdater.apply(Settings.builder().put("a.byte.size", 12).build(), Settings.EMPTY)
        );
        assertThat(e, hasToString(containsString("illegal value can't update [a.byte.size] from [2048b] to [12]")));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String expected = "failed to parse setting [a.byte.size] with value [12] as a size in bytes: unit is missing or unrecognized";
        assertThat(cause, hasToString(containsString(expected)));
        assertTrue(settingUpdater.apply(Settings.builder().put("a.byte.size", "12b").build(), Settings.EMPTY));
        assertThat(value.get(), equalTo(ByteSizeValue.ofBytes(12)));
    }

    public void testMemorySize() {
        Setting<ByteSizeValue> memorySizeValueSetting = Setting.memorySizeSetting(
            "a.byte.size",
            ByteSizeValue.ofBytes(1024),
            Property.Dynamic,
            Property.NodeScope
        );

        assertFalse(memorySizeValueSetting.isGroupSetting());
        ByteSizeValue memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertEquals(memorySizeValue.getBytes(), 1024);

        memorySizeValueSetting = Setting.memorySizeSetting("a.byte.size", s -> "2048b", Property.Dynamic, Property.NodeScope);
        memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertEquals(memorySizeValue.getBytes(), 2048);

        memorySizeValueSetting = Setting.memorySizeSetting("a.byte.size", "50%", Property.Dynamic, Property.NodeScope);
        assertFalse(memorySizeValueSetting.isGroupSetting());
        memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertEquals(memorySizeValue.getBytes(), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.5, 1.0);

        memorySizeValueSetting = Setting.memorySizeSetting("a.byte.size", s -> "25%", Property.Dynamic, Property.NodeScope);
        memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertEquals(memorySizeValue.getBytes(), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.25, 1.0);

        AtomicReference<ByteSizeValue> value = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<ByteSizeValue> settingUpdater = memorySizeValueSetting.newUpdater(value::set, logger);
        try {
            settingUpdater.apply(Settings.builder().put("a.byte.size", 12).build(), Settings.EMPTY);
            fail("no unit");
        } catch (IllegalArgumentException ex) {
            assertThat(ex, hasToString(containsString("illegal value can't update [a.byte.size] from [25%] to [12]")));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            final IllegalArgumentException cause = (IllegalArgumentException) ex.getCause();
            final String expected =
                "failed to parse setting [a.byte.size] with value [12] as a size in bytes: unit is missing or unrecognized";
            assertThat(cause, hasToString(containsString(expected)));
        }

        assertTrue(settingUpdater.apply(Settings.builder().put("a.byte.size", "12b").build(), Settings.EMPTY));
        assertEquals(ByteSizeValue.ofBytes(12), value.get());

        assertTrue(settingUpdater.apply(Settings.builder().put("a.byte.size", "20%").build(), Settings.EMPTY));
        assertEquals(ByteSizeValue.ofBytes((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.2)), value.get());
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
            assertThat(ex, hasToString(containsString("illegal value can't update [foo.bar] from [false] to [I am not a boolean]")));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            final IllegalArgumentException cause = (IllegalArgumentException) ex.getCause();
            assertThat(
                cause,
                hasToString(containsString("Failed to parse value [I am not a boolean] as only [true] or [false] are allowed."))
            );
        }
    }

    public void testSimpleUpdateOfFilteredSetting() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.Filtered);
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<Boolean> settingUpdater = booleanSetting.newUpdater(atomicBoolean::set, logger);

        // try update bogus value
        Settings build = Settings.builder().put("foo.bar", "I am not a boolean").build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> settingUpdater.apply(build, Settings.EMPTY));
        assertThat(ex, hasToString(equalTo("java.lang.IllegalArgumentException: illegal value can't update [foo.bar]")));
        assertNull(ex.getCause());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33135")
    public void testValidateStringSetting() {
        Settings settings = Settings.builder().putList("foo.bar", Arrays.asList("bla-a", "bla-b")).build();
        Setting<String> stringSetting = Setting.simpleString("foo.bar", Property.NodeScope);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stringSetting.get(settings));
        assertEquals("Found list type value for setting [foo.bar] but but did not expect a list for it.", e.getMessage());
    }

    private static final Setting<String> FOO_BAR_SETTING = new Setting<>(
        "foo.bar",
        "foobar",
        Function.identity(),
        new FooBarValidator(),
        Property.Dynamic,
        Property.NodeScope
    );

    private static final Setting<String> BAZ_QUX_SETTING = Setting.simpleString("baz.qux", Property.NodeScope);
    private static final Setting<String> QUUX_QUUZ_SETTING = Setting.simpleString("quux.quuz", Property.NodeScope);

    static class FooBarValidator implements Setting.Validator<String> {

        public static boolean invokedInIsolation;
        public static boolean invokedWithDependencies;

        @Override
        public void validate(final String value) {
            invokedInIsolation = true;
            assertThat(value, equalTo("foo.bar value"));
        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            invokedWithDependencies = true;
            assertTrue(settings.keySet().contains(BAZ_QUX_SETTING));
            assertThat(settings.get(BAZ_QUX_SETTING), equalTo("baz.qux value"));
            assertTrue(settings.keySet().contains(QUUX_QUUZ_SETTING));
            assertThat(settings.get(QUUX_QUUZ_SETTING), equalTo("quux.quuz value"));
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(BAZ_QUX_SETTING, QUUX_QUUZ_SETTING);
            return settings.iterator();
        }
    }

    // the purpose of this test is merely to ensure that a validator is invoked with the appropriate values
    public void testValidator() {
        final Settings settings = Settings.builder()
            .put("foo.bar", "foo.bar value")
            .put("baz.qux", "baz.qux value")
            .put("quux.quuz", "quux.quuz value")
            .build();
        FOO_BAR_SETTING.get(settings);
        assertTrue(FooBarValidator.invokedInIsolation);
        assertTrue(FooBarValidator.invokedWithDependencies);
    }

    public void testDuplicateSettingsPrefersPrimary() {
        Setting<String> fooBar = new Setting<>("foo.bar", new Setting<>("baz.qux", "", Function.identity()), Function.identity());
        assertThat(
            fooBar.get(Settings.builder().put("foo.bar", "primaryUsed").put("baz.qux", "fallbackUsed").build()),
            equalTo("primaryUsed")
        );
        assertThat(
            fooBar.get(Settings.builder().put("baz.qux", "fallbackUsed").put("foo.bar", "primaryUsed").build()),
            equalTo("primaryUsed")
        );
    }

    public void testValidatorForFilteredStringSetting() {
        final Setting<String> filteredStringSetting = new Setting<>("foo.bar", "foobar", Function.identity(), value -> {
            throw new SettingsException("validate always fails");
        }, Property.Filtered);

        final Settings settings = Settings.builder().put(filteredStringSetting.getKey(), filteredStringSetting.getKey() + " value").build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> filteredStringSetting.get(settings));
        assertThat(e, hasToString(containsString("Failed to parse value for setting [" + filteredStringSetting.getKey() + "]")));
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(e.getCause(), hasToString(containsString("validate always fails")));
    }

    public void testFilteredFloatSetting() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.floatSetting("foo", 42.0f, 43.0f, Property.Filtered)
        );
        assertThat(e, hasToString(containsString("Failed to parse value for setting [foo] must be >= 43.0")));
    }

    public void testFilteredDoubleSetting() {
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.doubleSetting("foo", 42.0, 43.0, Property.Filtered)
        );
        assertThat(e1, hasToString(containsString("Failed to parse value for setting [foo] must be >= 43.0")));

        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.doubleSetting("foo", 45.0, 43.0, 44.0, Property.Filtered)
        );
        assertThat(e2, hasToString(containsString("Failed to parse value for setting [foo] must be <= 44.0")));
    }

    public void testFilteredIntSetting() {
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.intSetting("foo", 42, 43, 44, Property.Filtered)
        );
        assertThat(e1, hasToString(containsString("Failed to parse value for setting [foo] must be >= 43")));

        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.intSetting("foo", 45, 43, 44, Property.Filtered)
        );
        assertThat(e2, hasToString(containsString("Failed to parse value for setting [foo] must be <= 44")));
    }

    public void testFilteredLongSetting() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.longSetting("foo", 42L, 43L, Property.Filtered)
        );
        assertThat(e, hasToString(containsString("Failed to parse value for setting [foo] must be >= 43")));
    }

    public void testFilteredTimeSetting() {
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.timeSetting("foo", TimeValue.timeValueHours(1), TimeValue.timeValueHours(2), Property.Filtered)
        );
        assertThat(e1, hasToString(containsString("failed to parse value for setting [foo], must be >= [2h]")));

        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.timeSetting(
                "foo",
                TimeValue.timeValueHours(4),
                TimeValue.timeValueHours(2),
                TimeValue.timeValueHours(3),
                Property.Filtered
            )
        );
        assertThat(e2, hasToString(containsString("failed to parse value for setting [foo], must be <= [3h]")));

        final Setting<TimeValue> minSetting = Setting.timeSetting(
            "foo",
            TimeValue.timeValueHours(3),
            TimeValue.timeValueHours(2),
            Property.Filtered
        );
        final Settings minSettings = Settings.builder().put("foo", "not a time value").build();
        final IllegalArgumentException e3 = expectThrows(IllegalArgumentException.class, () -> minSetting.get(minSettings));
        assertThat(e3, hasToString(containsString("failed to parse value for setting [foo] as a time value")));
        assertNull(e3.getCause());

        final Setting<TimeValue> maxSetting = Setting.timeSetting(
            "foo",
            TimeValue.timeValueHours(3),
            TimeValue.timeValueHours(2),
            TimeValue.timeValueHours(4),
            Property.Filtered
        );
        final Settings maxSettings = Settings.builder().put("foo", "not a time value").build();
        final IllegalArgumentException e4 = expectThrows(IllegalArgumentException.class, () -> maxSetting.get(maxSettings));
        assertThat(e4, hasToString(containsString("failed to parse value for setting [foo] as a time value")));
        assertNull(e4.getCause());
    }

    public void testFilteredBooleanSetting() {
        Setting<Boolean> setting = Setting.boolSetting("foo", false, Property.Filtered);
        final Settings settings = Settings.builder().put("foo", "not a boolean value").build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertThat(e, hasToString(containsString("Failed to parse value for setting [foo]")));
        assertNull(e.getCause());
    }

    public void testFloatSettingWithOtherSettingAsDefault() {
        float defaultFallbackValue = randomFloat();
        Setting<Float> fallbackSetting = Setting.floatSetting("fallback_setting", defaultFallbackValue);
        Setting<Float> floatSetting = Setting.floatSetting("float_setting", fallbackSetting, Float.MIN_VALUE);

        // Neither float_setting nor fallback_setting specified
        assertThat(floatSetting.get(Settings.builder().build()), equalTo(defaultFallbackValue));

        // Only fallback_setting specified
        float explicitFallbackValue = randomValueOtherThan(defaultFallbackValue, ESTestCase::randomFloat);
        assertThat(
            floatSetting.get(Settings.builder().put("fallback_setting", explicitFallbackValue).build()),
            equalTo(explicitFallbackValue)
        );

        // Both float_setting and fallback_setting specified
        float explicitFloatValue = randomValueOtherThanMany(
            v -> v != explicitFallbackValue && v != defaultFallbackValue,
            ESTestCase::randomFloat
        );
        assertThat(
            floatSetting.get(
                Settings.builder().put("fallback_setting", explicitFallbackValue).put("float_setting", explicitFloatValue).build()
            ),
            equalTo(explicitFloatValue)
        );
    }

    private enum TestEnum {
        ON,
        OFF
    }

    public void testThrowsIllegalArgumentExceptionOnInvalidEnumSetting() {
        Setting<TestEnum> setting = Setting.enumSetting(TestEnum.class, "foo", TestEnum.ON, Property.Filtered);
        final Settings settings = Settings.builder().put("foo", "bar").build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertThat(e, hasToString(containsString("No enum constant org.elasticsearch.common.settings.SettingTests.TestEnum.BAR")));
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
        Setting<TimeValue> setting = Setting.positiveTimeSetting("my.time.value", defaultValue, Property.NodeScope);
        assertFalse(setting.isGroupSetting());
        String aDefault = setting.getDefaultRaw(Settings.EMPTY);
        assertEquals(defaultValue.millis() + "ms", aDefault);
        assertEquals(defaultValue.millis(), setting.get(Settings.EMPTY).millis());
        assertEquals(defaultValue, setting.getDefault(Settings.EMPTY));

        Setting<String> secondaryDefault = new Setting<>(
            "foo.bar",
            (s) -> s.get("old.foo.bar", "some_default"),
            Function.identity(),
            Property.NodeScope
        );
        assertEquals("some_default", secondaryDefault.get(Settings.EMPTY));
        assertEquals("42", secondaryDefault.get(Settings.builder().put("old.foo.bar", 42).build()));

        Setting<String> secondaryDefaultViaSettings = new Setting<>("foo.bar", secondaryDefault, Function.identity(), Property.NodeScope);
        assertEquals("some_default", secondaryDefaultViaSettings.get(Settings.EMPTY));
        assertEquals("42", secondaryDefaultViaSettings.get(Settings.builder().put("old.foo.bar", 42).build()));

        // It gets more complicated when there are two settings objects....
        Settings hasFallback = Settings.builder().put("foo.bar", "o").build();
        Setting<String> fallsback = new Setting<>("foo.baz", secondaryDefault, Function.identity(), Property.NodeScope);
        assertEquals("o", fallsback.get(hasFallback));
        assertEquals("some_default", fallsback.get(Settings.EMPTY));
        assertEquals("some_default", fallsback.get(Settings.EMPTY, Settings.EMPTY));
        assertEquals("o", fallsback.get(Settings.EMPTY, hasFallback));
        assertEquals("o", fallsback.get(hasFallback, Settings.EMPTY));
        assertEquals("a", fallsback.get(Settings.builder().put("foo.bar", "a").build(), Settings.builder().put("foo.bar", "b").build()));
    }

    public void testComplexType() {
        AtomicReference<ComplexType> ref = new AtomicReference<>(null);
        Setting<ComplexType> setting = new Setting<>("foo.bar", (s) -> "", (s) -> new ComplexType(s), Property.Dynamic, Property.NodeScope);
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
            .put("foo.bar.3.value", "3")
            .build();
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

        ClusterSettings.SettingUpdater<Settings> predicateSettingUpdater = setting.newUpdater(ref::set, logger, (s) -> {
            throw randomBoolean() ? new RuntimeException("anything") : new IllegalArgumentException("illegal");
        });
        try {
            predicateSettingUpdater.apply(
                Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build(),
                Settings.EMPTY
            );
            fail("not accepted");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "illegal value can't update [foo.bar.] from [{}] to [{\"1.value\":\"1\",\"2.value\":\"2\"}]");
        }
    }

    public void testGroupKeyExists() {
        Setting<Settings> setting = Setting.groupSetting("foo.deprecated.", Property.NodeScope);

        assertFalse(setting.exists(Settings.EMPTY));
        assertTrue(setting.exists(Settings.builder().put("foo.deprecated.1.value", "1").build()));
    }

    public void testFilteredGroups() {
        AtomicReference<Settings> ref = new AtomicReference<>(null);
        Setting<Settings> setting = Setting.groupSetting("foo.bar.", Property.Filtered, Property.Dynamic);

        ClusterSettings.SettingUpdater<Settings> predicateSettingUpdater = setting.newUpdater(ref::set, logger, (s) -> {
            throw randomBoolean() ? new RuntimeException("anything") : new IllegalArgumentException("illegal");
        });
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> predicateSettingUpdater.apply(
                Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build(),
                Settings.EMPTY
            )
        );
        assertEquals("illegal value can't update [foo.bar.]", ex.getMessage());
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

        public void validate(Integer a, Integer b) {
            if (Integer.signum(a) != Integer.signum(b)) {
                throw new IllegalArgumentException("boom");
            }
        }
    }

    public void testComposite() {
        Composite c = new Composite();
        Setting<Integer> a = Setting.intSetting("foo.int.bar.a", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> b = Setting.intSetting("foo.int.bar.b", 1, Property.Dynamic, Property.NodeScope);
        ClusterSettings.SettingUpdater<Tuple<Integer, Integer>> settingUpdater = Setting.compoundUpdater(c::set, c::validate, a, b, logger);
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

    public void testCompositeValidator() {
        Composite c = new Composite();
        Setting<Integer> a = Setting.intSetting("foo.int.bar.a", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> b = Setting.intSetting("foo.int.bar.b", 1, Property.Dynamic, Property.NodeScope);
        ClusterSettings.SettingUpdater<Tuple<Integer, Integer>> settingUpdater = Setting.compoundUpdater(c::set, c::validate, a, b, logger);
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

        Settings invalid = Settings.builder().put("foo.int.bar.a", -2).put("foo.int.bar.b", 5).build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> settingUpdater.apply(invalid, previous));
        assertThat(exc.getMessage(), equalTo("boom"));

        // reset to default
        assertTrue(settingUpdater.apply(Settings.EMPTY, build));
        assertEquals(1, c.a.intValue());
        assertEquals(1, c.b.intValue());

    }

    public void testListKeyExists() {
        final Setting<List<String>> listSetting = Setting.listSetting(
            "foo",
            Collections.singletonList("bar"),
            Function.identity(),
            Property.NodeScope
        );
        Settings settings = Settings.builder().put("foo", "bar1,bar2").build();
        assertFalse(listSetting.exists(Settings.EMPTY));
        assertTrue(listSetting.exists(settings));

        settings = Settings.builder().put("foo.0", "foo1").put("foo.1", "foo2").build();
        assertFalse(listSetting.exists(Settings.EMPTY));
        assertTrue(listSetting.exists(settings));
    }

    public void testListSettingsDeprecated() {
        final Setting<List<String>> deprecatedListSetting = Setting.listSetting(
            "foo.deprecated",
            Collections.singletonList("foo.deprecated"),
            Function.identity(),
            Property.DeprecatedWarning,
            Property.NodeScope
        );
        final Setting<List<String>> nonDeprecatedListSetting = Setting.listSetting(
            "foo.non_deprecated",
            Collections.singletonList("foo.non_deprecated"),
            Function.identity(),
            Property.NodeScope
        );
        Settings settings = Settings.builder()
            .put("foo.deprecated", "foo.deprecated1,foo.deprecated2")
            .put("foo.non_deprecated", "foo.non_deprecated1,foo.non_deprecated2")
            .build();
        deprecatedListSetting.get(settings);
        nonDeprecatedListSetting.get(settings);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedListSetting });

        settings = Settings.builder()
            .put("foo.deprecated.0", "foo.deprecated1")
            .put("foo.deprecated.1", "foo.deprecated2")
            .put("foo.non_deprecated.0", "foo.non_deprecated1")
            .put("foo.non_deprecated.1", "foo.non_deprecated2")
            .build();
        deprecatedListSetting.get(settings);
        nonDeprecatedListSetting.get(settings);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedListSetting });
    }

    public void testListSettings() {
        Setting<List<String>> listSetting = Setting.listSetting(
            "foo.bar",
            Arrays.asList("foo,bar"),
            (s) -> s.toString(),
            Property.Dynamic,
            Property.NodeScope
        );
        List<String> value = listSetting.get(Settings.EMPTY);
        assertFalse(listSetting.exists(Settings.EMPTY));
        assertEquals(1, value.size());
        assertEquals("foo,bar", value.get(0));

        List<String> input = Arrays.asList("test", "test1, test2", "test", ",,,,");
        Settings.Builder builder = Settings.builder().putList("foo.bar", input.toArray(new String[0]));
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

        settingUpdater.apply(Settings.builder().putList("foo.bar", "123").build(), builder.build());
        assertEquals(1, ref.get().size());
        assertArrayEquals(ref.get().toArray(new String[0]), new String[] { "123" });

        settingUpdater.apply(Settings.builder().put("foo.bar", "1,2,3").build(), Settings.builder().putList("foo.bar", "123").build());
        assertEquals(3, ref.get().size());
        assertArrayEquals(ref.get().toArray(new String[0]), new String[] { "1", "2", "3" });

        settingUpdater.apply(Settings.EMPTY, Settings.builder().put("foo.bar", "1,2,3").build());
        assertEquals(1, ref.get().size());
        assertEquals("foo,bar", ref.get().get(0));

        Setting<List<Integer>> otherSettings = Setting.listSetting(
            "foo.bar",
            Collections.emptyList(),
            Integer::parseInt,
            Property.Dynamic,
            Property.NodeScope
        );
        List<Integer> defaultValue = otherSettings.get(Settings.EMPTY);
        assertEquals(0, defaultValue.size());
        List<Integer> intValues = otherSettings.get(Settings.builder().put("foo.bar", "0,1,2,3").build());
        assertEquals(4, intValues.size());
        for (int i = 0; i < intValues.size(); i++) {
            assertEquals(i, intValues.get(i).intValue());
        }

        Setting<List<String>> settingWithFallback = Setting.listSetting(
            "foo.baz",
            listSetting,
            Function.identity(),
            Property.Dynamic,
            Property.NodeScope
        );
        value = settingWithFallback.get(Settings.EMPTY);
        assertEquals(1, value.size());
        assertEquals("foo,bar", value.get(0));

        value = settingWithFallback.get(Settings.builder().putList("foo.bar", "1", "2").build());
        assertEquals(2, value.size());
        assertEquals("1", value.get(0));
        assertEquals("2", value.get(1));

        value = settingWithFallback.get(Settings.builder().putList("foo.baz", "3", "4").build());
        assertEquals(2, value.size());
        assertEquals("3", value.get(0));
        assertEquals("4", value.get(1));

        value = settingWithFallback.get(Settings.builder().putList("foo.baz", "3", "4").putList("foo.bar", "1", "2").build());
        assertEquals(2, value.size());
        assertEquals("3", value.get(0));
        assertEquals("4", value.get(1));
    }

    public void testListSettingAcceptsNumberSyntax() {
        Setting<List<String>> listSetting = Setting.listSetting(
            "foo.bar",
            Arrays.asList("foo,bar"),
            (s) -> s.toString(),
            Property.Dynamic,
            Property.NodeScope
        );
        List<String> input = Arrays.asList("test", "test1, test2", "test", ",,,,");
        Settings.Builder builder = Settings.builder().putList("foo.bar", input.toArray(new String[0]));
        // try to parse this really annoying format
        for (String key : builder.keys()) {
            assertTrue("key: " + key + " doesn't match", listSetting.match(key));
        }
        builder = Settings.builder().put("foo.bar", "1,2,3");
        for (String key : builder.keys()) {
            assertTrue("key: " + key + " doesn't match", listSetting.match(key));
        }
        assertFalse(listSetting.match("foo_bar"));
        assertFalse(listSetting.match("foo_bar.1"));
        assertTrue(listSetting.match("foo.bar"));
        assertTrue(listSetting.match("foo.bar." + randomIntBetween(0, 10000)));
    }

    public void testDynamicKeySetting() {
        Setting<Boolean> setting = Setting.prefixKeySetting("foo.", (key) -> Setting.boolSetting(key, false, Property.NodeScope));
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

    public void testPrefixKeySettingFallbackAsMap() {
        Setting.AffixSetting<Boolean> setting = Setting.prefixKeySetting(
            "foo.",
            "bar.",
            (ns, key) -> Setting.boolSetting(key, false, Property.NodeScope)
        );

        assertTrue(setting.match("foo.bar"));
        assertTrue(setting.match("bar.bar"));

        Map<String, Boolean> map = setting.getAsMap(Settings.builder().put("foo.bar", "true").build());
        assertEquals(1, map.size());
        assertTrue(map.get("bar"));

        map = setting.getAsMap(Settings.builder().put("bar.bar", "true").build());
        assertEquals(1, map.size());
        assertTrue(map.get("bar"));

        // Prefer primary
        map = setting.getAsMap(Settings.builder().put("foo.bar", "false").put("bar.bar", "true").build());
        assertEquals(1, map.size());
        assertFalse(map.get("bar"));
    }

    public void testAffixKeySetting() {
        Setting<Boolean> setting = Setting.affixKeySetting("foo.", "enable", (key) -> Setting.boolSetting(key, false, Property.NodeScope));
        assertTrue(setting.hasComplexMatcher());
        assertTrue(setting.match("foo.bar.enable"));
        assertTrue(setting.match("foo.baz.enable"));
        assertFalse(setting.match("foo.bar.baz.enable"));
        assertFalse(setting.match("foo.bar"));
        assertFalse(setting.match("foo.bar.baz.enabled"));
        assertFalse(setting.match("foo"));
        Setting<Boolean> concreteSetting = setting.getConcreteSetting("foo.bar.enable");
        assertTrue(concreteSetting.get(Settings.builder().put("foo.bar.enable", "true").build()));
        assertFalse(concreteSetting.get(Settings.builder().put("foo.baz.enable", "true").build()));

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> setting.getConcreteSetting("foo"));
        assertEquals("key [foo] must match [foo.*.enable] but didn't.", exc.getMessage());

        exc = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.affixKeySetting("foo", "enable", (key) -> Setting.boolSetting(key, false, Property.NodeScope))
        );
        assertEquals("prefix must end with a '.'", exc.getMessage());

        exc = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.prefixKeySetting("foo.", "bar", (ns, key) -> Setting.boolSetting(key, false, Property.NodeScope))
        );
        assertEquals("prefix must end with a '.'", exc.getMessage());

        Setting<List<String>> listAffixSetting = Setting.affixKeySetting(
            "foo.",
            "bar",
            (key) -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), Property.NodeScope)
        );

        assertTrue(listAffixSetting.hasComplexMatcher());
        assertTrue(listAffixSetting.match("foo.test.bar"));
        assertTrue(listAffixSetting.match("foo.test_1.bar"));
        assertFalse(listAffixSetting.match("foo.buzz.baz.bar"));
        assertFalse(listAffixSetting.match("foo.bar"));
        assertFalse(listAffixSetting.match("foo.baz"));
        assertFalse(listAffixSetting.match("foo"));
    }

    public void testAffixKeySettingWithSecure() {
        Setting.AffixSetting<SecureString> secureSetting = Setting.affixKeySetting(
            "foo.",
            "secret",
            (key) -> SecureSetting.secureString(key, null)
        );

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo.a.secret", "secret1");
        secureSettings.setString("foo.b.secret", "secret2");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();

        assertThat(secureSetting.exists(settings), is(true));

        Map<String, SecureString> secrets = secureSetting.getAsMap(settings);
        assertThat(secrets.keySet(), contains("a", "b"));

        Setting<SecureString> secureA = secureSetting.getConcreteSetting("foo.a.secret");
        assertThat(secureA.get(settings), is("secret1"));
        assertThat(secrets.get("a"), is("secret1"));
    }

    public void testAffixKeyExists() {
        Setting<Boolean> setting = Setting.affixKeySetting("foo.", "enable", (key) -> Setting.boolSetting(key, false, Property.NodeScope));
        assertFalse(setting.exists(Settings.EMPTY));
        assertTrue(setting.exists(Settings.builder().put("foo.test.enable", "true").build()));
    }

    public void testAffixSettingNamespaces() {
        Setting.AffixSetting<Boolean> setting = Setting.affixKeySetting(
            "foo.",
            "enable",
            (key) -> Setting.boolSetting(key, false, Property.NodeScope)
        );
        Settings build = Settings.builder()
            .put("foo.bar.enable", "true")
            .put("foo.baz.enable", "true")
            .put("foo.boom.enable", "true")
            .put("something.else", "true")
            .build();
        Set<String> namespaces = setting.getNamespaces(build);
        assertEquals(3, namespaces.size());
        assertTrue(namespaces.contains("bar"));
        assertTrue(namespaces.contains("baz"));
        assertTrue(namespaces.contains("boom"));
    }

    public void testAffixAsMap() {
        Setting.AffixSetting<String> setting = Setting.prefixKeySetting("foo.bar.", key -> Setting.simpleString(key, Property.NodeScope));
        Settings build = Settings.builder().put("foo.bar.baz", 2).put("foo.bar.foobar", 3).build();
        Map<String, String> asMap = setting.getAsMap(build);
        assertEquals(2, asMap.size());
        assertEquals("2", asMap.get("baz"));
        assertEquals("3", asMap.get("foobar"));

        setting = Setting.prefixKeySetting("foo.bar.", key -> Setting.simpleString(key, Property.NodeScope));
        build = Settings.builder().put("foo.bar.baz", 2).put("foo.bar.foobar", 3).put("foo.bar.baz.deep", 45).build();
        asMap = setting.getAsMap(build);
        assertEquals(3, asMap.size());
        assertEquals("2", asMap.get("baz"));
        assertEquals("3", asMap.get("foobar"));
        assertEquals("45", asMap.get("baz.deep"));
    }

    public void testGetAllConcreteSettings() {
        Setting.AffixSetting<List<String>> listAffixSetting = Setting.affixKeySetting(
            "foo.",
            "bar",
            (key) -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), Property.NodeScope)
        );

        Settings settings = Settings.builder()
            .putList("foo.1.bar", "1", "2")
            .putList("foo.2.bar", "3", "4", "5")
            .putList("foo.bar", "6")
            .putList("some.other", "6")
            .putList("foo.3.bar", "6")
            .build();
        Stream<Setting<List<String>>> allConcreteSettings = listAffixSetting.getAllConcreteSettings(settings);
        Map<String, List<String>> collect = allConcreteSettings.collect(Collectors.toMap(Setting::getKey, (s) -> s.get(settings)));
        assertEquals(3, collect.size());
        assertEquals(Arrays.asList("1", "2"), collect.get("foo.1.bar"));
        assertEquals(Arrays.asList("3", "4", "5"), collect.get("foo.2.bar"));
        assertEquals(Arrays.asList("6"), collect.get("foo.3.bar"));
    }

    public void testAffixSettingsFailOnGet() {
        Setting.AffixSetting<List<String>> listAffixSetting = Setting.affixKeySetting(
            "foo.",
            "bar",
            (key) -> Setting.listSetting(key, Collections.singletonList("testelement"), Function.identity(), Property.NodeScope)
        );
        expectThrows(UnsupportedOperationException.class, () -> listAffixSetting.get(Settings.EMPTY));
        assertEquals(Collections.singletonList("testelement"), listAffixSetting.getDefault(Settings.EMPTY));
        assertEquals("[\"testelement\"]", listAffixSetting.getDefaultRaw(Settings.EMPTY));
    }

    public void testAffixSettingsValidatorDependencies() {
        Setting<Integer> affix = Setting.affixKeySetting("abc.", "def", k -> Setting.intSetting(k, 10));
        Setting<Integer> fix0 = Setting.intSetting("abc.tuv", 20, 0);
        Setting<Integer> fix1 = Setting.intSetting("abc.qrx", 20, 0, new Setting.Validator<Integer>() {
            @Override
            public void validate(Integer value) {}

            String toString(Map<Setting<?>, Object> s) {
                return s.entrySet()
                    .stream()
                    .map(e -> e.getKey().getKey() + ":" + e.getValue().toString())
                    .sorted()
                    .collect(Collectors.joining(","));
            }

            @Override
            public void validate(Integer value, Map<Setting<?>, Object> settings, boolean isPresent) {
                if (settings.get(fix0).equals(fix0.getDefault(Settings.EMPTY))) {
                    settings.remove(fix0);
                }
                if (settings.size() == 1) {
                    throw new IllegalArgumentException(toString(settings));
                } else if (settings.size() == 2) {
                    throw new IllegalArgumentException(toString(settings));
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> a = List.of(affix, fix0);
                return a.iterator();
            }
        });

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> fix1.get(Settings.builder().put("abc.1.def", 11).put("abc.2.def", 12).put("abc.qrx", 11).build())
        );
        assertThat(e.getMessage(), is("abc.1.def:11,abc.2.def:12"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> fix1.get(Settings.builder().put("abc.3.def", 13).put("abc.qrx", 20).build())
        );
        assertThat(e.getMessage(), is("abc.3.def:13"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> fix1.get(Settings.builder().put("abc.4.def", 14).put("abc.qrx", 20).put("abc.tuv", 50).build())
        );
        assertThat(e.getMessage(), is("abc.4.def:14,abc.tuv:50"));

        assertEquals(
            fix1.get(Settings.builder().put("abc.3.def", 13).put("abc.1.def", 11).put("abc.2.def", 12).put("abc.qrx", 20).build()),
            Integer.valueOf(20)
        );

        assertEquals(fix1.get(Settings.builder().put("abc.qrx", 30).build()), Integer.valueOf(30));
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
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", (Property[]) null)
        );
        assertThat(ex.getMessage(), containsString("properties cannot be null for setting"));
    }

    public void testRejectConflictingDynamicAndFinalProperties() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", Property.Final, randomFrom(Property.Dynamic, Property.OperatorDynamic))
        );
        assertThat(ex.getMessage(), containsString("final setting [foo.bar] cannot be dynamic"));
    }

    public void testRejectConflictingDynamicAndOperatorDynamicProperties() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", Property.Dynamic, Property.OperatorDynamic)
        );
        assertThat(ex.getMessage(), containsString("setting [foo.bar] cannot be both dynamic and operator dynamic"));
    }

    public void testRejectNonIndexScopedNotCopyableOnResizeSetting() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", Property.NotCopyableOnResize)
        );
        assertThat(e, hasToString(containsString("non-index-scoped setting [foo.bar] can not have property [NotCopyableOnResize]")));
    }

    public void testRejectNonIndexScopedInternalIndexSetting() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", Property.InternalIndex)
        );
        assertThat(e, hasToString(containsString("non-index-scoped setting [foo.bar] can not have property [InternalIndex]")));
    }

    public void testRejectNonIndexScopedPrivateIndexSetting() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", Property.PrivateIndex)
        );
        assertThat(e, hasToString(containsString("non-index-scoped setting [foo.bar] can not have property [PrivateIndex]")));
    }

    public void testTimeValue() {
        final TimeValue random = randomTimeValue();

        Setting<TimeValue> setting = Setting.timeSetting("foo", random);
        assertThat(setting.get(Settings.EMPTY), equalTo(random));

        final int factor = randomIntBetween(1, 10);
        setting = Setting.timeSetting("foo", (s) -> TimeValue.timeValueMillis(random.getMillis() * factor), TimeValue.ZERO);
        assertThat(setting.get(Settings.builder().put("foo", "12h").build()), equalTo(TimeValue.timeValueHours(12)));
        assertThat(setting.get(Settings.EMPTY).getMillis(), equalTo(random.getMillis() * factor));
    }

    public void testTimeValueBounds() {
        Setting<TimeValue> settingWithLowerBound = Setting.timeSetting(
            "foo",
            TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(5)
        );
        assertThat(settingWithLowerBound.get(Settings.EMPTY), equalTo(TimeValue.timeValueSeconds(10)));

        assertThat(settingWithLowerBound.get(Settings.builder().put("foo", "5000ms").build()), equalTo(TimeValue.timeValueSeconds(5)));
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> settingWithLowerBound.get(Settings.builder().put("foo", "4999ms").build())
        );

        assertThat(illegalArgumentException.getMessage(), equalTo("failed to parse value [4999ms] for setting [foo], must be >= [5s]"));

        Setting<TimeValue> settingWithBothBounds = Setting.timeSetting(
            "bar",
            TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(5),
            TimeValue.timeValueSeconds(20)
        );
        assertThat(settingWithBothBounds.get(Settings.EMPTY), equalTo(TimeValue.timeValueSeconds(10)));

        assertThat(settingWithBothBounds.get(Settings.builder().put("bar", "5000ms").build()), equalTo(TimeValue.timeValueSeconds(5)));
        assertThat(settingWithBothBounds.get(Settings.builder().put("bar", "20000ms").build()), equalTo(TimeValue.timeValueSeconds(20)));
        illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> settingWithBothBounds.get(Settings.builder().put("bar", "4999ms").build())
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("failed to parse value [4999ms] for setting [bar], must be >= [5s]"));

        illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> settingWithBothBounds.get(Settings.builder().put("bar", "20001ms").build())
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("failed to parse value [20001ms] for setting [bar], must be <= [20s]"));
    }

    public void testSettingsGroupUpdater() {
        Setting<Integer> intSetting = Setting.intSetting("prefix.foo", 1, Property.NodeScope, Property.Dynamic);
        Setting<Integer> intSetting2 = Setting.intSetting("prefix.same", 1, Property.NodeScope, Property.Dynamic);
        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(
            s -> {},
            Arrays.asList(intSetting, intSetting2)
        );

        Settings current = Settings.builder().put("prefix.foo", 123).put("prefix.same", 5555).build();
        Settings previous = Settings.builder().put("prefix.foo", 321).put("prefix.same", 5555).build();
        assertTrue(updater.apply(current, previous));
    }

    public void testSettingsGroupUpdaterRemoval() {
        Setting<Integer> intSetting = Setting.intSetting("prefix.foo", 1, Property.NodeScope, Property.Dynamic);
        Setting<Integer> intSetting2 = Setting.intSetting("prefix.same", 1, Property.NodeScope, Property.Dynamic);
        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(
            s -> {},
            Arrays.asList(intSetting, intSetting2)
        );

        Settings current = Settings.builder().put("prefix.same", 5555).build();
        Settings previous = Settings.builder().put("prefix.foo", 321).put("prefix.same", 5555).build();
        assertTrue(updater.apply(current, previous));
    }

    public void testSettingsGroupUpdaterWithAffixSetting() {
        Setting<Integer> intSetting = Setting.intSetting("prefix.foo", 1, Property.NodeScope, Property.Dynamic);
        Setting.AffixSetting<String> prefixKeySetting = Setting.prefixKeySetting(
            "prefix.foo.bar.",
            key -> Setting.simpleString(key, Property.NodeScope, Property.Dynamic)
        );
        Setting.AffixSetting<String> affixSetting = Setting.affixKeySetting(
            "prefix.foo.",
            "suffix",
            key -> Setting.simpleString(key, Property.NodeScope, Property.Dynamic)
        );

        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(
            s -> {},
            Arrays.asList(intSetting, prefixKeySetting, affixSetting)
        );

        Settings.Builder currentSettingsBuilder = Settings.builder().put("prefix.foo.bar.baz", "foo").put("prefix.foo.infix.suffix", "foo");
        Settings.Builder previousSettingsBuilder = Settings.builder()
            .put("prefix.foo.bar.baz", "foo")
            .put("prefix.foo.infix.suffix", "foo");
        boolean removePrefixKeySetting = randomBoolean();
        boolean changePrefixKeySetting = randomBoolean();
        boolean removeAffixKeySetting = randomBoolean();
        boolean changeAffixKeySetting = randomBoolean();
        boolean removeAffixNamespace = randomBoolean();

        if (removePrefixKeySetting) {
            previousSettingsBuilder.remove("prefix.foo.bar.baz");
        }
        if (changePrefixKeySetting) {
            currentSettingsBuilder.put("prefix.foo.bar.baz", "bar");
        }
        if (removeAffixKeySetting) {
            previousSettingsBuilder.remove("prefix.foo.infix.suffix");
        }
        if (changeAffixKeySetting) {
            currentSettingsBuilder.put("prefix.foo.infix.suffix", "bar");
        }
        if (removeAffixKeySetting == false && changeAffixKeySetting == false && removeAffixNamespace) {
            currentSettingsBuilder.remove("prefix.foo.infix.suffix");
            currentSettingsBuilder.put("prefix.foo.infix2.suffix", "bar");
            previousSettingsBuilder.put("prefix.foo.infix2.suffix", "bar");
        }

        boolean expectedChange = removeAffixKeySetting
            || removePrefixKeySetting
            || changeAffixKeySetting
            || changePrefixKeySetting
            || removeAffixNamespace;
        assertThat(updater.apply(currentSettingsBuilder.build(), previousSettingsBuilder.build()), is(expectedChange));
    }

    public void testAffixNamespacesWithGroupSetting() {
        final Setting.AffixSetting<Settings> affixSetting = Setting.affixKeySetting(
            "prefix.",
            "suffix",
            (key) -> Setting.groupSetting(key + ".", Setting.Property.Dynamic, Setting.Property.NodeScope)
        );

        assertThat(affixSetting.getNamespaces(Settings.builder().put("prefix.infix.suffix", "anything").build()), hasSize(1));
        assertThat(affixSetting.getNamespaces(Settings.builder().put("prefix.infix.suffix.anything", "anything").build()), hasSize(1));
    }

    public void testGroupSettingUpdaterValidator() {
        final Setting.AffixSetting<Integer> affixSetting = Setting.affixKeySetting(
            "prefix.",
            "suffix",
            (key) -> Setting.intSetting(key, 5, Setting.Property.Dynamic, Setting.Property.NodeScope)
        );
        Setting<Integer> fixSetting = Setting.intSetting("abc", 1, Property.NodeScope);

        Consumer<Settings> validator = s -> {
            if (affixSetting.getNamespaces(s).contains("foo")) {
                if (fixSetting.get(s) == 2) {
                    throw new IllegalArgumentException("foo and 2 can't go together");
                }
            } else if (affixSetting.getNamespaces(s).contains("bar")) {
                throw new IllegalArgumentException("no bar");
            }
        };

        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(
            s -> {},
            Arrays.asList(affixSetting, fixSetting),
            validator
        );

        IllegalArgumentException illegal = expectThrows(IllegalArgumentException.class, () -> {
            updater.getValue(Settings.builder().put("prefix.foo.suffix", 5).put("abc", 2).build(), Settings.EMPTY);
        });
        assertEquals("foo and 2 can't go together", illegal.getMessage());

        illegal = expectThrows(IllegalArgumentException.class, () -> {
            updater.getValue(Settings.builder().put("prefix.bar.suffix", 6).put("abc", 3).build(), Settings.EMPTY);
        });
        assertEquals("no bar", illegal.getMessage());

        Settings s = updater.getValue(
            Settings.builder().put("prefix.foo.suffix", 5).put("prefix.bar.suffix", 5).put("abc", 3).build(),
            Settings.EMPTY
        );
        assertNotNull(s);
    }

    public void testExists() {
        final Setting<?> fooSetting = Setting.simpleString("foo", Property.NodeScope);
        assertFalse(fooSetting.exists(Settings.EMPTY));
        assertTrue(fooSetting.exists(Settings.builder().put("foo", "bar").build()));
    }

    public void testExistsWithSecure() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "foo");
        Setting<String> fooSetting = Setting.simpleString("foo", Property.NodeScope);
        assertFalse(fooSetting.exists(Settings.builder().setSecureSettings(secureSettings).build()));
    }

    public void testExistsWithFallback() {
        final int count = randomIntBetween(1, 16);
        Setting<String> current = Setting.simpleString("fallback0", Property.NodeScope);
        for (int i = 1; i < count; i++) {
            final Setting<String> next = new Setting<>(
                new Setting.SimpleKey("fallback" + i),
                current,
                Function.identity(),
                Property.NodeScope
            );
            current = next;
        }
        final Setting<String> fooSetting = new Setting<>(new Setting.SimpleKey("foo"), current, Function.identity(), Property.NodeScope);
        assertFalse(fooSetting.exists(Settings.EMPTY));
        if (randomBoolean()) {
            assertTrue(fooSetting.exists(Settings.builder().put("foo", "bar").build()));
        } else {
            final String setting = "fallback" + randomIntBetween(0, count - 1);
            assertFalse(fooSetting.exists(Settings.builder().put(setting, "bar").build()));
            assertTrue(fooSetting.existsOrFallbackExists(Settings.builder().put(setting, "bar").build()));
        }
    }

    public void testAffixMapUpdateWithNullSettingValue() {
        // GIVEN an affix setting changed from "prefix._foo"="bar" to "prefix._foo"=null
        final Settings current = Settings.builder().put("prefix._foo", (String) null).build();

        final Settings previous = Settings.builder().put("prefix._foo", "bar").build();

        final Setting.AffixSetting<String> affixSetting = Setting.prefixKeySetting(
            "prefix" + ".",
            key -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope)
        );

        final Consumer<Map<String, String>> consumer = (map) -> {};
        final BiConsumer<String, String> validator = (s1, s2) -> {};

        // WHEN creating an affix updater
        final SettingUpdater<Map<String, String>> updater = affixSetting.newAffixMapUpdater(consumer, logger, validator);

        // THEN affix updater is always expected to have changed (even when defaults are omitted)
        assertTrue(updater.hasChanged(current, previous));

        // THEN changes are expected when defaults aren't omitted
        final Map<String, String> updatedSettings = updater.getValue(current, previous);
        assertNotNull(updatedSettings);
        assertEquals(1, updatedSettings.size());

        // THEN changes are reported when defaults aren't omitted
        final String key = updatedSettings.keySet().iterator().next();
        final String value = updatedSettings.get(key);
        assertEquals("_foo", key);
        assertEquals("", value);
    }

    public void testNonSecureSettingInKeystore() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "bar");
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        Setting<String> setting = Setting.simpleString("foo", Property.NodeScope);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertThat(e.getMessage(), containsString("must be stored inside elasticsearch.yml"));
    }

    @TestLogging(
        value = "org.elasticsearch.common.settings.IndexScopedSettings:DEBUG",
        reason = "to ensure we log INFO-level messages from IndexScopedSettings"
    )
    public void testLogSettingUpdate() throws Exception {
        final IndexMetadata metadata = newIndexMeta(
            "index1",
            Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "20s").build()
        );
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);

        try (var mockLog = MockLog.capture(IndexScopedSettings.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "message",
                    "org.elasticsearch.common.settings.IndexScopedSettings",
                    Level.DEBUG,
                    "updating [index.refresh_interval] from [20s] to [10s]"
                ) {
                    @Override
                    public boolean innerMatch(LogEvent event) {
                        return event.getMarker().getName().equals(" [index1]");
                    }
                }
            );
            settings.updateIndexMetadata(
                newIndexMeta("index1", Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "10s").build())
            );

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testDynamicTest() {
        final Property property = randomFrom(Property.Dynamic, Property.OperatorDynamic);
        final Setting<String> setting = Setting.simpleString("foo.bar", property);
        assertTrue(setting.isDynamic());
        assertEquals(setting.isOperatorOnly(), property == Property.OperatorDynamic);
    }

    public void testCheckForDeprecation() {
        final String criticalSettingName = "foo.bar";
        final String warningSettingName = "foo.foo";
        final String settingValue = "blat";
        final Setting<String> undeprecatedSetting1 = Setting.simpleString(criticalSettingName, settingValue);
        final Setting<String> undeprecatedSetting2 = Setting.simpleString(warningSettingName, settingValue);
        final Settings settings = Settings.builder().put(criticalSettingName, settingValue).put(warningSettingName, settingValue).build();
        undeprecatedSetting1.checkDeprecation(settings);
        undeprecatedSetting2.checkDeprecation(settings);
        ensureNoWarnings();
        final Setting<String> criticalDeprecatedSetting = Setting.simpleString(
            criticalSettingName,
            settingValue,
            Property.DeprecatedWarning
        );
        criticalDeprecatedSetting.checkDeprecation(settings);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { criticalDeprecatedSetting });
        final Setting<String> deprecatedSettingWarningOnly = Setting.simpleString(
            warningSettingName,
            settingValue,
            Property.DeprecatedWarning
        );
        deprecatedSettingWarningOnly.checkDeprecation(settings);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSettingWarningOnly });
    }

    public void testCheckForDeprecationWithSkipSetting() {
        final String settingName = "foo.bar.hide.this";
        final String settingValue = "blat";
        final Setting<String> setting = Setting.simpleString(settingName, settingValue);
        final Settings settings = Settings.builder().put(settingName, settingValue).build();
        setting.checkDeprecation(settings);
        ensureNoWarnings();
        final Setting<String> deprecatedSetting = Setting.simpleString(settingName, settingValue, Property.DeprecatedWarning);
        deprecatedSetting.checkDeprecation(settings);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });
        final Settings settingsWithSkipDeprecationSetting = Settings.builder()
            .put(settingName, settingValue)
            .putList("deprecation.skip_deprecated_settings", settingName)
            .build();
        DeprecationLogger.initialize(settingsWithSkipDeprecationSetting);
        deprecatedSetting.checkDeprecation(settingsWithSkipDeprecationSetting);
        ensureNoWarnings();
    }

    public void testDeprecationPropertyValidation() {
        expectThrows(
            IllegalArgumentException.class,
            () -> Setting.boolSetting("a.bool.setting", true, Property.Deprecated, Property.DeprecatedWarning)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> Setting.boolSetting("a.bool.setting", true, Property.Deprecated, Property.IndexSettingDeprecatedInV7AndRemovedInV8)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> Setting.boolSetting("a.bool.setting", true, Property.DeprecatedWarning, Property.IndexSettingDeprecatedInV7AndRemovedInV8)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> Setting.boolSetting("a.bool.setting", true, Property.Deprecated, Property.IndexSettingDeprecatedInV8AndRemovedInV9)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> Setting.boolSetting("a.bool.setting", true, Property.DeprecatedWarning, Property.IndexSettingDeprecatedInV8AndRemovedInV9)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> Setting.boolSetting("a.bool.setting", true, Property.Deprecated, Property.IndexSettingDeprecatedInV9AndRemovedInV10)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> Setting.boolSetting(
                "a.bool.setting",
                true,
                Property.DeprecatedWarning,
                Property.IndexSettingDeprecatedInV9AndRemovedInV10
            )
        );
    }

    public void testIntSettingBounds() {
        Setting<Integer> setting = Setting.intSetting("int.setting", 0, Integer.MIN_VALUE, Integer.MAX_VALUE);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> setting.get(Settings.builder().put("int.setting", "2147483648").build())
        );
        assertThat(e.getMessage(), equalTo("Failed to parse value [2147483648] for setting [int.setting] must be <= 2147483647"));
        var e2 = expectThrows(
            IllegalArgumentException.class,
            () -> setting.get(Settings.builder().put("int.setting", "-2147483649").build())
        );
        assertThat(e2.getMessage(), equalTo("Failed to parse value [-2147483649] for setting [int.setting] must be >= -2147483648"));
    }

    public void testLongSettingBounds() {
        Setting<Long> setting = Setting.longSetting("long.setting", 0, Long.MIN_VALUE);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> setting.get(Settings.builder().put("long.setting", "9223372036854775808").build())
        );
        assertThat(
            e.getMessage(),
            equalTo("Failed to parse value [9223372036854775808] for setting [long.setting] must be <= 9223372036854775807")
        );
        var e2 = expectThrows(
            IllegalArgumentException.class,
            () -> setting.get(Settings.builder().put("long.setting", "-9223372036854775809").build())
        );
        assertThat(
            e2.getMessage(),
            equalTo("Failed to parse value [-9223372036854775809] for setting [long.setting] must be >= -9223372036854775808")
        );
    }
}
