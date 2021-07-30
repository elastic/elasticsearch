/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Setting.Property;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;

public class SettingsModuleTests extends ModuleTestCase {

    public void testValidate() {
        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "2.0").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "[2.0]").build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () ->  new SettingsModule(settings));
            assertEquals("Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]", ex.getMessage());
        }

        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "[2.0]")
                .put("some.foo.bar", 1).build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> new SettingsModule(settings));
            assertEquals("Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]", ex.getMessage());
            assertEquals(1, ex.getSuppressed().length);
            assertEquals("unknown setting [some.foo.bar] please check that any required plugins are installed, or check the breaking " +
                "changes documentation for removed settings", ex.getSuppressed()[0].getMessage());
        }

        {
            Settings settings = Settings.builder().put("index.codec", "default")
                .put("index.foo.bar", 1).build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> new SettingsModule(settings));
            assertEquals("node settings must not contain any index level settings", ex.getMessage());
        }

        {
            Settings settings = Settings.builder().put("index.codec", "default").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
    }

    public void testRegisterSettings() {
        {
            Settings settings = Settings.builder().put("some.custom.setting", "2.0").build();
            SettingsModule module = new SettingsModule(settings, Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope));
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("some.custom.setting", "false").build();
            try {
                new SettingsModule(settings, Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope));
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("Failed to parse value [false] for setting [some.custom.setting]", ex.getMessage());
            }
        }
    }

    public void testRegisterConsistentSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("some.custom.secure.consistent.setting", "secure_value");
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final Setting<?> concreteConsistentSetting = SecureSetting.secureString("some.custom.secure.consistent.setting", null,
                Setting.Property.Consistent);
        SettingsModule module = new SettingsModule(settings, concreteConsistentSetting);
        assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        assertThat(module.getConsistentSettings(), Matchers.containsInAnyOrder(concreteConsistentSetting));

        final Setting<?> concreteUnsecureConsistentSetting = Setting.simpleString("some.custom.UNSECURE.consistent.setting",
                Property.Consistent, Property.NodeScope);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new SettingsModule(Settings.builder().build(), concreteUnsecureConsistentSetting));
        assertThat(e.getMessage(), is("Invalid consistent secure setting [some.custom.UNSECURE.consistent.setting]"));

        secureSettings = new MockSecureSettings();
        secureSettings.setString("some.custom.secure.consistent.afix.wow.setting", "secure_value");
        final Settings settings2 = Settings.builder().setSecureSettings(secureSettings).build();
        final Setting<?> afixConcreteConsistentSetting = Setting.affixKeySetting(
                "some.custom.secure.consistent.afix.", "setting",
                key -> SecureSetting.secureString(key, null, Setting.Property.Consistent));
        module = new SettingsModule(settings2,afixConcreteConsistentSetting);
        assertInstanceBinding(module, Settings.class, (s) -> s == settings2);
        assertThat(module.getConsistentSettings(), Matchers.containsInAnyOrder(afixConcreteConsistentSetting));

        final Setting<?> concreteUnsecureConsistentAfixSetting = Setting.affixKeySetting(
                "some.custom.secure.consistent.afix.", "setting",
                key -> Setting.simpleString(key, Setting.Property.Consistent, Property.NodeScope));
        e = expectThrows(IllegalArgumentException.class,
                () -> new SettingsModule(Settings.builder().build(), concreteUnsecureConsistentAfixSetting));
        assertThat(e.getMessage(), is("Invalid consistent secure setting [some.custom.secure.consistent.afix.*.setting]"));
    }

    public void testLoggerSettings() {
        {
            Settings settings = Settings.builder().put("logger._root", "TRACE").put("logger.transport", "INFO").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }

        {
            Settings settings = Settings.builder().put("logger._root", "BOOM").put("logger.transport", "WOW").build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new SettingsModule(settings));
            assertEquals("Unknown level constant [BOOM].", ex.getMessage());
        }
    }

    public void testRegisterSettingsFilter() {
        Settings settings = Settings.builder().put("foo.bar", "false").put("bar.foo", false).put("bar.baz", false).build();
        try {
            new SettingsModule(settings, Arrays.asList(Setting.boolSetting("foo.bar", true, Property.NodeScope),
            Setting.boolSetting("bar.foo", true, Property.NodeScope, Property.Filtered),
            Setting.boolSetting("bar.baz", true, Property.NodeScope)), Arrays.asList("foo.*", "bar.foo"), emptySet());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("filter [bar.foo] has already been registered", ex.getMessage());
        }
        SettingsModule module = new SettingsModule(settings, Arrays.asList(Setting.boolSetting("foo.bar", true, Property.NodeScope),
            Setting.boolSetting("bar.foo", true, Property.NodeScope, Property.Filtered),
            Setting.boolSetting("bar.baz", true, Property.NodeScope)), Arrays.asList("foo.*"), emptySet());
        assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).size() == 1);
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).keySet().contains("bar.baz"));
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).get("bar.baz").equals("false"));

    }

    public void testMutuallyExclusiveScopes() {
        new SettingsModule(Settings.EMPTY, Setting.simpleString("foo.bar", Property.NodeScope));
        new SettingsModule(Settings.EMPTY, Setting.simpleString("index.foo.bar", Property.IndexScope));

        // Those should fail
        try {
            new SettingsModule(Settings.EMPTY, Setting.simpleString("foo.bar"));
            fail("No scope should fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("No scope found for setting"));
        }
        // Some settings have both scopes - that's fine too if they have per-node defaults
        try {
            new SettingsModule(Settings.EMPTY,
                Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope),
                Setting.simpleString("foo.bar", Property.NodeScope));
            fail("already registered");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot register setting [foo.bar] twice"));
        }

        try {
            new SettingsModule(Settings.EMPTY,
                Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope),
                Setting.simpleString("foo.bar", Property.IndexScope));
            fail("already registered");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot register setting [foo.bar] twice"));
        }
    }

    public void testPluginSettingWithoutNamespace() {
        final String key = randomAlphaOfLength(8);
        final Setting<String> setting = Setting.simpleString(key, Property.NodeScope);
        runSettingWithoutNamespaceTest(
            key, () -> new SettingsModule(Settings.EMPTY, List.of(setting), List.of(), Set.of(), Set.of(), Set.of()));
    }

    public void testClusterSettingWithoutNamespace() {
        final String key = randomAlphaOfLength(8);
        final Setting<String> setting = Setting.simpleString(key, Property.NodeScope);
        runSettingWithoutNamespaceTest(
            key, () -> new SettingsModule(Settings.EMPTY, List.of(), List.of(), Set.of(), Set.of(setting), Set.of()));
    }

    public void testIndexSettingWithoutNamespace() {
        final String key = randomAlphaOfLength(8);
        final Setting<String> setting = Setting.simpleString(key, Property.IndexScope);
        runSettingWithoutNamespaceTest(
            key, () -> new SettingsModule(Settings.EMPTY, List.of(), List.of(), Set.of(), Set.of(), Set.of(setting)));
    }

    private void runSettingWithoutNamespaceTest(final String key, final Supplier<SettingsModule> supplier) {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, supplier::get);
        assertThat(e, hasToString(containsString("setting [" + key + "] is not in any namespace, its name must contain a dot")));
    }

}
