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

import org.apache.logging.log4j.Level;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IllegalFormatCodePointException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;

public class ScopedSettingsTests extends ESTestCase {

    public void testResetSetting() {
        Setting<Integer> dynamicSetting = Setting.intSetting("some.dyn.setting", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> staticSetting = Setting.intSetting("some.static.setting", 1, Property.NodeScope);
        Settings currentSettings = Settings.builder().put("some.dyn.setting", 5).put("some.static.setting", 6).put("archived.foo.bar", 9)
            .build();
        ClusterSettings service = new ClusterSettings(currentSettings
            , new HashSet<>(Arrays.asList(dynamicSetting, staticSetting)));

        expectThrows(IllegalArgumentException.class, () ->
        service.updateDynamicSettings(Settings.builder().put("some.dyn.setting", 8).putNull("some.static.setting").build(),
            Settings.builder().put(currentSettings), Settings.builder(), "node"));

        Settings.Builder target = Settings.builder().put(currentSettings);
        Settings.Builder update = Settings.builder();
        assertTrue(service.updateDynamicSettings(Settings.builder().put("some.dyn.setting", 8).build(),
            target, update, "node"));
        assertEquals(8, dynamicSetting.get(target.build()).intValue());
        assertEquals(6, staticSetting.get(target.build()).intValue());
        assertEquals(9, target.build().getAsInt("archived.foo.bar", null).intValue());

        target = Settings.builder().put(currentSettings);
        update = Settings.builder();
        assertTrue(service.updateDynamicSettings(Settings.builder().putNull("some.dyn.setting").build(),
            target, update, "node"));
        assertEquals(1, dynamicSetting.get(target.build()).intValue());
        assertEquals(6, staticSetting.get(target.build()).intValue());
        assertEquals(9, target.build().getAsInt("archived.foo.bar", null).intValue());

        target = Settings.builder().put(currentSettings);
        update = Settings.builder();
        assertTrue(service.updateDynamicSettings(Settings.builder().putNull("archived.foo.bar").build(),
            target, update, "node"));
        assertEquals(5, dynamicSetting.get(target.build()).intValue());
        assertEquals(6, staticSetting.get(target.build()).intValue());
        assertNull(target.build().getAsInt("archived.foo.bar", null));

        target = Settings.builder().put(currentSettings);
        update = Settings.builder();
        assertTrue(service.updateDynamicSettings(Settings.builder().putNull("some.*").build(),
            target, update, "node"));
        assertEquals(1, dynamicSetting.get(target.build()).intValue());
        assertEquals(6, staticSetting.get(target.build()).intValue());
        assertEquals(9, target.build().getAsInt("archived.foo.bar", null).intValue());

        target = Settings.builder().put(currentSettings);
        update = Settings.builder();
        assertTrue(service.updateDynamicSettings(Settings.builder().putNull("*").build(),
            target, update, "node"));
        assertEquals(1, dynamicSetting.get(target.build()).intValue());
        assertEquals(6, staticSetting.get(target.build()).intValue());
        assertNull(target.build().getAsInt("archived.foo.bar", null));
    }

    public void testAddConsumer() {
        Setting<Integer> testSetting = Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> testSetting2 = Setting.intSetting("foo.bar.baz", 1, Property.Dynamic, Property.NodeScope);
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY, Collections.singleton(testSetting));

        AtomicInteger consumer = new AtomicInteger();
        service.addSettingsUpdateConsumer(testSetting, consumer::set);
        AtomicInteger consumer2 = new AtomicInteger();
        try {
            service.addSettingsUpdateConsumer(testSetting2, consumer2::set);
            fail("setting not registered");
        } catch (IllegalArgumentException ex) {
            assertEquals("Setting is not registered for key [foo.bar.baz]", ex.getMessage());
        }

        try {
            service.addSettingsUpdateConsumer(testSetting, testSetting2, (a, b) -> {consumer.set(a); consumer2.set(b);});
            fail("setting not registered");
        } catch (IllegalArgumentException ex) {
            assertEquals("Setting is not registered for key [foo.bar.baz]", ex.getMessage());
        }
        assertEquals(0, consumer.get());
        assertEquals(0, consumer2.get());
        service.applySettings(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", 15).build());
        assertEquals(2, consumer.get());
        assertEquals(0, consumer2.get());
    }

    public void testAddConsumerAffix() {
        Setting.AffixSetting<Integer> intSetting = Setting.affixKeySetting("foo.", "bar",
            (k) ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<List<Integer>> listSetting = Setting.affixKeySetting("foo.", "list",
            (k) -> Setting.listSetting(k, Arrays.asList("1"), Integer::parseInt, Property.Dynamic, Property.NodeScope));
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY,new HashSet<>(Arrays.asList(intSetting, listSetting)));
        Map<String, List<Integer>> listResults = new HashMap<>();
        Map<String, Integer> intResults = new HashMap<>();

        BiConsumer<String, Integer> intConsumer = intResults::put;
        BiConsumer<String, List<Integer>> listConsumer = listResults::put;

        service.addAffixUpdateConsumer(listSetting, listConsumer, (s, k) -> {});
        service.addAffixUpdateConsumer(intSetting, intConsumer, (s, k) -> {});
        assertEquals(0, listResults.size());
        assertEquals(0, intResults.size());
        service.applySettings(Settings.builder()
            .put("foo.test.bar", 2)
            .put("foo.test_1.bar", 7)
            .putArray("foo.test_list.list", "16", "17")
            .putArray("foo.test_list_1.list", "18", "19", "20")
            .build());
        assertEquals(2, intResults.get("test").intValue());
        assertEquals(7, intResults.get("test_1").intValue());
        assertEquals(Arrays.asList(16, 17), listResults.get("test_list"));
        assertEquals(Arrays.asList(18, 19, 20), listResults.get("test_list_1"));
        assertEquals(2, listResults.size());
        assertEquals(2, intResults.size());

        listResults.clear();
        intResults.clear();

        service.applySettings(Settings.builder()
            .put("foo.test.bar", 2)
            .put("foo.test_1.bar", 8)
            .putArray("foo.test_list.list", "16", "17")
            .putNull("foo.test_list_1.list")
            .build());
        assertNull("test wasn't changed", intResults.get("test"));
        assertEquals(8, intResults.get("test_1").intValue());
        assertNull("test_list wasn't changed", listResults.get("test_list"));
        assertEquals(Arrays.asList(1), listResults.get("test_list_1")); // reset to default
        assertEquals(1, listResults.size());
        assertEquals(1, intResults.size());
    }

    public void testApply() {
        Setting<Integer> testSetting = Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> testSetting2 = Setting.intSetting("foo.bar.baz", 1, Property.Dynamic, Property.NodeScope);
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY, new HashSet<>(Arrays.asList(testSetting, testSetting2)));

        AtomicInteger consumer = new AtomicInteger();
        service.addSettingsUpdateConsumer(testSetting, consumer::set);
        AtomicInteger consumer2 = new AtomicInteger();
        service.addSettingsUpdateConsumer(testSetting2, consumer2::set, (s) -> assertTrue(s > 0));

        AtomicInteger aC = new AtomicInteger();
        AtomicInteger bC = new AtomicInteger();
        service.addSettingsUpdateConsumer(testSetting, testSetting2, (a, b) -> {aC.set(a); bC.set(b);});

        assertEquals(0, consumer.get());
        assertEquals(0, consumer2.get());
        assertEquals(0, aC.get());
        assertEquals(0, bC.get());
        try {
            service.applySettings(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", -15).build());
            fail("invalid value");
        } catch (IllegalArgumentException ex) {
            assertEquals("illegal value can't update [foo.bar.baz] from [1] to [-15]", ex.getMessage());
        }
        assertEquals(0, consumer.get());
        assertEquals(0, consumer2.get());
        assertEquals(0, aC.get());
        assertEquals(0, bC.get());
        try {
            service.validateUpdate(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", -15).build());
            fail("invalid value");
        } catch (IllegalArgumentException ex) {
            assertEquals("illegal value can't update [foo.bar.baz] from [1] to [-15]", ex.getMessage());
        }

        assertEquals(0, consumer.get());
        assertEquals(0, consumer2.get());
        assertEquals(0, aC.get());
        assertEquals(0, bC.get());
        service.validateUpdate(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", 15).build());
        assertEquals(0, consumer.get());
        assertEquals(0, consumer2.get());
        assertEquals(0, aC.get());
        assertEquals(0, bC.get());

        service.applySettings(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", 15).build());
        assertEquals(2, consumer.get());
        assertEquals(15, consumer2.get());
        assertEquals(2, aC.get());
        assertEquals(15, bC.get());
    }

    public void testGet() {
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        // group setting - complex matcher
        Setting setting = settings.get("cluster.routing.allocation.require.value");
        assertEquals(setting, FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING);

        setting = settings.get("cluster.routing.allocation.total_shards_per_node");
        assertEquals(setting, ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING);

        // array settings - complex matcher
        assertNotNull(settings.get("transport.tracer.include." + randomIntBetween(1, 100)));
        assertSame(TransportService.TRACE_LOG_INCLUDE_SETTING, settings.get("transport.tracer.include." + randomIntBetween(1, 100)));

        // array settings - complex matcher - only accepts numbers
        assertNull(settings.get("transport.tracer.include.FOO"));
    }

    public void testIsDynamic(){
        ClusterSettings settings =
            new ClusterSettings(Settings.EMPTY,
                new HashSet<>(Arrays.asList(Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope),
                    Setting.intSetting("foo.bar.baz", 1, Property.NodeScope))));
        assertFalse(settings.hasDynamicSetting("foo.bar.baz"));
        assertTrue(settings.hasDynamicSetting("foo.bar"));
        assertNotNull(settings.get("foo.bar.baz"));
        settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        assertTrue(settings.hasDynamicSetting("transport.tracer.include." + randomIntBetween(1, 100)));
        assertFalse(settings.hasDynamicSetting("transport.tracer.include.BOOM"));
        assertTrue(settings.hasDynamicSetting("cluster.routing.allocation.require.value"));
    }

    public void testDiff() throws IOException {
        Setting<Integer> fooBarBaz = Setting.intSetting("foo.bar.baz", 1, Property.NodeScope);
        Setting<Integer> fooBar = Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope);
        Setting<Settings> someGroup = Setting.groupSetting("some.group.", Property.Dynamic, Property.NodeScope);
        Setting<Boolean> someAffix = Setting.affixKeySetting("some.prefix.", "somekey", (key) -> Setting.boolSetting(key, true,
            Property.NodeScope));
        Setting<List<String>> foorBarQuux =
                Setting.listSetting("foo.bar.quux", Arrays.asList("a", "b", "c"), Function.identity(), Property.NodeScope);
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, new HashSet<>(Arrays.asList(fooBar, fooBarBaz, foorBarQuux,
            someGroup, someAffix)));
        Settings diff = settings.diff(Settings.builder().put("foo.bar", 5).build(), Settings.EMPTY);
        assertEquals(4, diff.size()); // 4 since foo.bar.quux has 3 values essentially
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertArrayEquals(diff.getAsArray("foo.bar.quux", null), new String[] {"a", "b", "c"});

        diff = settings.diff(
                Settings.builder().put("foo.bar", 5).build(),
                Settings.builder().put("foo.bar.baz", 17).putArray("foo.bar.quux", "d", "e", "f").build());
        assertEquals(4, diff.size()); // 4 since foo.bar.quux has 3 values essentially
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(17));
        assertArrayEquals(diff.getAsArray("foo.bar.quux", null), new String[] {"d", "e", "f"});

        diff = settings.diff(
            Settings.builder().put("some.group.foo", 5).build(),
            Settings.builder().put("some.group.foobar", 17, "some.group.foo", 25).build());
        assertEquals(6, diff.size()); // 6 since foo.bar.quux has 3 values essentially
        assertThat(diff.getAsInt("some.group.foobar", null), equalTo(17));
        assertNull(diff.get("some.group.foo"));
        assertArrayEquals(diff.getAsArray("foo.bar.quux", null), new String[] {"a", "b", "c"});
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertThat(diff.getAsInt("foo.bar", null), equalTo(1));

        diff = settings.diff(
            Settings.builder().put("some.prefix.foo.somekey", 5).build(),
            Settings.builder().put("some.prefix.foobar.somekey", 17,
                "some.prefix.foo.somekey", 18).build());
        assertEquals(6, diff.size()); // 6 since foo.bar.quux has 3 values essentially
        assertThat(diff.getAsInt("some.prefix.foobar.somekey", null), equalTo(17));
        assertNull(diff.get("some.prefix.foo.somekey"));
        assertArrayEquals(diff.getAsArray("foo.bar.quux", null), new String[] {"a", "b", "c"});
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertThat(diff.getAsInt("foo.bar", null), equalTo(1));
    }

    public void testDiffWithAffixAndComplexMatcher() {
        Setting<Integer> fooBarBaz = Setting.intSetting("foo.bar.baz", 1, Property.NodeScope);
        Setting<Integer> fooBar = Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope);
        Setting<Settings> someGroup = Setting.groupSetting("some.group.", Property.Dynamic, Property.NodeScope);
        Setting<Boolean> someAffix = Setting.affixKeySetting("some.prefix.", "somekey", (key) -> Setting.boolSetting(key, true,
            Property.NodeScope));
        Setting<List<String>> foorBarQuux = Setting.affixKeySetting("foo.", "quux",
            (key) -> Setting.listSetting(key,  Arrays.asList("a", "b", "c"), Function.identity(), Property.NodeScope));
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, new HashSet<>(Arrays.asList(fooBar, fooBarBaz, foorBarQuux,
            someGroup, someAffix)));
        Settings diff = settings.diff(Settings.builder().put("foo.bar", 5).build(), Settings.EMPTY);
        assertEquals(1, diff.size());
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertNull(diff.getAsArray("foo.bar.quux", null)); // affix settings don't know their concrete keys

        diff = settings.diff(
            Settings.builder().put("foo.bar", 5).build(),
            Settings.builder().put("foo.bar.baz", 17).putArray("foo.bar.quux", "d", "e", "f").build());
        assertEquals(4, diff.size());
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(17));
        assertArrayEquals(diff.getAsArray("foo.bar.quux", null), new String[] {"d", "e", "f"});

        diff = settings.diff(
            Settings.builder().put("some.group.foo", 5).build(),
            Settings.builder().put("some.group.foobar", 17, "some.group.foo", 25).build());
        assertEquals(3, diff.size());
        assertThat(diff.getAsInt("some.group.foobar", null), equalTo(17));
        assertNull(diff.get("some.group.foo"));
        assertNull(diff.getAsArray("foo.bar.quux", null)); // affix settings don't know their concrete keys
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertThat(diff.getAsInt("foo.bar", null), equalTo(1));

        diff = settings.diff(
            Settings.builder().put("some.prefix.foo.somekey", 5).build(),
            Settings.builder().put("some.prefix.foobar.somekey", 17,
                "some.prefix.foo.somekey", 18).build());
        assertEquals(3, diff.size());
        assertThat(diff.getAsInt("some.prefix.foobar.somekey", null), equalTo(17));
        assertNull(diff.get("some.prefix.foo.somekey"));
        assertNull(diff.getAsArray("foo.bar.quux", null)); // affix settings don't know their concrete keys
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertThat(diff.getAsInt("foo.bar", null), equalTo(1));

        diff = settings.diff(
            Settings.builder().put("some.prefix.foo.somekey", 5).build(),
            Settings.builder().put("some.prefix.foobar.somekey", 17,
                "some.prefix.foo.somekey", 18)
            .putArray("foo.bar.quux", "x", "y", "z")
            .putArray("foo.baz.quux", "d", "e", "f")
                .build());
        assertEquals(9, diff.size());
        assertThat(diff.getAsInt("some.prefix.foobar.somekey", null), equalTo(17));
        assertNull(diff.get("some.prefix.foo.somekey"));
        assertArrayEquals(diff.getAsArray("foo.bar.quux", null), new String[] {"x", "y", "z"});
        assertArrayEquals(diff.getAsArray("foo.baz.quux", null), new String[] {"d", "e", "f"});
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertThat(diff.getAsInt("foo.bar", null), equalTo(1));
    }

    public void testUpdateTracer() {
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AtomicReference<List<String>> ref = new AtomicReference<>();
        settings.addSettingsUpdateConsumer(TransportService.TRACE_LOG_INCLUDE_SETTING, ref::set);
        settings.applySettings(Settings.builder()
                .putArray("transport.tracer.include", "internal:index/shard/recovery/*", "internal:gateway/local*").build());
        assertNotNull(ref.get().size());
        assertEquals(ref.get().size(), 2);
        assertTrue(ref.get().contains("internal:index/shard/recovery/*"));
        assertTrue(ref.get().contains("internal:gateway/local*"));
    }

    public void testGetSetting() {
        IndexScopedSettings settings = new IndexScopedSettings(
           Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        IndexScopedSettings copy = settings.copy(Settings.builder().put("index.store.type", "boom").build(),
                newIndexMeta("foo", Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 3).build()));
        assertEquals(3, copy.get(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING).intValue());
        assertEquals(1, copy.get(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING).intValue());
        assertEquals("boom", copy.get(IndexModule.INDEX_STORE_TYPE_SETTING)); // test fallback to node settings
    }

    public void testValidateWithSuggestion() {
        IndexScopedSettings settings = new IndexScopedSettings(
            Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
            () -> settings.validate(Settings.builder().put("index.numbe_of_replica", "1").build()));
        assertEquals(iae.getMessage(), "unknown setting [index.numbe_of_replica] did you mean [index.number_of_replicas]?");
    }

    public void testValidate() {
        IndexScopedSettings settings = new IndexScopedSettings(
            Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        String unknownMsgSuffix = " please check that any required plugins are installed, or check the breaking changes documentation for" +
            " removed settings";
        settings.validate(Settings.builder().put("index.store.type", "boom"));
        settings.validate(Settings.builder().put("index.store.type", "boom").build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            settings.validate(Settings.builder().put("index.store.type", "boom", "i.am.not.a.setting", true)));
        assertEquals("unknown setting [i.am.not.a.setting]" + unknownMsgSuffix, e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () ->
            settings.validate(Settings.builder().put("index.store.type", "boom", "i.am.not.a.setting", true).build()));
        assertEquals("unknown setting [i.am.not.a.setting]" + unknownMsgSuffix, e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () ->
            settings.validate(Settings.builder().put("index.store.type", "boom", "index.number_of_replicas", true).build()));
        assertEquals("Failed to parse value [true] for setting [index.number_of_replicas]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () ->
            settings.validate("index.number_of_replicas", Settings.builder().put("index.number_of_replicas", "true").build()));
        assertEquals("Failed to parse value [true] for setting [index.number_of_replicas]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () ->
            settings.validate("index.similarity.classic.type", Settings.builder().put("index.similarity.classic.type", "mine").build()));
        assertEquals("illegal value for [index.similarity.classic] cannot redefine built-in similarity", e.getMessage());
    }

    public void testValidateSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("some.secure.setting", "secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, Collections.emptySet());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.validate(settings));
        assertThat(e.getMessage(), startsWith("unknown secure setting [some.secure.setting]"));

        ClusterSettings clusterSettings2 = new ClusterSettings(settings,
            Collections.singleton(SecureSetting.secureString("some.secure.setting", null, false)));
        clusterSettings2.validate(settings);
    }


    public static IndexMetaData newIndexMeta(String name, Settings indexSettings) {
        Settings build = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(indexSettings)
            .build();
        IndexMetaData metaData = IndexMetaData.builder(name).settings(build).build();
        return metaData;
    }

    public void testKeyPattern() {
        assertTrue(AbstractScopedSettings.isValidKey("a.b.c-b.d"));
        assertTrue(AbstractScopedSettings.isValidKey("a.b.c.d"));
        assertTrue(AbstractScopedSettings.isValidKey("a.b_012.c_b.d"));
        assertTrue(AbstractScopedSettings.isValidKey("a"));
        assertFalse(AbstractScopedSettings.isValidKey("a b"));
        assertFalse(AbstractScopedSettings.isValidKey(""));
        assertFalse(AbstractScopedSettings.isValidKey("\""));

        try {
            new IndexScopedSettings(
                Settings.EMPTY, Collections.singleton(Setting.groupSetting("foo.bar.", Property.IndexScope)));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("illegal settings key: [foo.bar.] must start with [index.]", e.getMessage());
        }

        try {
            new IndexScopedSettings(
                Settings.EMPTY, Collections.singleton(Setting.simpleString("foo.bar", Property.IndexScope)));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("illegal settings key: [foo.bar] must start with [index.]", e.getMessage());
        }

        try {
            new IndexScopedSettings(
                Settings.EMPTY, Collections.singleton(Setting.groupSetting("index. foo.", Property.IndexScope)));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("illegal settings key: [index. foo.]", e.getMessage());
        }
        new IndexScopedSettings(
            Settings.EMPTY, Collections.singleton(Setting.groupSetting("index.", Property.IndexScope)));
        try {
            new IndexScopedSettings(
                Settings.EMPTY, Collections.singleton(Setting.boolSetting("index.", true, Property.IndexScope)));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("illegal settings key: [index.]", e.getMessage());
        }
        new IndexScopedSettings(
            Settings.EMPTY, Collections.singleton(Setting.boolSetting("index.boo", true, Property.IndexScope)));

        new ClusterSettings(
            Settings.EMPTY, Collections.singleton(Setting.boolSetting("index.boo", true, Property.NodeScope)));
    }

    public void testAffixKeyPattern() {
        assertTrue(AbstractScopedSettings.isValidAffixKey("prefix.*.suffix"));
        assertTrue(AbstractScopedSettings.isValidAffixKey("prefix.*.split.suffix"));
        assertTrue(AbstractScopedSettings.isValidAffixKey("split.prefix.*.split.suffix"));
        assertFalse(AbstractScopedSettings.isValidAffixKey("prefix.*.suffix."));
        assertFalse(AbstractScopedSettings.isValidAffixKey("prefix.*"));
        assertFalse(AbstractScopedSettings.isValidAffixKey("*.suffix"));
        assertFalse(AbstractScopedSettings.isValidAffixKey("*"));
        assertFalse(AbstractScopedSettings.isValidAffixKey(""));
    }

    public void testLoggingUpdates() {
        final Level level = ESLoggerFactory.getRootLogger().getLevel();
        final Level testLevel = ESLoggerFactory.getLogger("test").getLevel();
        Level property = randomFrom(Level.values());
        Settings.Builder builder = Settings.builder().put("logger.level", property);
        try {
            ClusterSettings settings = new ClusterSettings(builder.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            IllegalArgumentException ex =
                expectThrows(
                    IllegalArgumentException.class,
                    () -> settings.validate(Settings.builder().put("logger._root", "boom").build()));
            assertEquals("Unknown level constant [BOOM].", ex.getMessage());
            assertEquals(level, ESLoggerFactory.getRootLogger().getLevel());
            settings.applySettings(Settings.builder().put("logger._root", "TRACE").build());
            assertEquals(Level.TRACE, ESLoggerFactory.getRootLogger().getLevel());
            settings.applySettings(Settings.builder().build());
            assertEquals(property, ESLoggerFactory.getRootLogger().getLevel());
            settings.applySettings(Settings.builder().put("logger.test", "TRACE").build());
            assertEquals(Level.TRACE, ESLoggerFactory.getLogger("test").getLevel());
            settings.applySettings(Settings.builder().build());
            assertEquals(property, ESLoggerFactory.getLogger("test").getLevel());
        } finally {
            Loggers.setLevel(ESLoggerFactory.getRootLogger(), level);
            Loggers.setLevel(ESLoggerFactory.getLogger("test"), testLevel);
        }
    }

    public void testFallbackToLoggerLevel() {
        final Level level = ESLoggerFactory.getRootLogger().getLevel();
        try {
            ClusterSettings settings =
                new ClusterSettings(Settings.builder().put("logger.level", "ERROR").build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            assertEquals(level, ESLoggerFactory.getRootLogger().getLevel());
            settings.applySettings(Settings.builder().put("logger._root", "TRACE").build());
            assertEquals(Level.TRACE, ESLoggerFactory.getRootLogger().getLevel());
            settings.applySettings(Settings.builder().build()); // here we fall back to 'logger.level' which is our default.
            assertEquals(Level.ERROR, ESLoggerFactory.getRootLogger().getLevel());
        } finally {
            Loggers.setLevel(ESLoggerFactory.getRootLogger(), level);
        }
    }

    public void testOverlappingComplexMatchSettings() {
        Set<Setting<?>> settings = new LinkedHashSet<>(2);
        final boolean groupFirst = randomBoolean();
        final Setting<?> groupSetting = Setting.groupSetting("foo.", Property.NodeScope);
        final Setting<?> listSetting =
            Setting.listSetting("foo.bar", Collections.emptyList(), Function.identity(), Property.NodeScope);
        settings.add(groupFirst ? groupSetting : listSetting);
        settings.add(groupFirst ? listSetting : groupSetting);

        try {
            new ClusterSettings(Settings.EMPTY, settings);
            fail("an exception should have been thrown because settings overlap");
        } catch (IllegalArgumentException e) {
            if (groupFirst) {
                assertEquals("complex setting key: [foo.bar] overlaps existing setting key: [foo.]", e.getMessage());
            } else {
                assertEquals("complex setting key: [foo.] overlaps existing setting key: [foo.bar]", e.getMessage());
            }
        }
    }
}
