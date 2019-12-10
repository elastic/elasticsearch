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
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.sameInstance;

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

    public void testResetSettingWithIPValidator() {
        Settings currentSettings = Settings.builder().put("index.routing.allocation.require._ip", "192.168.0.1,127.0.0.1")
            .put("index.some.dyn.setting", 1)
            .build();
        Setting<Integer> dynamicSetting = Setting.intSetting("index.some.dyn.setting", 1, Property.Dynamic, Property.IndexScope);

        IndexScopedSettings settings = new IndexScopedSettings(currentSettings,
            new HashSet<>(Arrays.asList(dynamicSetting, IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING)));
        Map<String, String> s = IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(currentSettings);
        assertEquals(1, s.size());
        assertEquals("192.168.0.1,127.0.0.1", s.get("_ip"));
        Settings.Builder builder = Settings.builder();
        Settings updates = Settings.builder().putNull("index.routing.allocation.require._ip")
            .put("index.some.dyn.setting", 1).build();
        settings.validate(updates, false);
        settings.updateDynamicSettings(updates,
            Settings.builder().put(currentSettings), builder, "node");
        currentSettings = builder.build();
        s = IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(currentSettings);
        assertEquals(0, s.size());
        assertEquals(1, dynamicSetting.get(currentSettings).intValue());
        assertEquals(1, currentSettings.size());
    }

    public void testNoopSettingsUpdate() {
        String value = "192.168.0.1,127.0.0.1";
        String setting = "index.routing.allocation.require._ip";
        Settings currentSettings = Settings.builder().put(setting, value)
            .build();
        Settings updates = Settings.builder().put(setting, value).build();
        IndexScopedSettings settings = new IndexScopedSettings(currentSettings,
            new HashSet<>(Collections.singletonList(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING)));
        assertFalse(settings.updateSettings(updates, Settings.builder().put(currentSettings), Settings.builder(), ""));
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
            service.addSettingsUpdateConsumer(testSetting, testSetting2, (a, b) -> {
                consumer.set(a);
                consumer2.set(b);
            });
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

    public void testDependentSettings() {
        Setting.AffixSetting<String> stringSetting = Setting.affixKeySetting("foo.", "name",
            (k) -> Setting.simpleString(k, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<Integer> intSetting = Setting.affixKeySetting("foo.", "bar",
            (k) ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope), () -> stringSetting);

        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY, new HashSet<>(Arrays.asList(intSetting, stringSetting)));

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
            () -> service.validate(Settings.builder().put("foo.test.bar", 7).build(), true));
        assertEquals("missing required setting [foo.test.name] for setting [foo.test.bar]", iae.getMessage());

        service.validate(Settings.builder()
            .put("foo.test.name", "test")
            .put("foo.test.bar", 7)
            .build(), true);

        service.validate(Settings.builder().put("foo.test.bar", 7).build(), false);
    }

    public void testDependentSettingsValidate() {
        Setting.AffixSetting<String> stringSetting = Setting.affixKeySetting(
            "foo.",
            "name",
            (k) -> Setting.simpleString(k, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<Integer> intSetting = Setting.affixKeySetting(
            "foo.",
            "bar",
            (k) -> Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope),
            new Setting.AffixSettingDependency() {

                @Override
                public Setting.AffixSetting getSetting() {
                    return stringSetting;
                }

                @Override
                public void validate(final String key, final Object value, final Object dependency) {
                    if ("valid".equals(dependency) == false) {
                        throw new SettingsException("[" + key + "] is set but [name] is [" + dependency + "]");
                    }
                }
            });

        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY, new HashSet<>(Arrays.asList(intSetting, stringSetting)));

        SettingsException iae = expectThrows(
            SettingsException.class,
            () -> service.validate(Settings.builder().put("foo.test.bar", 7).put("foo.test.name", "invalid").build(), true));
        assertEquals("[foo.test.bar] is set but [name] is [invalid]", iae.getMessage());

        service.validate(Settings.builder()
                .put("foo.test.bar", 7)
                .put("foo.test.name", "valid")
                .build(),
            true);

        service.validate(Settings.builder()
            .put("foo.test.bar", 7)
            .put("foo.test.name", "invalid")
            .build(),
            false);
    }

    public void testDependentSettingsWithFallback() {
        Setting.AffixSetting<String> nameFallbackSetting =
                Setting.affixKeySetting("fallback.", "name", k -> Setting.simpleString(k, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<String> nameSetting = Setting.affixKeySetting(
                "foo.",
                "name",
                k -> Setting.simpleString(
                        k,
                        "_na_".equals(k)
                                ? nameFallbackSetting.getConcreteSettingForNamespace(k)
                                : nameFallbackSetting.getConcreteSetting(k.replaceAll("^foo", "fallback")),
                        Property.Dynamic,
                        Property.NodeScope));
        Setting.AffixSetting<Integer> barSetting = Setting.affixKeySetting(
            "foo.",
            "bar",
            k -> Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope),
            () -> nameSetting);

        final AbstractScopedSettings service =
                new ClusterSettings(Settings.EMPTY,new HashSet<>(Arrays.asList(nameFallbackSetting, nameSetting, barSetting)));

        final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> service.validate(Settings.builder().put("foo.test.bar", 7).build(), true));
        assertThat(e, hasToString(containsString("missing required setting [foo.test.name] for setting [foo.test.bar]")));

        service.validate(Settings.builder().put("foo.test.name", "test").put("foo.test.bar", 7).build(), true);
        service.validate(Settings.builder().put("fallback.test.name", "test").put("foo.test.bar", 7).build(), true);
    }

    public void testTupleAffixUpdateConsumer() {
        String prefix = randomAlphaOfLength(3) + "foo.";
        String intSuffix = randomAlphaOfLength(3);
        String listSuffix = randomAlphaOfLength(4);
        Setting.AffixSetting<Integer> intSetting = Setting.affixKeySetting(prefix, intSuffix,
            (k) ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<List<Integer>> listSetting = Setting.affixKeySetting(prefix, listSuffix,
            (k) -> Setting.listSetting(k, Arrays.asList("1"), Integer::parseInt, Property.Dynamic, Property.NodeScope));
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY,new HashSet<>(Arrays.asList(intSetting, listSetting)));
        Map<String, Tuple<List<Integer>, Integer>> results = new HashMap<>();
        Function<String, String> listBuilder = g -> (prefix + g + "." + listSuffix);
        Function<String, String> intBuilder = g -> (prefix + g + "." + intSuffix);
        String group1 = randomAlphaOfLength(3);
        String group2 = randomAlphaOfLength(4);
        String group3 = randomAlphaOfLength(5);
        BiConsumer<String, Tuple<List<Integer>, Integer>> listConsumer = results::put;

        service.addAffixUpdateConsumer(listSetting, intSetting, listConsumer, (s, k) -> {
            if (k.v1().isEmpty() && k.v2() == 2) {
                throw new IllegalArgumentException("boom");
            }
        });
        assertEquals(0, results.size());
        service.applySettings(Settings.builder()
            .put(intBuilder.apply(group1), 2)
            .put(intBuilder.apply(group2), 7)
            .putList(listBuilder.apply(group1), "16", "17")
            .putList(listBuilder.apply(group2), "18", "19", "20")
            .build());
        assertEquals(2, results.get(group1).v2().intValue());
        assertEquals(7, results.get(group2).v2().intValue());
        assertEquals(Arrays.asList(16, 17), results.get(group1).v1());
        assertEquals(Arrays.asList(18, 19, 20), results.get(group2).v1());
        assertEquals(2, results.size());

        results.clear();

        service.applySettings(Settings.builder()
            .put(intBuilder.apply(group1), 2)
            .put(intBuilder.apply(group2), 7)
            .putList(listBuilder.apply(group1), "16", "17")
            .putNull(listBuilder.apply(group2)) // removed
            .build());

        assertNull(group1 + " wasn't changed", results.get(group1));
        assertEquals(1, results.get(group2).v1().size());
        assertEquals(Arrays.asList(1), results.get(group2).v1());
        assertEquals(7, results.get(group2).v2().intValue());
        assertEquals(1, results.size());
        results.clear();

        service.applySettings(Settings.builder()
            .put(intBuilder.apply(group1), 2)
            .put(intBuilder.apply(group2), 7)
            .putList(listBuilder.apply(group1), "16", "17")
            .putList(listBuilder.apply(group3), "5", "6") // added
            .build());
        assertNull(group1 + " wasn't changed", results.get(group1));
        assertNull(group2 + " wasn't changed", results.get(group2));

        assertEquals(2, results.get(group3).v1().size());
        assertEquals(Arrays.asList(5, 6), results.get(group3).v1());
        assertEquals(1, results.get(group3).v2().intValue());
        assertEquals(1, results.size());
        results.clear();

        service.applySettings(Settings.builder()
            .put(intBuilder.apply(group1), 4) // modified
            .put(intBuilder.apply(group2), 7)
            .putList(listBuilder.apply(group1), "16", "17")
            .putList(listBuilder.apply(group3), "5", "6")
            .build());
        assertNull(group2 + " wasn't changed", results.get(group2));
        assertNull(group3 + " wasn't changed", results.get(group3));

        assertEquals(2, results.get(group1).v1().size());
        assertEquals(Arrays.asList(16, 17), results.get(group1).v1());
        assertEquals(4, results.get(group1).v2().intValue());
        assertEquals(1, results.size());
        results.clear();

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
            service.applySettings(Settings.builder()
                .put(intBuilder.apply(group1), 2) // modified to trip validator
                .put(intBuilder.apply(group2), 7)
                .putList(listBuilder.apply(group1)) // modified to trip validator
                .putList(listBuilder.apply(group3), "5", "6")
                .build())
        );
        assertEquals("boom", iae.getMessage());
        assertEquals(0, results.size());
    }

    public void testAffixGroupUpdateConsumer() {
        String prefix = randomAlphaOfLength(3) + "foo.";
        String intSuffix = randomAlphaOfLength(3);
        String listSuffix = randomAlphaOfLength(4);
        Setting.AffixSetting<Integer> intSetting = Setting.affixKeySetting(prefix, intSuffix,
            (k) ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<List<Integer>> listSetting = Setting.affixKeySetting(prefix, listSuffix,
            (k) -> Setting.listSetting(k, Arrays.asList("1"), Integer::parseInt, Property.Dynamic, Property.NodeScope));
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY,new HashSet<>(Arrays.asList(intSetting, listSetting)));
        Map<String, Settings> results = new HashMap<>();
        Function<String, String> listBuilder = g -> (prefix + g + "." + listSuffix);
        Function<String, String> intBuilder = g -> (prefix + g + "." + intSuffix);
        String group1 = randomAlphaOfLength(3);
        String group2 = randomAlphaOfLength(4);
        String group3 = randomAlphaOfLength(5);
        BiConsumer<String, Settings> listConsumer = results::put;

        service.addAffixGroupUpdateConsumer(Arrays.asList(intSetting, listSetting), listConsumer);
        assertEquals(0, results.size());
        service.applySettings(Settings.builder()
            .put(intBuilder.apply(group1), 2)
            .put(intBuilder.apply(group2), 7)
            .putList(listBuilder.apply(group1), "16", "17")
            .putList(listBuilder.apply(group2), "18", "19", "20")
            .build());
        Settings groupOneSettings = results.get(group1);
        Settings groupTwoSettings = results.get(group2);
        assertEquals(2, intSetting.getConcreteSettingForNamespace(group1).get(groupOneSettings).intValue());
        assertEquals(7, intSetting.getConcreteSettingForNamespace(group2).get(groupTwoSettings).intValue());
        assertEquals(Arrays.asList(16, 17), listSetting.getConcreteSettingForNamespace(group1).get(groupOneSettings));
        assertEquals(Arrays.asList(18, 19, 20), listSetting.getConcreteSettingForNamespace(group2).get(groupTwoSettings));
        assertEquals(2, groupOneSettings.size());
        assertEquals(2, groupTwoSettings.size());
        assertEquals(2, results.size());

        results.clear();

        service.applySettings(Settings.builder()
            .put(intBuilder.apply(group1), 2)
            .put(intBuilder.apply(group2), 7)
            .putList(listBuilder.apply(group1), "16", "17")
            .putNull(listBuilder.apply(group2)) // removed
            .build());

        assertNull(group1 + " wasn't changed", results.get(group1));
        groupTwoSettings = results.get(group2);
        assertEquals(7, intSetting.getConcreteSettingForNamespace(group2).get(groupTwoSettings).intValue());
        assertEquals(Arrays.asList(1), listSetting.getConcreteSettingForNamespace(group2).get(groupTwoSettings));
        assertEquals(1, results.size());
        assertEquals(2, groupTwoSettings.size());
        results.clear();

        service.applySettings(Settings.builder()
            .put(intBuilder.apply(group1), 2)
            .put(intBuilder.apply(group2), 7)
            .putList(listBuilder.apply(group1), "16", "17")
            .putList(listBuilder.apply(group3), "5", "6") // added
            .build());
        assertNull(group1 + " wasn't changed", results.get(group1));
        assertNull(group2 + " wasn't changed", results.get(group2));

        Settings groupThreeSettings = results.get(group3);
        assertEquals(1, intSetting.getConcreteSettingForNamespace(group3).get(groupThreeSettings).intValue());
        assertEquals(Arrays.asList(5, 6), listSetting.getConcreteSettingForNamespace(group3).get(groupThreeSettings));
        assertEquals(1, results.size());
        assertEquals(1, groupThreeSettings.size());
        results.clear();

        service.applySettings(Settings.builder()
            .put(intBuilder.apply(group1), 4) // modified
            .put(intBuilder.apply(group2), 7)
            .putList(listBuilder.apply(group1), "16", "17")
            .putList(listBuilder.apply(group3), "5", "6")
            .build());
        assertNull(group2 + " wasn't changed", results.get(group2));
        assertNull(group3 + " wasn't changed", results.get(group3));

        groupOneSettings = results.get(group1);
        assertEquals(4, intSetting.getConcreteSettingForNamespace(group1).get(groupOneSettings).intValue());
        assertEquals(Arrays.asList(16, 17), listSetting.getConcreteSettingForNamespace(group1).get(groupOneSettings));
        assertEquals(1, results.size());
        assertEquals(2, groupOneSettings.size());
        results.clear();
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
            .putList("foo.test_list.list", "16", "17")
            .putList("foo.test_list_1.list", "18", "19", "20")
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
            .putList("foo.test_list.list", "16", "17")
            .putNull("foo.test_list_1.list")
            .build());
        assertNull("test wasn't changed", intResults.get("test"));
        assertEquals(8, intResults.get("test_1").intValue());
        assertNull("test_list wasn't changed", listResults.get("test_list"));
        assertEquals(Arrays.asList(1), listResults.get("test_list_1")); // reset to default
        assertEquals(1, listResults.size());
        assertEquals(1, intResults.size());
    }

    public void testAddConsumerAffixMap() {
        Setting.AffixSetting<Integer> intSetting = Setting.affixKeySetting("foo.", "bar",
            (k) ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<List<Integer>> listSetting = Setting.affixKeySetting("foo.", "list",
            (k) -> Setting.listSetting(k, Arrays.asList("1"), Integer::parseInt, Property.Dynamic, Property.NodeScope));
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY,new HashSet<>(Arrays.asList(intSetting, listSetting)));
        Map<String, List<Integer>> listResults = new HashMap<>();
        Map<String, Integer> intResults = new HashMap<>();

        Consumer<Map<String,Integer>> intConsumer = (map) -> {
            intResults.clear();
            intResults.putAll(map);
        };
        Consumer<Map<String, List<Integer>>> listConsumer = (map) -> {
            listResults.clear();
            listResults.putAll(map);
        };
        service.addAffixMapUpdateConsumer(listSetting, listConsumer, (s, k) -> {});
        service.addAffixMapUpdateConsumer(intSetting, intConsumer, (s, k) -> {});
        assertEquals(0, listResults.size());
        assertEquals(0, intResults.size());
        service.applySettings(Settings.builder()
            .put("foo.test.bar", 2)
            .put("foo.test_1.bar", 7)
            .putList("foo.test_list.list", "16", "17")
            .putList("foo.test_list_1.list", "18", "19", "20")
            .build());
        assertEquals(2, intResults.get("test").intValue());
        assertEquals(7, intResults.get("test_1").intValue());
        assertEquals(Arrays.asList(16, 17), listResults.get("test_list"));
        assertEquals(Arrays.asList(18, 19, 20), listResults.get("test_list_1"));
        assertEquals(2, listResults.size());
        assertEquals(2, intResults.size());

        service.applySettings(Settings.builder()
            .put("foo.test.bar", 2)
            .put("foo.test_1.bar", 7)
            .putList("foo.test_list.list", "16", "17")
            .putList("foo.test_list_1.list", "18", "19", "20")
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
            .putList("foo.test_list.list", "16", "17")
            .putNull("foo.test_list_1.list")
            .build());
        assertNull("test wasn't changed", intResults.get("test"));
        assertEquals(8, intResults.get("test_1").intValue());
        assertNull("test_list wasn't changed", listResults.get("test_list"));
        assertEquals(Arrays.asList(1), listResults.get("test_list_1")); // reset to default
        assertEquals(1, listResults.size());
        assertEquals(1, intResults.size());
    }

    public void testAffixMapConsumerNotCalledWithNull() {
        Setting.AffixSetting<Integer> prefixSetting = Setting.prefixKeySetting("eggplant.",
                (k) ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope));
        Setting.AffixSetting<Integer> otherSetting = Setting.prefixKeySetting("other.",
                (k) ->  Setting.intSetting(k, 1, Property.Dynamic, Property.NodeScope));
        AbstractScopedSettings service = new ClusterSettings(Settings.EMPTY,new HashSet<>(Arrays.asList(prefixSetting, otherSetting)));
        Map<String, Integer> affixResults = new HashMap<>();

        Consumer<Map<String,Integer>> consumer = (map) -> {
            logger.info("--> consuming settings {}", map);
            affixResults.clear();
            affixResults.putAll(map);
        };
        service.addAffixMapUpdateConsumer(prefixSetting, consumer, (s, k) -> {});
        assertEquals(0, affixResults.size());
        service.applySettings(Settings.builder()
                .put("eggplant._name", 2)
                .build());
        assertThat(affixResults.size(), equalTo(1));
        assertThat(affixResults.get("_name"), equalTo(2));

        service.applySettings(Settings.builder()
                .put("eggplant._name", 2)
                .put("other.thing", 3)
                .build());

        assertThat(affixResults.get("_name"), equalTo(2));
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
        service.addSettingsUpdateConsumer(testSetting, testSetting2, (a, b) -> {
            aC.set(a);
            bC.set(b);
        });

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
        // affix setting - complex matcher
        Setting setting = settings.get("cluster.routing.allocation.require.value");
        assertEquals(setting,
            FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSetting("cluster.routing.allocation.require.value"));

        setting = settings.get("cluster.routing.allocation.total_shards_per_node");
        assertEquals(setting, ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING);

        // array settings - complex matcher
        assertNotNull(settings.get("transport.tracer.include." + randomIntBetween(1, 100)));
        assertSame(TransportSettings.TRACE_LOG_INCLUDE_SETTING, settings.get("transport.tracer.include." + randomIntBetween(1, 100)));

        // array settings - complex matcher - only accepts numbers
        assertNull(settings.get("transport.tracer.include.FOO"));
    }

    public void testIsDynamic(){
        ClusterSettings settings =
            new ClusterSettings(Settings.EMPTY,
                new HashSet<>(Arrays.asList(Setting.intSetting("foo.bar", 1, Property.Dynamic, Property.NodeScope),
                    Setting.intSetting("foo.bar.baz", 1, Property.NodeScope))));
        assertFalse(settings.isDynamicSetting("foo.bar.baz"));
        assertTrue(settings.isDynamicSetting("foo.bar"));
        assertNotNull(settings.get("foo.bar.baz"));
        settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        assertTrue(settings.isDynamicSetting("transport.tracer.include." + randomIntBetween(1, 100)));
        assertFalse(settings.isDynamicSetting("transport.tracer.include.BOOM"));
        assertTrue(settings.isDynamicSetting("cluster.routing.allocation.require.value"));
    }

    public void testIsFinal() {
        ClusterSettings settings =
            new ClusterSettings(Settings.EMPTY,
                new HashSet<>(Arrays.asList(Setting.intSetting("foo.int", 1, Property.Final, Property.NodeScope),
                    Setting.groupSetting("foo.group.",  Property.Final, Property.NodeScope),
                    Setting.groupSetting("foo.list.",  Property.Final, Property.NodeScope),
                    Setting.intSetting("foo.int.baz", 1, Property.NodeScope))));

        assertFalse(settings.isFinalSetting("foo.int.baz"));
        assertTrue(settings.isFinalSetting("foo.int"));

        assertFalse(settings.isFinalSetting("foo.list"));
        assertTrue(settings.isFinalSetting("foo.list.0.key"));
        assertTrue(settings.isFinalSetting("foo.list.key"));

        assertFalse(settings.isFinalSetting("foo.group"));
        assertTrue(settings.isFinalSetting("foo.group.key"));
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
        assertEquals(2, diff.size());
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertEquals(diff.getAsList("foo.bar.quux", null), Arrays.asList("a", "b", "c"));

        diff = settings.diff(
                Settings.builder().put("foo.bar", 5).build(),
                Settings.builder().put("foo.bar.baz", 17).putList("foo.bar.quux", "d", "e", "f").build());
        assertEquals(2, diff.size());
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(17));
        assertEquals(diff.getAsList("foo.bar.quux", null), Arrays.asList("d", "e", "f"));

        diff = settings.diff(
            Settings.builder().put("some.group.foo", 5).build(),
            Settings.builder().put("some.group.foobar", 17).put("some.group.foo", 25).build());
        assertEquals(4, diff.size());
        assertThat(diff.getAsInt("some.group.foobar", null), equalTo(17));
        assertNull(diff.get("some.group.foo"));
        assertEquals(diff.getAsList("foo.bar.quux", null), Arrays.asList("a", "b", "c"));
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertThat(diff.getAsInt("foo.bar", null), equalTo(1));

        diff = settings.diff(
            Settings.builder().put("some.prefix.foo.somekey", 5).build(),
            Settings.builder().put("some.prefix.foobar.somekey", 17).put("some.prefix.foo.somekey", 18).build());
        assertEquals(4, diff.size());
        assertThat(diff.getAsInt("some.prefix.foobar.somekey", null), equalTo(17));
        assertNull(diff.get("some.prefix.foo.somekey"));
        assertEquals(diff.getAsList("foo.bar.quux", null), Arrays.asList("a", "b", "c"));
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
        assertNull(diff.getAsList("foo.bar.quux", null)); // affix settings don't know their concrete keys

        diff = settings.diff(
            Settings.builder().put("foo.bar", 5).build(),
            Settings.builder().put("foo.bar.baz", 17).putList("foo.bar.quux", "d", "e", "f").build());
        assertEquals(2, diff.size());
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(17));
        assertEquals(diff.getAsList("foo.bar.quux", null), Arrays.asList("d", "e", "f"));

        diff = settings.diff(
            Settings.builder().put("some.group.foo", 5).build(),
            Settings.builder().put("some.group.foobar", 17).put("some.group.foo", 25).build());
        assertEquals(3, diff.size());
        assertThat(diff.getAsInt("some.group.foobar", null), equalTo(17));
        assertNull(diff.get("some.group.foo"));
        assertNull(diff.getAsList("foo.bar.quux", null)); // affix settings don't know their concrete keys
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertThat(diff.getAsInt("foo.bar", null), equalTo(1));

        diff = settings.diff(
            Settings.builder().put("some.prefix.foo.somekey", 5).build(),
            Settings.builder().put("some.prefix.foobar.somekey", 17).put("some.prefix.foo.somekey", 18).build());
        assertEquals(3, diff.size());
        assertThat(diff.getAsInt("some.prefix.foobar.somekey", null), equalTo(17));
        assertNull(diff.get("some.prefix.foo.somekey"));
        assertNull(diff.getAsList("foo.bar.quux", null)); // affix settings don't know their concrete keys
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertThat(diff.getAsInt("foo.bar", null), equalTo(1));

        diff = settings.diff(
            Settings.builder().put("some.prefix.foo.somekey", 5).build(),
            Settings.builder().put("some.prefix.foobar.somekey", 17).put("some.prefix.foo.somekey", 18)
            .putList("foo.bar.quux", "x", "y", "z")
            .putList("foo.baz.quux", "d", "e", "f")
                .build());
        assertEquals(5, diff.size());
        assertThat(diff.getAsInt("some.prefix.foobar.somekey", null), equalTo(17));
        assertNull(diff.get("some.prefix.foo.somekey"));
        assertEquals(diff.getAsList("foo.bar.quux", null), Arrays.asList("x", "y", "z"));
        assertEquals(diff.getAsList("foo.baz.quux", null), Arrays.asList("d", "e", "f"));
        assertThat(diff.getAsInt("foo.bar.baz", null), equalTo(1));
        assertThat(diff.getAsInt("foo.bar", null), equalTo(1));
    }

    public void testUpdateTracer() {
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AtomicReference<List<String>> ref = new AtomicReference<>();
        settings.addSettingsUpdateConsumer(TransportSettings.TRACE_LOG_INCLUDE_SETTING, ref::set);
        settings.applySettings(Settings.builder()
                .putList("transport.tracer.include", "internal:index/shard/recovery/*", "internal:gateway/local*").build());
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
            () -> settings.validate(Settings.builder().put("index.numbe_of_replica", "1").build(), false));
        assertEquals(iae.getMessage(), "unknown setting [index.numbe_of_replica] did you mean [index.number_of_replicas]?");
    }

    public void testValidate() {
        IndexScopedSettings settings = new IndexScopedSettings(
            Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        String unknownMsgSuffix = " please check that any required plugins are installed, or check the breaking changes documentation for" +
            " removed settings";
        settings.validate(Settings.builder().put("index.store.type", "boom").build(), false);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            settings.validate(Settings.builder().put("index.store.type", "boom").put("i.am.not.a.setting", true).build(), false));
        assertEquals("unknown setting [i.am.not.a.setting]" + unknownMsgSuffix, e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () ->
            settings.validate(Settings.builder().put("index.store.type", "boom").put("index.number_of_replicas", true).build(), false));
        assertEquals("Failed to parse value [true] for setting [index.number_of_replicas]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () ->
            settings.validate("index.number_of_replicas", Settings.builder().put("index.number_of_replicas", "true").build(), false));
        assertEquals("Failed to parse value [true] for setting [index.number_of_replicas]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () ->
            settings.validate("index.similarity.boolean.type", Settings.builder().put("index.similarity.boolean.type", "mine").build(),
                false));
        assertEquals("illegal value for [index.similarity.boolean] cannot redefine built-in similarity", e.getMessage());
    }

    public void testValidateSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("some.secure.setting", "secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, Collections.emptySet());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.validate(settings, false));
        assertThat(e.getMessage(), startsWith("unknown secure setting [some.secure.setting]"));

        ClusterSettings clusterSettings2 = new ClusterSettings(settings,
            Collections.singleton(SecureSetting.secureString("some.secure.setting", null)));
        clusterSettings2.validate(settings, false);
    }

    public void testDiffSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("some.secure.setting", "secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY,
            Collections.singleton(SecureSetting.secureString("some.secure.setting", null)));

        Settings diffed = clusterSettings.diff(Settings.EMPTY, settings);
        assertTrue(diffed.isEmpty());
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
        final Level level = LogManager.getRootLogger().getLevel();
        final Level testLevel = LogManager.getLogger("test").getLevel();
        Level property = randomFrom(Level.values());
        Settings.Builder builder = Settings.builder().put("logger.level", property);
        try {
            ClusterSettings settings = new ClusterSettings(builder.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            IllegalArgumentException ex =
                expectThrows(
                    IllegalArgumentException.class,
                    () -> settings.validate(Settings.builder().put("logger._root", "boom").build(), false));
            assertEquals("Unknown level constant [BOOM].", ex.getMessage());
            assertEquals(level, LogManager.getRootLogger().getLevel());
            settings.applySettings(Settings.builder().put("logger._root", "TRACE").build());
            assertEquals(Level.TRACE, LogManager.getRootLogger().getLevel());
            settings.applySettings(Settings.builder().build());
            assertEquals(property, LogManager.getRootLogger().getLevel());
            settings.applySettings(Settings.builder().put("logger.test", "TRACE").build());
            assertEquals(Level.TRACE, LogManager.getLogger("test").getLevel());
            settings.applySettings(Settings.builder().build());
            assertEquals(property, LogManager.getLogger("test").getLevel());
        } finally {
            Loggers.setLevel(LogManager.getRootLogger(), level);
            Loggers.setLevel(LogManager.getLogger("test"), testLevel);
        }
    }

    public void testFallbackToLoggerLevel() {
        final Level level = LogManager.getRootLogger().getLevel();
        try {
            ClusterSettings settings =
                new ClusterSettings(Settings.builder().put("logger.level", "ERROR").build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            assertEquals(level, LogManager.getRootLogger().getLevel());
            settings.applySettings(Settings.builder().put("logger._root", "TRACE").build());
            assertEquals(Level.TRACE, LogManager.getRootLogger().getLevel());
            settings.applySettings(Settings.builder().build()); // here we fall back to 'logger.level' which is our default.
            assertEquals(Level.ERROR, LogManager.getRootLogger().getLevel());
        } finally {
            Loggers.setLevel(LogManager.getRootLogger(), level);
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

    public void testUpdateNumberOfShardsFail() {
        IndexScopedSettings settings = new IndexScopedSettings(Settings.EMPTY,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> settings.updateSettings(Settings.builder().put("index.number_of_shards", 8).build(),
                Settings.builder(), Settings.builder(), "index"));
        assertThat(ex.getMessage(),
            containsString("final index setting [index.number_of_shards], not updateable"));
    }

    public void testFinalSettingUpdateFail() {
        Setting<Integer> finalSetting = Setting.intSetting("some.final.setting", 1, Property.Final, Property.NodeScope);
        Setting<Settings> finalGroupSetting = Setting.groupSetting("some.final.group.", Property.Final, Property.NodeScope);
        Settings currentSettings = Settings.builder()
            .put("some.final.setting", 9)
            .put("some.final.group.foo", 7)
            .build();
        ClusterSettings service = new ClusterSettings(currentSettings
            , new HashSet<>(Arrays.asList(finalSetting, finalGroupSetting)));

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () ->
            service.updateDynamicSettings(Settings.builder().put("some.final.setting", 8).build(),
                Settings.builder().put(currentSettings), Settings.builder(), "node"));
        assertThat(exc.getMessage(), containsString("final node setting [some.final.setting]"));

        exc = expectThrows(IllegalArgumentException.class, () ->
            service.updateDynamicSettings(Settings.builder().putNull("some.final.setting").build(),
                Settings.builder().put(currentSettings), Settings.builder(), "node"));
        assertThat(exc.getMessage(), containsString("final node setting [some.final.setting]"));

        exc = expectThrows(IllegalArgumentException.class, () ->
            service.updateSettings(Settings.builder().put("some.final.group.new", 8).build(),
                Settings.builder().put(currentSettings), Settings.builder(), "node"));
        assertThat(exc.getMessage(), containsString("final node setting [some.final.group.new]"));

        exc = expectThrows(IllegalArgumentException.class, () ->
            service.updateSettings(Settings.builder().put("some.final.group.foo", 5).build(),
                Settings.builder().put(currentSettings), Settings.builder(), "node"));
        assertThat(exc.getMessage(), containsString("final node setting [some.final.group.foo]"));
    }

    public void testInternalIndexSettingsFailsValidation() {
        final Setting<String> indexInternalSetting = Setting.simpleString("index.internal", Property.InternalIndex, Property.IndexScope);
        final IndexScopedSettings indexScopedSettings =
                new IndexScopedSettings(Settings.EMPTY, Collections.singleton(indexInternalSetting));
        final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> {
                    final Settings settings = Settings.builder().put("index.internal", "internal").build();
                    indexScopedSettings.validate(settings, false, /* validateInternalOrPrivateIndex */ true);
                });
        final String message = "can not update internal setting [index.internal]; this setting is managed via a dedicated API";
        assertThat(e, hasToString(containsString(message)));
    }

    public void testPrivateIndexSettingsFailsValidation() {
        final Setting<String> indexInternalSetting = Setting.simpleString("index.private", Property.PrivateIndex, Property.IndexScope);
        final IndexScopedSettings indexScopedSettings =
                new IndexScopedSettings(Settings.EMPTY, Collections.singleton(indexInternalSetting));
        final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> {
                    final Settings settings = Settings.builder().put("index.private", "private").build();
                    indexScopedSettings.validate(settings, false, /* validateInternalOrPrivateIndex */ true);
                });
        final String message = "can not update private setting [index.private]; this setting is managed by Elasticsearch";
        assertThat(e, hasToString(containsString(message)));
    }

    public void testInternalIndexSettingsSkipValidation() {
        final Setting<String> internalIndexSetting = Setting.simpleString("index.internal", Property.InternalIndex, Property.IndexScope);
        final IndexScopedSettings indexScopedSettings =
                new IndexScopedSettings(Settings.EMPTY, Collections.singleton(internalIndexSetting));
        // nothing should happen, validation should not throw an exception
        final Settings settings = Settings.builder().put("index.internal", "internal").build();
        indexScopedSettings.validate(settings, false, /* validateInternalOrPrivateIndex */ false);
    }

    public void testPrivateIndexSettingsSkipValidation() {
        final Setting<String> internalIndexSetting = Setting.simpleString("index.private", Property.PrivateIndex, Property.IndexScope);
        final IndexScopedSettings indexScopedSettings =
                new IndexScopedSettings(Settings.EMPTY, Collections.singleton(internalIndexSetting));
        // nothing should happen, validation should not throw an exception
        final Settings settings = Settings.builder().put("index.private", "private").build();
        indexScopedSettings.validate(settings, false, /* validateInternalOrPrivateIndex */ false);
    }

    public void testUpgradeSetting() {
        final Setting<String> oldSetting = Setting.simpleString("foo.old", Property.NodeScope);
        final Setting<String> newSetting = Setting.simpleString("foo.new", Property.NodeScope);
        final Setting<String> remainingSetting = Setting.simpleString("foo.remaining", Property.NodeScope);

        final AbstractScopedSettings service =
                new ClusterSettings(
                        Settings.EMPTY,
                        new HashSet<>(Arrays.asList(oldSetting, newSetting, remainingSetting)),
                        Collections.singleton(new SettingUpgrader<String>() {

                            @Override
                            public Setting<String> getSetting() {
                                return oldSetting;
                            }

                            @Override
                            public String getKey(final String key) {
                                return "foo.new";
                            }

                            @Override
                            public String getValue(final String value) {
                                return "new." + value;
                            }

                        }));

        final Settings settings =
                Settings.builder()
                        .put("foo.old", randomAlphaOfLength(8))
                        .put("foo.remaining", randomAlphaOfLength(8))
                        .build();
        final Settings upgradedSettings = service.upgradeSettings(settings);
        assertFalse(oldSetting.exists(upgradedSettings));
        assertTrue(newSetting.exists(upgradedSettings));
        assertThat(newSetting.get(upgradedSettings), equalTo("new." + oldSetting.get(settings)));
        assertTrue(remainingSetting.exists(upgradedSettings));
        assertThat(remainingSetting.get(upgradedSettings), equalTo(remainingSetting.get(settings)));
    }

    public void testUpgradeSettingsNoChangesPreservesInstance() {
        final Setting<String> oldSetting = Setting.simpleString("foo.old", Property.NodeScope);
        final Setting<String> newSetting = Setting.simpleString("foo.new", Property.NodeScope);
        final Setting<String> remainingSetting = Setting.simpleString("foo.remaining", Property.NodeScope);

        final AbstractScopedSettings service =
                new ClusterSettings(
                        Settings.EMPTY,
                        new HashSet<>(Arrays.asList(oldSetting, newSetting, remainingSetting)),
                        Collections.singleton(new SettingUpgrader<String>() {

                            @Override
                            public Setting<String> getSetting() {
                                return oldSetting;
                            }

                            @Override
                            public String getKey(final String key) {
                                return "foo.new";
                            }

                        }));

        final Settings settings = Settings.builder().put("foo.remaining", randomAlphaOfLength(8)).build();
        final Settings upgradedSettings = service.upgradeSettings(settings);
        assertThat(upgradedSettings, sameInstance(settings));
    }

    public void testUpgradeComplexSetting() {
        final Setting.AffixSetting<String> oldSetting =
                Setting.affixKeySetting("foo.old.", "suffix", key -> Setting.simpleString(key, Property.NodeScope));
        final Setting.AffixSetting<String> newSetting =
                Setting.affixKeySetting("foo.new.", "suffix", key -> Setting.simpleString(key, Property.NodeScope));
        final Setting.AffixSetting<String> remainingSetting =
                Setting.affixKeySetting("foo.remaining.", "suffix", key -> Setting.simpleString(key, Property.NodeScope));

        final AbstractScopedSettings service =
                new ClusterSettings(
                        Settings.EMPTY,
                        new HashSet<>(Arrays.asList(oldSetting, newSetting, remainingSetting)),
                        Collections.singleton(new SettingUpgrader<String>() {

                            @Override
                            public Setting<String> getSetting() {
                                return oldSetting;
                            }

                            @Override
                            public String getKey(final String key) {
                                return key.replaceFirst("^foo\\.old", "foo\\.new");
                            }

                            @Override
                            public String getValue(final String value) {
                                return "new." + value;
                            }

                        }));

        final int count = randomIntBetween(1, 8);
        final List<String> concretes = new ArrayList<>(count);
        final Settings.Builder builder = Settings.builder();
        for (int i = 0; i < count; i++) {
            final String concrete = randomAlphaOfLength(8);
            concretes.add(concrete);
            builder.put("foo.old." + concrete + ".suffix", randomAlphaOfLength(8));
            builder.put("foo.remaining." + concrete + ".suffix", randomAlphaOfLength(8));
        }
        final Settings settings = builder.build();
        final Settings upgradedSettings = service.upgradeSettings(settings);
        for (final String concrete : concretes) {
            assertFalse(oldSetting.getConcreteSettingForNamespace(concrete).exists(upgradedSettings));
            assertTrue(newSetting.getConcreteSettingForNamespace(concrete).exists(upgradedSettings));
            assertThat(
                    newSetting.getConcreteSettingForNamespace(concrete).get(upgradedSettings),
                    equalTo("new." + oldSetting.getConcreteSettingForNamespace(concrete).get(settings)));
            assertTrue(remainingSetting.getConcreteSettingForNamespace(concrete).exists(upgradedSettings));
            assertThat(
                    remainingSetting.getConcreteSettingForNamespace(concrete).get(upgradedSettings),
                    equalTo(remainingSetting.getConcreteSettingForNamespace(concrete).get(settings)));
        }
    }

    public void testUpgradeListSetting() {
        final Setting<List<String>> oldSetting =
                Setting.listSetting("foo.old", Collections.emptyList(), Function.identity(), Property.NodeScope);
        final Setting<List<String>> newSetting =
                Setting.listSetting("foo.new", Collections.emptyList(), Function.identity(), Property.NodeScope);

        final AbstractScopedSettings service =
                new ClusterSettings(
                        Settings.EMPTY,
                        new HashSet<>(Arrays.asList(oldSetting, newSetting)),
                        Collections.singleton(new SettingUpgrader<List<String>>() {

                            @Override
                            public Setting<List<String>> getSetting() {
                                return oldSetting;
                            }

                            @Override
                            public String getKey(final String key) {
                                return "foo.new";
                            }

                            @Override
                            public List<String> getListValue(final List<String> value) {
                                return value.stream().map(s -> "new." + s).collect(Collectors.toList());
                            }
                        }));

        final int length = randomIntBetween(0, 16);
        final List<String> values = length == 0 ? Collections.emptyList() : new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            values.add(randomAlphaOfLength(8));
        }

        final Settings settings = Settings.builder().putList("foo.old", values).build();
        final Settings upgradedSettings = service.upgradeSettings(settings);
        assertFalse(oldSetting.exists(upgradedSettings));
        assertTrue(newSetting.exists(upgradedSettings));
        assertThat(
                newSetting.get(upgradedSettings),
                equalTo(oldSetting.get(settings).stream().map(s -> "new." + s).collect(Collectors.toList())));
    }

}
