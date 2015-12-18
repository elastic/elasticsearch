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

import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ScopedSettingsTests extends ESTestCase {

    public void testAddConsumer() {
        Setting<Integer> testSetting = Setting.intSetting("foo.bar", 1, true, Setting.Scope.CLUSTER);
        Setting<Integer> testSetting2 = Setting.intSetting("foo.bar.baz", 1, true, Setting.Scope.CLUSTER);
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

    public void testApply() {
        Setting<Integer> testSetting = Setting.intSetting("foo.bar", 1, true, Setting.Scope.CLUSTER);
        Setting<Integer> testSetting2 = Setting.intSetting("foo.bar.baz", 1, true, Setting.Scope.CLUSTER);
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
            service.dryRun(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", -15).build());
            fail("invalid value");
        } catch (IllegalArgumentException ex) {
            assertEquals("illegal value can't update [foo.bar.baz] from [1] to [-15]", ex.getMessage());
        }

        assertEquals(0, consumer.get());
        assertEquals(0, consumer2.get());
        assertEquals(0, aC.get());
        assertEquals(0, bC.get());
        service.dryRun(Settings.builder().put("foo.bar", 2).put("foo.bar.baz", 15).build());
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
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, new HashSet<>(Arrays.asList(Setting.intSetting("foo.bar", 1, true, Setting.Scope.CLUSTER), Setting.intSetting("foo.bar.baz", 1, false, Setting.Scope.CLUSTER))));
        assertFalse(settings.hasDynamicSetting("foo.bar.baz"));
        assertTrue(settings.hasDynamicSetting("foo.bar"));
        assertNotNull(settings.get("foo.bar.baz"));
        settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        assertTrue(settings.hasDynamicSetting("transport.tracer.include." + randomIntBetween(1, 100)));
        assertFalse(settings.hasDynamicSetting("transport.tracer.include.BOOM"));
        assertTrue(settings.hasDynamicSetting("cluster.routing.allocation.require.value"));
    }

    public void testDiff() throws IOException {
        Setting<Integer> foobarbaz = Setting.intSetting("foo.bar.baz", 1, false, Setting.Scope.CLUSTER);
        Setting<Integer> foobar = Setting.intSetting("foo.bar", 1, true, Setting.Scope.CLUSTER);
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, new HashSet<>(Arrays.asList(foobar, foobarbaz)));
        Settings diff = settings.diff(Settings.builder().put("foo.bar", 5).build(), Settings.EMPTY);
        assertEquals(diff.getAsMap().size(), 1);
        assertEquals(diff.getAsInt("foo.bar.baz", null), Integer.valueOf(1));

        diff = settings.diff(Settings.builder().put("foo.bar", 5).build(), Settings.builder().put("foo.bar.baz", 17).build());
        assertEquals(diff.getAsMap().size(), 1);
        assertEquals(diff.getAsInt("foo.bar.baz", null), Integer.valueOf(17));
    }

    public void testUpdateTracer() {
        ClusterSettings settings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AtomicReference<List<String>> ref = new AtomicReference<>();
        settings.addSettingsUpdateConsumer(TransportService.TRACE_LOG_INCLUDE_SETTING, ref::set);
        settings.applySettings(Settings.builder().putArray("transport.tracer.include", "internal:index/shard/recovery/*", "internal:gateway/local*").build());
        assertNotNull(ref.get().size());
        assertEquals(ref.get().size(), 2);
        assertTrue(ref.get().contains("internal:index/shard/recovery/*"));
        assertTrue(ref.get().contains("internal:gateway/local*"));
    }
}
