/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;

public class MockPluginsServiceTests extends ESTestCase {

    public static class TestPlugin1 extends Plugin {

        public TestPlugin1() {};

        // for map/flatmap/foreach testing
        @Override
        public List<String> getSettingsFilter() {
            return List.of("test value 1");
        }

    }

    public static class TestPlugin2 extends Plugin {

        public TestPlugin2() {};

        // for map/flatmap/foreach testing
        @Override
        public List<String> getSettingsFilter() {
            return List.of("test value 2");
        }
    }

    private MockPluginsService mockPluginsService;

    @Before
    public void setup() {
        List<Class<? extends Plugin>> classpathPlugins = List.of(TestPlugin1.class, TestPlugin2.class);
        Settings pathHomeSetting = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        this.mockPluginsService = new MockPluginsService(
            pathHomeSetting,
            TestEnvironment.newEnvironment(pathHomeSetting),
            classpathPlugins
        );

    }

    public void testSuperclassMethods() {
        List<List<String>> mapResult = mockPluginsService.map(Plugin::getSettingsFilter).toList();
        assertThat(mapResult, containsInAnyOrder(List.of("test value 1"), List.of("test value 2")));

        List<String> flatMapResult = mockPluginsService.flatMap(Plugin::getSettingsFilter).toList();
        assertThat(flatMapResult, containsInAnyOrder("test value 1", "test value 2"));

        List<String> forEachCollector = new ArrayList<>();
        mockPluginsService.forEach(p -> forEachCollector.addAll(p.getSettingsFilter()));
        assertThat(forEachCollector, containsInAnyOrder("test value 1", "test value 2"));

        Map<String, Plugin> pluginMap = mockPluginsService.pluginMap();
        assertThat(pluginMap.keySet(), containsInAnyOrder(containsString("TestPlugin1"), containsString("TestPlugin2")));

        List<TestPlugin1> plugin1 = mockPluginsService.filterPlugins(TestPlugin1.class);
        assertThat(plugin1, contains(instanceOf(TestPlugin1.class)));
    }

    public void testInfo() {
        PluginsAndModules pam = this.mockPluginsService.info();

        assertThat(pam.getModuleInfos(), empty());

        List<String> pluginNames = pam.getPluginInfos().stream().map(PluginRuntimeInfo::descriptor).map(PluginDescriptor::getName).toList();
        assertThat(pluginNames, containsInAnyOrder(containsString("TestPlugin1"), containsString("TestPlugin2")));
    }
}
