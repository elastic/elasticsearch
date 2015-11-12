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
package org.elasticsearch.index;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0)
public class SettingsListenerIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(SettingsListenerPlugin.class);
    }

    public static class SettingsListenerPlugin extends Plugin {
        private final SettingsTestingService service = new SettingsTestingService();

        /**
         * The name of the plugin.
         */
        @Override
        public String name() {
            return "settings-listener";
        }

        /**
         * The description of the plugin.
         */
        @Override
        public String description() {
            return "Settings Listenern Plugin";
        }

        public void onModule(ClusterModule clusterModule) {
            clusterModule.registerIndexDynamicSetting("index.test.new.setting", Validator.INTEGER);
        }

        @Override
        public void onIndexModule(IndexModule module) {
            if (module.getIndex().getName().equals("test")) { // only for the test index
                module.addIndexSettingsListener(service);
                service.accept(module.getSettings());
            }
        }

        @Override
        public Collection<Module> nodeModules() {
            return Collections.<Module>singletonList(new SettingsListenerModule(service));
        }
    }

    public static class SettingsListenerModule extends AbstractModule {
        private final SettingsTestingService service;

        public SettingsListenerModule(SettingsTestingService service) {
            this.service = service;
        }

        @Override
        protected void configure() {
            bind(SettingsTestingService.class).toInstance(service);
        }
    }

    public static class SettingsTestingService implements Consumer<Settings> {
        public volatile int value;

        @Override
        public void accept(Settings settings) {
            value = settings.getAsInt("index.test.new.setting", -1);
        }
    }

    public void testListener() {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
                .put("index.test.new.setting", 21)
                .build()).get());

        for (SettingsTestingService instance : internalCluster().getInstances(SettingsTestingService.class)) {
            assertEquals(21, instance.value);
        }

        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
                .put("index.test.new.setting", 42)).get();
        for (SettingsTestingService instance : internalCluster().getInstances(SettingsTestingService.class)) {
            assertEquals(42, instance.value);
        }

        assertAcked(client().admin().indices().prepareCreate("other").setSettings(Settings.builder()
                .put("index.test.new.setting", 21)
                .build()).get());

        for (SettingsTestingService instance : internalCluster().getInstances(SettingsTestingService.class)) {
            assertEquals(42, instance.value);
        }

        client().admin().indices().prepareUpdateSettings("other").setSettings(Settings.builder()
                .put("index.test.new.setting", 84)).get();

        for (SettingsTestingService instance : internalCluster().getInstances(SettingsTestingService.class)) {
            assertEquals(42, instance.value);
        }
    }
}
