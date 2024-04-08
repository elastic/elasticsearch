/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ClusterScope(scope = SUITE, supportsDedicatedMasters = false, numDataNodes = 1, numClientNodes = 0)
public class SettingsListenerIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(SettingsListenerPlugin.class);
    }

    public static class SettingsListenerPlugin extends Plugin {
        private final SettingsTestingService service = new SettingsTestingService();

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(SettingsTestingService.VALUE);
        }

        @Override
        public void onIndexModule(IndexModule module) {
            if (module.getIndex().getName().equals("test")) { // only for the test index
                module.addSettingsUpdateConsumer(SettingsTestingService.VALUE, service::setValue);
                service.setValue(SettingsTestingService.VALUE.get(module.getSettings()));
            }
        }

        @Override
        public Collection<?> createComponents(PluginServices services) {
            return Collections.singletonList(service);
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

    public static class SettingsTestingService {
        public volatile int value;
        public static Setting<Integer> VALUE = Setting.intSetting("index.test.new.setting", -1, -1, Property.Dynamic, Property.IndexScope);

        public void setValue(int value) {
            this.value = value;
        }

    }

    public void testListener() {
        assertAcked(indicesAdmin().prepareCreate("test").setSettings(Settings.builder().put("index.test.new.setting", 21).build()).get());

        for (SettingsTestingService instance : internalCluster().getDataNodeInstances(SettingsTestingService.class)) {
            assertEquals(21, instance.value);
        }

        updateIndexSettings(Settings.builder().put("index.test.new.setting", 42), "test");
        for (SettingsTestingService instance : internalCluster().getDataNodeInstances(SettingsTestingService.class)) {
            assertEquals(42, instance.value);
        }

        assertAcked(indicesAdmin().prepareCreate("other").setSettings(Settings.builder().put("index.test.new.setting", 21).build()).get());

        for (SettingsTestingService instance : internalCluster().getDataNodeInstances(SettingsTestingService.class)) {
            assertEquals(42, instance.value);
        }

        updateIndexSettings(Settings.builder().put("index.test.new.setting", 84), "other");
        for (SettingsTestingService instance : internalCluster().getDataNodeInstances(SettingsTestingService.class)) {
            assertEquals(42, instance.value);
        }

        try {
            indicesAdmin().prepareUpdateSettings("other").setSettings(Settings.builder().put("index.test.new.setting", -5)).get();
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [-5] for setting [index.test.new.setting] must be >= -1", ex.getMessage());
        }
    }
}
