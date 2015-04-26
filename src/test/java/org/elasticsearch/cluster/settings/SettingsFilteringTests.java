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

package org.elasticsearch.cluster.settings;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = SUITE, numDataNodes = 1)
public class SettingsFilteringTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", SettingsFilteringPlugin.class.getName())
                .build();
    }

    public static class SettingsFilteringPlugin extends AbstractPlugin {
        /**
         * The name of the plugin.
         */
        @Override
        public String name() {
            return "settings-filtering";
        }

        /**
         * The description of the plugin.
         */
        @Override
        public String description() {
            return "Settings Filtering Plugin";
        }

        @Override
        public Collection<Class<? extends Module>> indexModules() {
            Collection<Class<? extends Module>> modules = newArrayList();
            modules.add(SettingsFilteringModule.class);
            return modules;
        }
    }

    public static class SettingsFilteringModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(SettingsFilteringService.class).asEagerSingleton();
        }
    }

    public static class SettingsFilteringService {
        @Inject
        public SettingsFilteringService(SettingsFilter settingsFilter) {
            settingsFilter.addFilter("index.filter_test.foo");
            settingsFilter.addFilter("index.filter_test.bar*");
        }
    }


    @Test
    public void testSettingsFiltering() {

        assertAcked(client().admin().indices().prepareCreate("test-idx").setSettings(ImmutableSettings.builder()
                .put("filter_test.foo", "test")
                .put("filter_test.bar1", "test")
                .put("filter_test.bar2", "test")
                .put("filter_test.notbar", "test")
                .put("filter_test.notfoo", "test")
                .build()).get());
        GetSettingsResponse response = client().admin().indices().prepareGetSettings("test-idx").get();
        Settings settings = response.getIndexToSettings().get("test-idx");

        assertThat(settings.get("index.filter_test.foo"), nullValue());
        assertThat(settings.get("index.filter_test.bar1"), nullValue());
        assertThat(settings.get("index.filter_test.bar2"), nullValue());
        assertThat(settings.get("index.filter_test.notbar"), equalTo("test"));
        assertThat(settings.get("index.filter_test.notfoo"), equalTo("test"));
    }

}
