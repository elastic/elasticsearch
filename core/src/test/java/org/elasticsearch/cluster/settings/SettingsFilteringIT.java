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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = SUITE, numDataNodes = 1)
public class SettingsFilteringIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(SettingsFilteringPlugin.class);
    }

    public static class SettingsFilteringPlugin extends Plugin {
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
        public Collection<Module> indexModules(Settings indexSettings) {
            return Collections.<Module>singletonList(new SettingsFilteringModule());
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

        assertAcked(client().admin().indices().prepareCreate("test-idx").setSettings(Settings.builder()
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
