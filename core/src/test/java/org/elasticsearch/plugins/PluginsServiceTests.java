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

package org.elasticsearch.plugins;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class PluginsServiceTests extends ESTestCase {
    public static class AdditionalSettingsPlugin1 extends Plugin {
        @Override
        public String name() {
            return "additional-settings1";
        }
        @Override
        public String description() {
            return "adds additional setting 'foo.bar'";
        }
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("foo.bar", "1").put(IndexStoreModule.STORE_TYPE, IndexStoreModule.Type.MMAPFS.getSettingsKey()).build();
        }
    }
    public static class AdditionalSettingsPlugin2 extends Plugin {
        @Override
        public String name() {
            return "additional-settings2";
        }
        @Override
        public String description() {
            return "adds additional setting 'foo.bar'";
        }
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("foo.bar", "2").build();
        }
    }

    static PluginsService newPluginsService(Settings settings, Class<? extends Plugin>... classpathPlugins) {
        return new PluginsService(settings, new Environment(settings).pluginsFile(), Arrays.asList(classpathPlugins));
    }

    public void testAdditionalSettings() {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("my.setting", "test")
            .put(IndexStoreModule.STORE_TYPE, IndexStoreModule.Type.SIMPLEFS.getSettingsKey()).build();
        PluginsService service = newPluginsService(settings, AdditionalSettingsPlugin1.class);
        Settings newSettings = service.updatedSettings();
        assertEquals("test", newSettings.get("my.setting")); // previous settings still exist
        assertEquals("1", newSettings.get("foo.bar")); // added setting exists
        assertEquals(IndexStoreModule.Type.SIMPLEFS.getSettingsKey(), newSettings.get(IndexStoreModule.STORE_TYPE)); // does not override pre existing settings
    }

    public void testAdditionalSettingsClash() {
        Settings settings = Settings.builder()
            .put("path.home", createTempDir()).build();
        PluginsService service = newPluginsService(settings, AdditionalSettingsPlugin1.class, AdditionalSettingsPlugin2.class);
        try {
            service.updatedSettings();
            fail("Expected exception when building updated settings");
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg, msg.contains("Cannot have additional setting [foo.bar]"));
            assertTrue(msg, msg.contains("plugin [additional-settings1]"));
            assertTrue(msg, msg.contains("plugin [additional-settings2]"));
        }
    }
}
