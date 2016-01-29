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

import org.elasticsearch.common.inject.ModuleTestCase;

public class SettingsModuleTests extends ModuleTestCase {

    public void testValidate() {
        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "2.0").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "[2.0]").build();
            SettingsModule module = new SettingsModule(settings);
            try {
                assertInstanceBinding(module, Settings.class, (s) -> s == settings);
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]", ex.getMessage());
            }
        }
    }

    public void testRegisterSettings() {
        {
            Settings settings = Settings.builder().put("some.custom.setting", "2.0").build();
            SettingsModule module = new SettingsModule(settings);
            module.registerSetting(Setting.floatSetting("some.custom.setting", 1.0f, false, Setting.Scope.CLUSTER));
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("some.custom.setting", "false").build();
            SettingsModule module = new SettingsModule(settings);
            module.registerSetting(Setting.floatSetting("some.custom.setting", 1.0f, false, Setting.Scope.CLUSTER));
            try {
                assertInstanceBinding(module, Settings.class, (s) -> s == settings);
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("Failed to parse value [false] for setting [some.custom.setting]", ex.getMessage());
            }
        }
    }

    public void testTribeSetting() {
        {
            Settings settings = Settings.builder().put("tribe.t1.cluster.routing.allocation.balance.shard", "2.0").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("tribe.t1.cluster.routing.allocation.balance.shard", "[2.0]").build();
            SettingsModule module = new SettingsModule(settings);
            try {
                assertInstanceBinding(module, Settings.class, (s) -> s == settings);
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("tribe.t1 validation failed: Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]", ex.getMessage());
            }
        }
    }

    public void testSpecialTribeSetting() {
        {
            Settings settings = Settings.builder().put("tribe.blocks.write", "false").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("tribe.blocks.write", "BOOM").build();
            SettingsModule module = new SettingsModule(settings);
            try {
                assertInstanceBinding(module, Settings.class, (s) -> s == settings);
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("Failed to parse value [BOOM] cannot be parsed to boolean [ true/1/on/yes OR false/0/off/no ]", ex.getMessage());
            }
        }
        {
            Settings settings = Settings.builder().put("tribe.blocks.wtf", "BOOM").build();
            SettingsModule module = new SettingsModule(settings);
            try {
                assertInstanceBinding(module, Settings.class, (s) -> s == settings);
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("tribe.blocks validation failed: unknown setting [wtf]", ex.getMessage());
            }
        }
    }


    public void testLoggerSettings() {
        {
            Settings settings = Settings.builder().put("logger._root", "TRACE").put("logger.transport", "INFO").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }

        {
            Settings settings = Settings.builder().put("logger._root", "BOOM").put("logger.transport", "WOW").build();
            SettingsModule module = new SettingsModule(settings);
            try {
                assertInstanceBinding(module, Settings.class, (s) -> s == settings);
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("No enum constant org.elasticsearch.common.logging.ESLoggerFactory.LogLevel.BOOM", ex.getMessage());
            }
        }

    }

    public void testRegisterSettingsFilter() {
        Settings settings = Settings.builder().put("foo.bar", "false").put("bar.foo", false).put("bar.baz", false).build();
        SettingsModule module = new SettingsModule(settings);
        module.registerSetting(Setting.boolSetting("foo.bar", true, false, Setting.Scope.CLUSTER));
        module.registerSetting(Setting.boolSetting("bar.foo", true, false, Setting.Scope.CLUSTER, true));
        module.registerSetting(Setting.boolSetting("bar.baz", true, false, Setting.Scope.CLUSTER));

        module.registerSettingsFilter("foo.*");
        try {
            module.registerSettingsFilter("bar.foo");
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("filter [bar.foo] has already been registered", ex.getMessage());
        }
        assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).getAsMap().size() == 1);
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).getAsMap().containsKey("bar.baz"));
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).getAsMap().get("bar.baz").equals("false"));

    }
}
