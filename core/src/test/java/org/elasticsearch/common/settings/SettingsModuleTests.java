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
import org.elasticsearch.common.settings.Setting.Property;
import org.joda.time.MonthDay;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SettingsModuleTests extends ModuleTestCase {

    public void testValidate() {
        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "2.0").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "[2.0]").build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () ->  new SettingsModule(settings));
            assertEquals("Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]", ex.getMessage());
        }

        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "[2.0]")
                .put("some.foo.bar", 1).build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> new SettingsModule(settings));
            assertEquals("Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]", ex.getMessage());
            assertEquals(1, ex.getSuppressed().length);
            assertEquals("unknown setting [some.foo.bar]", ex.getSuppressed()[0].getMessage());
        }

        {
            Settings settings = Settings.builder().put("index.codec", "default")
                .put("index.foo.bar", 1).build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> new SettingsModule(settings));
            assertEquals("node settings must not contain any index level settings", ex.getMessage());
        }

        {
            Settings settings = Settings.builder().put("index.codec", "default").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
    }

    public void testRegisterSettings() {
        {
            Settings settings = Settings.builder().put("some.custom.setting", "2.0").build();
            SettingsModule module = new SettingsModule(settings, Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope));
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("some.custom.setting", "false").build();
            try {
                new SettingsModule(settings, Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope));
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
            try {
                new SettingsModule(settings);
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals(
                        "tribe.t1 validation failed: Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]",
                        ex.getMessage());
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
            try {
                new SettingsModule(settings);
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("Failed to parse value [BOOM] cannot be parsed to boolean [ true/1/on/yes OR false/0/off/no ]",
                        ex.getMessage());
            }
        }
        {
            Settings settings = Settings.builder().put("tribe.blocks.wtf", "BOOM").build();
            try {
                new SettingsModule(settings);
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
            try {
                new SettingsModule(settings);
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("No enum constant org.elasticsearch.common.logging.ESLoggerFactory.LogLevel.BOOM", ex.getMessage());
            }
        }

    }

    public void testRegisterSettingsFilter() {
        Settings settings = Settings.builder().put("foo.bar", "false").put("bar.foo", false).put("bar.baz", false).build();
        try {
            new SettingsModule(settings, Arrays.asList(Setting.boolSetting("foo.bar", true, Property.NodeScope),
            Setting.boolSetting("bar.foo", true, Property.NodeScope, Property.Filtered),
            Setting.boolSetting("bar.baz", true, Property.NodeScope)), Arrays.asList("foo.*", "bar.foo"));
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("filter [bar.foo] has already been registered", ex.getMessage());
        }
        SettingsModule module = new SettingsModule(settings, Arrays.asList(Setting.boolSetting("foo.bar", true, Property.NodeScope),
            Setting.boolSetting("bar.foo", true, Property.NodeScope, Property.Filtered),
            Setting.boolSetting("bar.baz", true, Property.NodeScope)), Arrays.asList("foo.*"));
        assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).getAsMap().size() == 1);
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).getAsMap().containsKey("bar.baz"));
        assertInstanceBinding(module, SettingsFilter.class, (s) -> s.filter(settings).getAsMap().get("bar.baz").equals("false"));

    }

    public void testMutuallyExclusiveScopes() {
        new SettingsModule(Settings.EMPTY, Setting.simpleString("foo.bar", Property.NodeScope));
        new SettingsModule(Settings.EMPTY, Setting.simpleString("index.foo.bar", Property.IndexScope));

        // Those should fail
        try {
            new SettingsModule(Settings.EMPTY, Setting.simpleString("foo.bar"));
            fail("No scope should fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("No scope found for setting"));
        }
        // Some settings have both scopes - that's fine too if they have per-node defaults
        try {
            new SettingsModule(Settings.EMPTY,
                Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope),
                Setting.simpleString("foo.bar", Property.NodeScope));
            fail("already registered");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot register setting [foo.bar] twice"));
        }

        try {
            new SettingsModule(Settings.EMPTY,
                Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope),
                Setting.simpleString("foo.bar", Property.IndexScope));
            fail("already registered");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot register setting [foo.bar] twice"));
        }
    }

    public void testOldMaxClauseCountSetting() {
            Settings settings = Settings.builder().put("index.query.bool.max_clause_count", 1024).build();
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> new SettingsModule(settings));
            assertEquals("unknown setting [index.query.bool.max_clause_count] did you mean [indices.query.bool.max_clause_count]?",
                ex.getMessage());
    }

    public void testRegisterShared() {
        Property scope = randomFrom(Property.NodeScope, Property.IndexScope);
        expectThrows(IllegalArgumentException.class, () ->
            new SettingsModule(Settings.EMPTY,
                Setting.simpleString("index.foo.bar", scope), Setting.simpleString("index.foo.bar", scope))
        );
        expectThrows(IllegalArgumentException.class, () ->
            new SettingsModule(Settings.EMPTY,
                Setting.simpleString("index.foo.bar", scope, Property.Shared), Setting.simpleString("index.foo.bar", scope))
        );
        expectThrows(IllegalArgumentException.class, () ->
            new SettingsModule(Settings.EMPTY,
                Setting.simpleString("index.foo.bar", scope), Setting.simpleString("index.foo.bar", scope, Property.Shared))
        );
        new SettingsModule(Settings.EMPTY,
            Setting.simpleString("index.foo.bar", scope, Property.Shared),
            Setting.simpleString("index.foo.bar", scope, Property.Shared));
    }
}
