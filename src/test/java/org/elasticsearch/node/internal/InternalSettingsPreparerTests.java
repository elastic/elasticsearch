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

package org.elasticsearch.node.internal;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class InternalSettingsPreparerTests extends ElasticsearchTestCase {
    @Before
    public void setupSystemProperties() {
        System.setProperty("es.node.zone", "foo");
    }

    @After
    public void cleanupSystemProperties() {
        System.clearProperty("es.node.zone");
    }

    @Test
    public void testIgnoreSystemProperties() {
        Tuple<Settings, Environment> tuple = InternalSettingsPreparer.prepareSettings(settingsBuilder().put("node.zone", "bar").build(), true);
        // Should use setting from the system property
        assertThat(tuple.v1().get("node.zone"), equalTo("foo"));

        tuple = InternalSettingsPreparer.prepareSettings(settingsBuilder().put("config.ignore_system_properties", true).put("node.zone", "bar").build(), true);
        // Should use setting from the system property
        assertThat(tuple.v1().get("node.zone"), equalTo("bar"));
    }
}
