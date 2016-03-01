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

package org.elasticsearch.tribe;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

public class TribeServiceTests extends ESTestCase {
    public void testMinimalSettings() {
        Settings globalSettings = Settings.builder()
            .put("node.name", "nodename")
            .put("path.home", "some/path").build();
        Settings clientSettings = TribeService.buildClientSettings("tribe1", globalSettings, Settings.EMPTY);
        assertEquals("some/path", clientSettings.get("path.home"));
        assertEquals("nodename/tribe1", clientSettings.get("node.name"));
        assertEquals("tribe1", clientSettings.get("tribe.name"));
        assertEquals("false", clientSettings.get("http.enabled"));
        assertEquals("true", clientSettings.get("node.client"));
        assertEquals(5, clientSettings.getAsMap().size());
    }

    public void testEnvironmentSettings() {
        Settings globalSettings = Settings.builder()
            .put("node.name", "nodename")
            .put("path.home", "some/path")
            .put("path.conf", "conf/path")
            .put("path.plugins", "plugins/path")
            .put("path.logs", "logs/path").build();
        Settings clientSettings = TribeService.buildClientSettings("tribe1", globalSettings, Settings.EMPTY);
        assertEquals("some/path", clientSettings.get("path.home"));
        assertEquals("conf/path", clientSettings.get("path.conf"));
        assertEquals("plugins/path", clientSettings.get("path.plugins"));
        assertEquals("logs/path", clientSettings.get("path.logs"));
    }

    public void testPassthroughSettings() {
        Settings globalSettings = Settings.builder()
            .put("node.name", "nodename")
            .put("path.home", "some/path")
            .put("network.host", "0.0.0.0")
            .put("network.bind_host", "1.1.1.1")
            .put("network.publish_host", "2.2.2.2").build();
        Settings clientSettings = TribeService.buildClientSettings("tribe1", globalSettings, Settings.EMPTY);
        assertEquals("0.0.0.0", clientSettings.get("network.host"));
        assertEquals("1.1.1.1", clientSettings.get("network.bind_host"));
        assertEquals("2.2.2.2", clientSettings.get("network.publish_host"));
    }
}
