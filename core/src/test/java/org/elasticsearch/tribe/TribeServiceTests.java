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
        assertEquals("false", clientSettings.get("node.master"));
        assertEquals("false", clientSettings.get("node.data"));
        assertEquals("false", clientSettings.get("node.ingest"));
        assertEquals(7, clientSettings.getAsMap().size());
    }

    public void testEnvironmentSettings() {
        Settings globalSettings = Settings.builder()
            .put("node.name", "nodename")
            .put("path.home", "some/path")
            .put("path.conf", "conf/path")
            .put("path.scripts", "scripts/path")
            .put("path.logs", "logs/path").build();
        Settings clientSettings = TribeService.buildClientSettings("tribe1", globalSettings, Settings.EMPTY);
        assertEquals("some/path", clientSettings.get("path.home"));
        assertEquals("conf/path", clientSettings.get("path.conf"));
        assertEquals("scripts/path", clientSettings.get("path.scripts"));
        assertEquals("logs/path", clientSettings.get("path.logs"));

        Settings tribeSettings = Settings.builder()
            .put("path.home", "alternate/path").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            TribeService.buildClientSettings("tribe1", globalSettings, tribeSettings);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Setting [path.home] not allowed in tribe client"));
    }

    public void testPassthroughSettings() {
        Settings globalSettings = Settings.builder()
            .put("node.name", "nodename")
            .put("path.home", "some/path")
            .put("network.host", "0.0.0.0")
            .put("network.bind_host", "1.1.1.1")
            .put("network.publish_host", "2.2.2.2")
            .put("transport.host", "3.3.3.3")
            .put("transport.bind_host", "4.4.4.4")
            .put("transport.publish_host", "5.5.5.5").build();
        Settings clientSettings = TribeService.buildClientSettings("tribe1", globalSettings, Settings.EMPTY);
        assertEquals("0.0.0.0", clientSettings.get("network.host"));
        assertEquals("1.1.1.1", clientSettings.get("network.bind_host"));
        assertEquals("2.2.2.2", clientSettings.get("network.publish_host"));
        assertEquals("3.3.3.3", clientSettings.get("transport.host"));
        assertEquals("4.4.4.4", clientSettings.get("transport.bind_host"));
        assertEquals("5.5.5.5", clientSettings.get("transport.publish_host"));

        // per tribe client overrides still work
        Settings tribeSettings = Settings.builder()
            .put("network.host", "3.3.3.3")
            .put("network.bind_host", "4.4.4.4")
            .put("network.publish_host", "5.5.5.5")
            .put("transport.host", "6.6.6.6")
            .put("transport.bind_host", "7.7.7.7")
            .put("transport.publish_host", "8.8.8.8").build();
        clientSettings = TribeService.buildClientSettings("tribe1", globalSettings, tribeSettings);
        assertEquals("3.3.3.3", clientSettings.get("network.host"));
        assertEquals("4.4.4.4", clientSettings.get("network.bind_host"));
        assertEquals("5.5.5.5", clientSettings.get("network.publish_host"));
        assertEquals("6.6.6.6", clientSettings.get("transport.host"));
        assertEquals("7.7.7.7", clientSettings.get("transport.bind_host"));
        assertEquals("8.8.8.8", clientSettings.get("transport.publish_host"));
    }
}
