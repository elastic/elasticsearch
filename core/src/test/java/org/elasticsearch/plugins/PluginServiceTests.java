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

import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class PluginServiceTests extends ElasticsearchTestCase {

    void writeProperties(Path pluginDir, String... stringProps) throws IOException {
        assert stringProps.length % 2 == 0;
        Path propertiesFile = pluginDir.resolve(PluginsService.ES_PLUGIN_PROPERTIES);
        Properties properties =  new Properties();
        for (int i = 0; i < stringProps.length; i += 2) {
            properties.put(stringProps[i], stringProps[i + 1]);
        }
        try (OutputStream out = Files.newOutputStream(propertiesFile)) {
            properties.store(out, "");
        }
    }

    public void testMetadata() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        Files.createDirectories(pluginDir);
        writeProperties(pluginDir,
            "description", "fake desc",
            "version", "1.0",
            "jvm", "true",
            "plugin", "FakePlugin");
        PluginInfo info = PluginsService.readMetadata(pluginDir);
        assertEquals("fake-plugin", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals("1.0", info.getVersion());
        assertEquals("FakePlugin", info.getClassname());
        assertTrue(info.isJvm());
        assertFalse(info.isIsolated()); // TODO: isolated was not specified, default should be true
        assertFalse(info.isSite());
        assertNull(info.getUrl());

    }
}
