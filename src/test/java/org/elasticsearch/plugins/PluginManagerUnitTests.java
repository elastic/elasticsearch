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

import com.google.common.io.Files;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class PluginManagerUnitTests extends ElasticsearchTestCase {

    @Test
    public void testThatConfigDirectoryCanBeOutsideOfElasticsearchHomeDirectory() throws IOException {
        String pluginName = randomAsciiOfLength(10);
        File homeFolder = newTempDir();
        File genericConfigFolder = newTempDir();

        Settings settings = settingsBuilder()
                .put("path.conf", genericConfigFolder)
                .put("path.home", homeFolder)
                .build();
        Environment environment = new Environment(settings);

        PluginManager.PluginHandle pluginHandle = new PluginManager.PluginHandle(pluginName, "version", "user", "repo");
        String configDirPath = Files.simplifyPath(pluginHandle.configDir(environment).getCanonicalPath());
        String expectedDirPath = Files.simplifyPath(new File(genericConfigFolder, pluginName).getCanonicalPath());

        assertThat(configDirPath, is(expectedDirPath));
    }
}
