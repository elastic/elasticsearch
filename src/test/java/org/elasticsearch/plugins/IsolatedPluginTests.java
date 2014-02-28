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
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

// NB: the tests uses System Properties to pass the information from different plugins (loaded in separate CLs) to the test.
// hence the use of try/finally blocks to clean these up after the test has been executed as otherwise the test framework will trigger a failure
@ClusterScope(scope = Scope.TEST, numNodes = 1)
public class IsolatedPluginTests extends ElasticsearchIntegrationTest {

    private static final Settings SETTINGS;
    private static final File PLUGIN_DIR;
    private Properties props;

    static {
        PLUGIN_DIR = Files.createTempDir();

        SETTINGS = ImmutableSettings.settingsBuilder()
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("force.http.enabled", true)
                .put("plugin.isolation", true)
                .put("path.plugins", PLUGIN_DIR.getAbsolutePath())
                .build();
    }

    @Before
    public void beforeTest() throws Exception {
        props = new Properties();
        props.putAll(System.getProperties());

        logger.info("Installing plugins into folder {}", PLUGIN_DIR);
        FileSystemUtils.mkdirs(PLUGIN_DIR);

        // copy plugin
        copyPlugin(new File(PLUGIN_DIR, "plugin-v1"));
        copyPlugin(new File(PLUGIN_DIR, "plugin-v2"));
    }

    private void copyPlugin(File p1) throws IOException {
        FileSystemUtils.mkdirs(p1);
        // copy plugin
        File dir = new File(p1, "org/elasticsearch/plugins/isolation/");
        FileSystemUtils.mkdirs(dir);
        copyFile(getClass().getResourceAsStream("isolation/DummyClass.class"), new File(dir, "DummyClass.class"));
        copyFile(getClass().getResourceAsStream("isolation/IsolatedPlugin.class"), new File(dir, "IsolatedPlugin.class"));
        copyFile(getClass().getResourceAsStream("isolation/es-plugin.properties"), new File(p1, "es-plugin.properties"));
    }

    private void copyFile(InputStream source, File out) throws IOException {
        out.createNewFile();
        Streams.copy(source, new FileOutputStream(out));
    }

    @After
    public void afterTest() throws Exception {
        FileSystemUtils.deleteRecursively(PLUGIN_DIR);
    }

    protected Settings nodeSettings(int nodeOrdinal) {
        return SETTINGS;
    }

    @Test
    public void testPluginNumberOfIsolatedInstances() throws Exception {
        try {
            NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
            assertThat(nodesInfoResponse.getNodes().length, equalTo(1));
            assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos(), notNullValue());
            assertThat(nodesInfoResponse.getNodes()[0].getPlugins().getInfos().size(), equalTo(2));
        } finally {
            System.setProperties(props);
        }
    }

    @Test
    public void testIsolatedPluginProperties() throws Exception {
        try {
            cluster().client();
            Properties p = System.getProperties();
            assertThat(p.getProperty("es.test.isolated.plugin.count"), equalTo("2"));
            String prop = p.getProperty("es.test.isolated.plugin.instantiated.hashes");
            String[] hashes = Strings.delimitedListToStringArray(prop, " ");
            // 2 plugins plus trailing space
            assertThat(hashes.length, equalTo(3));
            assertThat(p.getProperty("es.test.isolated.plugin.instantiated"), equalTo(hashes[1]));
        } finally {
            System.setProperties(props);
        }
    }
}