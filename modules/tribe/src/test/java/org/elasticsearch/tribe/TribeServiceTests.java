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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.MergableCustomMetaData;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetaData;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.instanceOf;

public class TribeServiceTests extends ESTestCase {
    public void testMinimalSettings() {
        Settings globalSettings = Settings.builder()
            .put("node.name", "nodename")
            .put("path.home", "some/path").build();
        Settings clientSettings = TribeService.buildClientSettings("tribe1", "parent_id", globalSettings, Settings.EMPTY);
        assertEquals("some/path", clientSettings.get("path.home"));
        assertEquals("nodename/tribe1", clientSettings.get("node.name"));
        assertEquals("tribe1", clientSettings.get("tribe.name"));
        assertFalse(NetworkModule.HTTP_ENABLED.get(clientSettings));
        assertEquals("false", clientSettings.get("node.master"));
        assertEquals("false", clientSettings.get("node.data"));
        assertEquals("false", clientSettings.get("node.ingest"));
        assertEquals("false", clientSettings.get("node.local_storage"));
        assertEquals("3707202549613653169", clientSettings.get("node.id.seed")); // should be fixed by the parent id and tribe name
        assertEquals(9, clientSettings.size());
    }

    public void testEnvironmentSettings() {
        Settings globalSettings = Settings.builder()
            .put("node.name", "nodename")
            .put("path.home", "some/path")
            .put("path.logs", "logs/path").build();
        Settings clientSettings = TribeService.buildClientSettings("tribe1", "parent_id", globalSettings, Settings.EMPTY);
        assertEquals("some/path", clientSettings.get("path.home"));
        assertEquals("logs/path", clientSettings.get("path.logs"));

        Settings tribeSettings = Settings.builder()
            .put("path.home", "alternate/path").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            TribeService.buildClientSettings("tribe1", "parent_id", globalSettings, tribeSettings);
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
        Settings clientSettings = TribeService.buildClientSettings("tribe1", "parent_id", globalSettings, Settings.EMPTY);
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
        clientSettings = TribeService.buildClientSettings("tribe1", "parent_id", globalSettings, tribeSettings);
        assertEquals("3.3.3.3", clientSettings.get("network.host"));
        assertEquals("4.4.4.4", clientSettings.get("network.bind_host"));
        assertEquals("5.5.5.5", clientSettings.get("network.publish_host"));
        assertEquals("6.6.6.6", clientSettings.get("transport.host"));
        assertEquals("7.7.7.7", clientSettings.get("transport.bind_host"));
        assertEquals("8.8.8.8", clientSettings.get("transport.publish_host"));
    }

    public void testMergeCustomMetaDataSimple() {
        Map<String, MetaData.Custom> mergedCustoms =
                TribeService.mergeChangedCustomMetaData(Collections.singleton(MergableCustomMetaData1.TYPE),
                        s -> Collections.singletonList(new MergableCustomMetaData1("data1")));
        TestCustomMetaData mergedCustom = (TestCustomMetaData) mergedCustoms.get(MergableCustomMetaData1.TYPE);
        assertThat(mergedCustom, instanceOf(MergableCustomMetaData1.class));
        assertNotNull(mergedCustom);
        assertEquals(mergedCustom.getData(), "data1");
    }

    public void testMergeCustomMetaData() {
        Map<String, MetaData.Custom> mergedCustoms =
                TribeService.mergeChangedCustomMetaData(Collections.singleton(MergableCustomMetaData1.TYPE),
                        s -> Arrays.asList(new MergableCustomMetaData1("data1"), new MergableCustomMetaData1("data2")));
        TestCustomMetaData mergedCustom = (TestCustomMetaData) mergedCustoms.get(MergableCustomMetaData1.TYPE);
        assertThat(mergedCustom, instanceOf(MergableCustomMetaData1.class));
        assertNotNull(mergedCustom);
        assertEquals(mergedCustom.getData(), "data2");
    }

    public void testMergeMultipleCustomMetaData() {
        Map<String, List<MergableCustomMetaData>> inputMap = new HashMap<>();
        inputMap.put(MergableCustomMetaData1.TYPE,
                Arrays.asList(new MergableCustomMetaData1("data10"), new MergableCustomMetaData1("data11")));
        inputMap.put(MergableCustomMetaData2.TYPE,
                Arrays.asList(new MergableCustomMetaData2("data21"), new MergableCustomMetaData2("data20")));
        Map<String, MetaData.Custom> mergedCustoms = TribeService.mergeChangedCustomMetaData(
                        Sets.newHashSet(MergableCustomMetaData1.TYPE, MergableCustomMetaData2.TYPE), inputMap::get);
        TestCustomMetaData mergedCustom = (TestCustomMetaData) mergedCustoms.get(MergableCustomMetaData1.TYPE);
        assertNotNull(mergedCustom);
        assertThat(mergedCustom, instanceOf(MergableCustomMetaData1.class));
        assertEquals(mergedCustom.getData(), "data11");
        mergedCustom = (TestCustomMetaData) mergedCustoms.get(MergableCustomMetaData2.TYPE);
        assertNotNull(mergedCustom);
        assertThat(mergedCustom, instanceOf(MergableCustomMetaData2.class));
        assertEquals(mergedCustom.getData(), "data21");
    }

    public void testMergeCustomMetaDataFromMany() {
        Map<String, List<MergableCustomMetaData>> inputMap = new HashMap<>();
        int n = randomIntBetween(3, 5);
        List<MergableCustomMetaData> customList1 = new ArrayList<>();
        for (int i = 0; i <= n; i++) {
            customList1.add(new MergableCustomMetaData1("data1"+String.valueOf(i)));
        }
        Collections.shuffle(customList1, random());
        inputMap.put(MergableCustomMetaData1.TYPE, customList1);
        List<MergableCustomMetaData> customList2 = new ArrayList<>();
        for (int i = 0; i <= n; i++) {
            customList2.add(new MergableCustomMetaData2("data2"+String.valueOf(i)));
        }
        Collections.shuffle(customList2, random());
        inputMap.put(MergableCustomMetaData2.TYPE, customList2);

        Map<String, MetaData.Custom> mergedCustoms = TribeService.mergeChangedCustomMetaData(
                        Sets.newHashSet(MergableCustomMetaData1.TYPE, MergableCustomMetaData2.TYPE), inputMap::get);
        TestCustomMetaData mergedCustom = (TestCustomMetaData) mergedCustoms.get(MergableCustomMetaData1.TYPE);
        assertNotNull(mergedCustom);
        assertThat(mergedCustom, instanceOf(MergableCustomMetaData1.class));
        assertEquals(mergedCustom.getData(), "data1"+String.valueOf(n));
        mergedCustom = (TestCustomMetaData) mergedCustoms.get(MergableCustomMetaData2.TYPE);
        assertNotNull(mergedCustom);
        assertThat(mergedCustom, instanceOf(MergableCustomMetaData2.class));
        assertEquals(mergedCustom.getData(), "data2"+String.valueOf(n));
    }

    public static class MockTribePlugin extends TribePlugin {

        static List<Class<? extends Plugin>> classpathPlugins = Arrays.asList(MockTribePlugin.class, getTestTransportPlugin());

        public MockTribePlugin(Settings settings) {
            super(settings);
        }

        protected Function<Settings, Node> nodeBuilder(Path configPath) {
            return settings -> new MockNode(new Environment(settings, configPath), classpathPlugins);
        }

    }

    public void testTribeNodeDeprecation() throws IOException {
        final Path tempDir = createTempDir();
        Settings.Builder settings = Settings.builder()
            .put("node.name", "test-node")
            .put("path.home", tempDir)
            .put(NetworkModule.HTTP_ENABLED.getKey(), false)
            .put(NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), getTestTransportType());

        final boolean tribeServiceEnable = randomBoolean();
        if (tribeServiceEnable) {
            String clusterName = "single-node-cluster";
            String tribeSetting = "tribe." + clusterName + ".";
            settings.put(tribeSetting + ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName)
                .put(tribeSetting + NetworkModule.TRANSPORT_TYPE_SETTING.getKey(), getTestTransportType());
        }
        try (Node node = new MockNode(settings.build(), MockTribePlugin.classpathPlugins)) {
            if (tribeServiceEnable) {
                assertWarnings("tribe nodes are deprecated in favor of cross-cluster search and will be removed in Elasticsearch 7.0.0");
            }
        }
    }

    static class MergableCustomMetaData1 extends TestCustomMetaData
            implements MergableCustomMetaData<MergableCustomMetaData1> {
        public static final String TYPE = "custom_md_1";

        protected MergableCustomMetaData1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        public static MergableCustomMetaData1 readFrom(StreamInput in) throws IOException {
            return readFrom(MergableCustomMetaData1::new, in);
        }

        public static NamedDiff<MetaData.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }

        @Override
        public MergableCustomMetaData1 merge(MergableCustomMetaData1 other) {
            return (getData().compareTo(other.getData()) >= 0) ? this : other;
        }
    }

    static class MergableCustomMetaData2 extends TestCustomMetaData
            implements MergableCustomMetaData<MergableCustomMetaData2> {
        public static final String TYPE = "custom_md_2";

        protected MergableCustomMetaData2(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        public static MergableCustomMetaData2 readFrom(StreamInput in) throws IOException {
            return readFrom(MergableCustomMetaData2::new, in);
        }

        public static NamedDiff<MetaData.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }


        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }

        @Override
        public MergableCustomMetaData2 merge(MergableCustomMetaData2 other) {
            return (getData().compareTo(other.getData()) >= 0) ? this : other;
        }
    }
}
