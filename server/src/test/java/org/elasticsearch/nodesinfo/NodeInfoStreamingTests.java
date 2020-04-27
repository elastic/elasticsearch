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

package org.elasticsearch.nodesinfo;

import org.elasticsearch.Build;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.ingest.ProcessorInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.TransportInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.IsEqual.equalTo;

public class NodeInfoStreamingTests extends ESTestCase {

    public void testNodeInfoStreaming() throws IOException {
        NodeInfo nodeInfo = createNodeInfo();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeInfo.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                NodeInfo readNodeInfo = new NodeInfo(in);
                assertExpectedUnchanged(nodeInfo, readNodeInfo);
            }
        }
    }
    // checks all properties that are expected to be unchanged.
    // Once we start changing them between versions this method has to be changed as well
    private void assertExpectedUnchanged(NodeInfo nodeInfo, NodeInfo readNodeInfo) throws IOException {
        assertThat(nodeInfo.getBuild().toString(), equalTo(readNodeInfo.getBuild().toString()));
        assertThat(nodeInfo.getHostname(), equalTo(readNodeInfo.getHostname()));
        assertThat(nodeInfo.getVersion(), equalTo(readNodeInfo.getVersion()));
        compareJsonOutput(nodeInfo.getInfo(HttpInfo.class), readNodeInfo.getInfo(HttpInfo.class));
        compareJsonOutput(nodeInfo.getInfo(JvmInfo.class), readNodeInfo.getInfo(JvmInfo.class));
        compareJsonOutput(nodeInfo.getInfo(ProcessInfo.class), readNodeInfo.getInfo(ProcessInfo.class));
        compareJsonOutput(nodeInfo.getSettings(), readNodeInfo.getSettings());
        compareJsonOutput(nodeInfo.getInfo(ThreadPoolInfo.class), readNodeInfo.getInfo(ThreadPoolInfo.class));
        compareJsonOutput(nodeInfo.getInfo(TransportInfo.class), readNodeInfo.getInfo(TransportInfo.class));
        compareJsonOutput(nodeInfo.getNode(), readNodeInfo.getNode());
        compareJsonOutput(nodeInfo.getInfo(OsInfo.class), readNodeInfo.getInfo(OsInfo.class));
        compareJsonOutput(nodeInfo.getInfo(PluginsAndModules.class), readNodeInfo.getInfo(PluginsAndModules.class));
        compareJsonOutput(nodeInfo.getInfo(IngestInfo.class), readNodeInfo.getInfo(IngestInfo.class));
    }

    private void compareJsonOutput(ToXContent param1, ToXContent param2) throws IOException {
        if (param1 == null) {
            assertNull(param2);
            return;
        }
        ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        XContentBuilder param1Builder = jsonBuilder();
        param1Builder.startObject();
        param1.toXContent(param1Builder, params);
        param1Builder.endObject();

        XContentBuilder param2Builder = jsonBuilder();
        param2Builder.startObject();
        param2.toXContent(param2Builder, params);
        param2Builder.endObject();
        assertThat(Strings.toString(param1Builder), equalTo(Strings.toString(param2Builder)));
    }

    private static NodeInfo createNodeInfo() {
        Build build = Build.CURRENT;
        DiscoveryNode node = new DiscoveryNode("test_node", buildNewFakeTransportAddress(),
                emptyMap(), emptySet(), VersionUtils.randomVersion(random()));
        Settings settings = randomBoolean() ? null : Settings.builder().put("test", "setting").build();
        OsInfo osInfo = null;
        if (randomBoolean()) {
            int availableProcessors = randomIntBetween(1, 64);
            int allocatedProcessors = randomIntBetween(1, availableProcessors);
            long refreshInterval = randomBoolean() ? -1 : randomNonNegativeLong();
            String name = randomAlphaOfLengthBetween(3, 10);
            String arch = randomAlphaOfLengthBetween(3, 10);
            String version = randomAlphaOfLengthBetween(3, 10);
            osInfo = new OsInfo(refreshInterval, availableProcessors, allocatedProcessors, name, name, arch, version);
        }
        ProcessInfo process = randomBoolean() ? null : new ProcessInfo(randomInt(), randomBoolean(), randomNonNegativeLong());
        JvmInfo jvm = randomBoolean() ? null : JvmInfo.jvmInfo();
        ThreadPoolInfo threadPoolInfo = null;
        if (randomBoolean()) {
            int numThreadPools = randomIntBetween(1, 10);
            List<ThreadPool.Info> threadPoolInfos = new ArrayList<>(numThreadPools);
            for (int i = 0; i < numThreadPools; i++) {
                threadPoolInfos.add(new ThreadPool.Info(randomAlphaOfLengthBetween(3, 10),
                        randomFrom(ThreadPool.ThreadPoolType.values()), randomInt()));
            }
            threadPoolInfo = new ThreadPoolInfo(threadPoolInfos);
        }
        Map<String, BoundTransportAddress> profileAddresses = new HashMap<>();
        BoundTransportAddress dummyBoundTransportAddress = new BoundTransportAddress(
                new TransportAddress[]{buildNewFakeTransportAddress()}, buildNewFakeTransportAddress());
        profileAddresses.put("test_address", dummyBoundTransportAddress);
        TransportInfo transport = randomBoolean() ? null : new TransportInfo(dummyBoundTransportAddress, profileAddresses);
        HttpInfo httpInfo = randomBoolean() ? null : new HttpInfo(dummyBoundTransportAddress, randomNonNegativeLong());

        PluginsAndModules pluginsAndModules = null;
        if (randomBoolean()) {
            int numPlugins = randomIntBetween(0, 5);
            List<PluginInfo> plugins = new ArrayList<>();
            for (int i = 0; i < numPlugins; i++) {
                plugins.add(new PluginInfo(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10),
                    randomAlphaOfLengthBetween(3, 10), VersionUtils.randomVersion(random()), "1.8",
                    randomAlphaOfLengthBetween(3, 10), Collections.emptyList(), randomBoolean()));
            }
            int numModules = randomIntBetween(0, 5);
            List<PluginInfo> modules = new ArrayList<>();
            for (int i = 0; i < numModules; i++) {
                modules.add(new PluginInfo(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10),
                    randomAlphaOfLengthBetween(3, 10), VersionUtils.randomVersion(random()), "1.8",
                    randomAlphaOfLengthBetween(3, 10), Collections.emptyList(), randomBoolean()));
            }
            pluginsAndModules = new PluginsAndModules(plugins, modules);
        }

        IngestInfo ingestInfo = null;
        if (randomBoolean()) {
            int numProcessors = randomIntBetween(0, 5);
            List<ProcessorInfo> processors = new ArrayList<>(numProcessors);
            for (int i = 0; i < numProcessors; i++) {
                processors.add(new ProcessorInfo(randomAlphaOfLengthBetween(3, 10)));
            }
            ingestInfo = new IngestInfo(processors);
        }

        ByteSizeValue indexingBuffer = null;
        if (randomBoolean()) {
            // pick a random long that sometimes exceeds an int:
            indexingBuffer = new ByteSizeValue(random().nextLong() & ((1L<<40)-1));
        }
        return new NodeInfo(VersionUtils.randomVersion(random()), build, node, settings, osInfo, process, jvm,
            threadPoolInfo, transport, httpInfo, pluginsAndModules, ingestInfo, indexingBuffer);
    }
}
