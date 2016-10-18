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
package org.elasticsearch.node;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.MockTcpTransportPlugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class NodeTests extends ESTestCase {

    public void testNodeName() throws IOException {
        final Path tempDir = createTempDir();
        final String name = randomBoolean() ? randomAsciiOfLength(10) : null;
        Settings.Builder settings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", randomLong()))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(NetworkModule.HTTP_ENABLED.getKey(), false)
            .put("transport.type", "mock-socket-network")
            .put(Node.NODE_DATA_SETTING.getKey(), true);
        if (name != null) {
            settings.put(Node.NODE_NAME_SETTING.getKey(), name);
        }
        try (Node node = new MockNode(settings.build(), Collections.singleton(MockTcpTransportPlugin.class))) {
            final Settings nodeSettings = randomBoolean() ? node.settings() : node.getEnvironment().settings();
            if (name == null) {
                assertThat(Node.NODE_NAME_SETTING.get(nodeSettings), equalTo(node.getNodeEnvironment().nodeId().substring(0, 7)));
            } else {
                assertThat(Node.NODE_NAME_SETTING.get(nodeSettings), equalTo(name));
            }
        }
    }

    public void testWarnIfPreRelease() {
        final Logger logger = mock(Logger.class);

        final int id = randomIntBetween(1, 9) * 1000000;
        final Version releaseVersion = Version.fromId(id + 99);
        final Version preReleaseVersion = Version.fromId(id + randomIntBetween(0, 98));

        Node.warnIfPreRelease(releaseVersion, false, logger);
        verifyNoMoreInteractions(logger);

        reset(logger);
        Node.warnIfPreRelease(releaseVersion, true, logger);
        verify(logger).warn(
            "version [{}] is a pre-release version of Elasticsearch and is not suitable for production", releaseVersion + "-SNAPSHOT");

        reset(logger);
        final boolean isSnapshot = randomBoolean();
        Node.warnIfPreRelease(preReleaseVersion, isSnapshot, logger);
        verify(logger).warn(
            "version [{}] is a pre-release version of Elasticsearch and is not suitable for production",
            preReleaseVersion + (isSnapshot ? "-SNAPSHOT" : ""));

    }

}
