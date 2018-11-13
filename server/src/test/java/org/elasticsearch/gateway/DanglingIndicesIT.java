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
package org.elasticsearch.gateway;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DanglingIndicesIT extends ESIntegTestCase {

    public void testNonDataNodeWithShardDataFolders() throws Exception {
        final String indexName = "simple1";

        final InternalTestCluster cluster = internalCluster();
        // keep master node to be able to start coordinator-only node
        final String master = cluster.startMasterOnlyNode();

        String node = cluster.startNode();

        createIndex(indexName,
            Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .build());
        ensureGreen(indexName);

        if (randomBoolean()) {
            index(indexName, "_doc", "1", "f", "f");
        }

        final Path[] nodeDataPaths = internalCluster().getInstance(NodeEnvironment.class, node).nodeDataPaths();

        cluster.stopRandomNode(InternalTestCluster.nameFilter(node));

        final boolean masterNode = randomBoolean();
        final IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> {String n = masterNode ? cluster.startMasterOnlyNode() : cluster.startCoordinatingOnlyNode(Settings.EMPTY);});
        final String message = exception.getMessage();
        assertThat(message, containsString(" node cannot have shard data "));
        assertThat(message, containsString(", found: ["));

        // resolve and delete all paths those block a node to start
        final int idx = message.indexOf("[") + 1;
        final Path[] paths =
            Arrays.stream(message.substring(idx, message.indexOf(']', idx)).split(",")).
                map(s -> PathUtils.get(s.trim())).toArray(Path[]::new);
        IOUtils.rm(paths);

        // start node again after clean up at the same data path
        node = masterNode ? cluster.startMasterOnlyNode() : cluster.startCoordinatingOnlyNode(Settings.EMPTY);
        final Path[] newNodeDataPaths = internalCluster().getInstance(NodeEnvironment.class, node).nodeDataPaths();
        assertThat(nodeDataPaths, equalTo(newNodeDataPaths));

    }

}
