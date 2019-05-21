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

package org.elasticsearch.env;

import org.elasticsearch.Version;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.nio.file.Path;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeEnvironmentIT extends ESIntegTestCase {
    public void testStartFailureOnDataForNonDataNode() throws Exception {
        final String indexName = "test-fail-on-data";

        logger.info("--> starting one node");
        internalCluster().startNode();

        logger.info("--> creating index");
        prepareCreate(indexName, Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
        ).get();
        final String indexUUID = resolveIndex(indexName).getUUID();

        logger.info("--> restarting the node with node.data=false and node.master=false");
        IllegalStateException ex = expectThrows(IllegalStateException.class,
            "Node started with node.data=false and node.master=false while having existing index metadata must fail",
            () ->
                internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
                    @Override
                    public Settings onNodeStopped(String nodeName) {
                        return Settings.builder()
                            .put(Node.NODE_DATA_SETTING.getKey(), false)
                            .put(Node.NODE_MASTER_SETTING.getKey(), false)
                            .build();
                    }
                }));
        assertThat(ex.getMessage(), containsString(indexUUID));
        assertThat(ex.getMessage(),
            startsWith("Node is started with "
                + Node.NODE_DATA_SETTING.getKey()
                + "=false and "
                + Node.NODE_MASTER_SETTING.getKey()
                + "=false, but has index metadata"));

        // client() also starts the node
        logger.info("--> indexing a simple document");
        client().prepareIndex(indexName, "type1", "1").setSource("field1", "value1").get();

        logger.info("--> restarting the node with node.data=true and node.master=true");
        internalCluster().restartRandomDataNode();

        logger.info("--> restarting the node with node.data=false");
        ex = expectThrows(IllegalStateException.class,
            "Node started with node.data=false while having existing shard data must fail",
            () ->
                internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
                    @Override
                    public Settings onNodeStopped(String nodeName) {
                        return Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build();
                    }
                }));
        assertThat(ex.getMessage(), containsString(indexUUID));
        assertThat(ex.getMessage(),
            startsWith("Node is started with "
                + Node.NODE_DATA_SETTING.getKey()
                + "=false, but has shard data"));
    }

    private IllegalStateException expectThrowsOnRestart(CheckedConsumer<Path[], Exception> onNodeStopped) {
        internalCluster().startNode();
        final Path[] dataPaths = internalCluster().getInstance(NodeEnvironment.class).nodeDataPaths();
        return expectThrows(IllegalStateException.class,
            () -> internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) {
                    try {
                        onNodeStopped.accept(dataPaths);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    return Settings.EMPTY;
                }
            }));
    }

    public void testFailsToStartIfDowngraded() {
        final IllegalStateException illegalStateException = expectThrowsOnRestart(dataPaths ->
            NodeMetaData.FORMAT.writeAndCleanup(new NodeMetaData(randomAlphaOfLength(10), NodeMetaDataTests.tooNewVersion()), dataPaths));
        assertThat(illegalStateException.getMessage(),
            allOf(startsWith("cannot downgrade a node from version ["), endsWith("] to version [" + Version.CURRENT + "]")));
    }

    public void testFailsToStartIfUpgradedTooFar() {
        final IllegalStateException illegalStateException = expectThrowsOnRestart(dataPaths ->
            NodeMetaData.FORMAT.writeAndCleanup(new NodeMetaData(randomAlphaOfLength(10), NodeMetaDataTests.tooOldVersion()), dataPaths));
        assertThat(illegalStateException.getMessage(),
            allOf(startsWith("cannot upgrade a node from version ["), endsWith("] directly to version [" + Version.CURRENT + "]")));
    }
}
