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

package org.elasticsearch.cluster.routing;


import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Arrays;

public class RoutingBackwardCompatibilityTests extends ESTestCase {

    public void testBackwardCompatibility() throws Exception {
        Path baseDir = createTempDir();
        Node node = new Node(Settings.builder().put("path.home", baseDir.toString()).build());
        try {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(RoutingBackwardCompatibilityTests.class.getResourceAsStream("/org/elasticsearch/cluster/routing/shard_routes.txt"), "UTF-8"))) {
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    if (line.startsWith("#")) { // comment
                        continue;
                    }
                    String[] parts = line.split("\t");
                    assertEquals(Arrays.toString(parts), 7, parts.length);
                    final String index = parts[0];
                    final int numberOfShards = Integer.parseInt(parts[1]);
                    final String type = parts[2];
                    final String id = parts[3];
                    final String routing = "null".equals(parts[4]) ? null : parts[4];
                    final int pre20ExpectedShardId = Integer.parseInt(parts[5]);
                    final int currentExpectedShard = Integer.parseInt(parts[6]);

                    OperationRouting operationRouting = node.injector().getInstance(OperationRouting.class);
                    for (Version version : VersionUtils.allVersions()) {
                        final Settings settings = settings(version).build();
                        IndexMetaData indexMetaData = IndexMetaData.builder(index).settings(settings).numberOfShards(numberOfShards).numberOfReplicas(randomInt(3)).build();
                        MetaData.Builder metaData = MetaData.builder().put(indexMetaData, false);
                        RoutingTable routingTable = RoutingTable.builder().addAsNew(indexMetaData).build();
                        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
                        final int shardId = operationRouting.indexShards(clusterState, index, type, id, routing).shardId().getId();
                        if (version.before(Version.V_2_0_0_beta1)) {
                            assertEquals(pre20ExpectedShardId, shardId);
                        } else {
                            assertEquals(currentExpectedShard, shardId);
                        }
                    }
                }
            }
        } finally {
            node.close();
        }
    }

}
