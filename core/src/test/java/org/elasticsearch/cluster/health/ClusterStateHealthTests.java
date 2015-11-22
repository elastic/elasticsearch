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
package org.elasticsearch.cluster.health;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.*;

public class ClusterStateHealthTests extends ESTestCase {
    private final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);

    public void testClusterHealth() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        MetaData.Builder metaData = MetaData.builder();
        for (int i = randomInt(4); i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetaData indexMetaData = IndexMetaData
                    .builder("test_" + Integer.toString(i))
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
                    .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetaData, counter);
            metaData.put(indexMetaData, true);
            routingTable.add(indexRoutingTable);
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable.build()).build();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndices(clusterState, IndicesOptions.strictExpand(), (String[]) null);
        ClusterStateHealth clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices);
        logger.info("cluster status: {}, expected {}", clusterStateHealth.getStatus(), counter.status());
        clusterStateHealth = maybeSerialize(clusterStateHealth);
        assertClusterHealth(clusterStateHealth, counter);
    }

    public void testValidations() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        IndexMetaData indexMetaData = IndexMetaData
                .builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(2)
                .numberOfReplicas(2)
                .build();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetaData, counter);
        indexMetaData = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(3).build();

        ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetaData, indexRoutingTable);
        assertThat(indexHealth.getValidationFailures(), Matchers.hasSize(2));

        RoutingTable.Builder routingTable = RoutingTable.builder();
        MetaData.Builder metaData = MetaData.builder();
        metaData.put(indexMetaData, true);
        routingTable.add(indexRoutingTable);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable.build()).build();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndices(clusterState, IndicesOptions.strictExpand(), (String[]) null);
        ClusterStateHealth clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices);
        clusterStateHealth = maybeSerialize(clusterStateHealth);
        // currently we have no cluster level validation failures as index validation issues are reported per index.
        assertThat(clusterStateHealth.getValidationFailures(), Matchers.hasSize(0));
    }


    ClusterStateHealth maybeSerialize(ClusterStateHealth clusterStateHealth) throws IOException {
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            clusterStateHealth.writeTo(out);
            StreamInput in = StreamInput.wrap(out.bytes());
            clusterStateHealth = ClusterStateHealth.readClusterHealth(in);
        }
        return clusterStateHealth;
    }

    private void assertClusterHealth(ClusterStateHealth clusterStateHealth, RoutingTableGenerator.ShardCounter counter) {
        assertThat(clusterStateHealth.getStatus(), equalTo(counter.status()));
        assertThat(clusterStateHealth.getActiveShards(), equalTo(counter.active));
        assertThat(clusterStateHealth.getActivePrimaryShards(), equalTo(counter.primaryActive));
        assertThat(clusterStateHealth.getInitializingShards(), equalTo(counter.initializing));
        assertThat(clusterStateHealth.getRelocatingShards(), equalTo(counter.relocating));
        assertThat(clusterStateHealth.getUnassignedShards(), equalTo(counter.unassigned));
        assertThat(clusterStateHealth.getValidationFailures(), empty());
        assertThat(clusterStateHealth.getActiveShardsPercent(), is(allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(100.0))));
    }
}