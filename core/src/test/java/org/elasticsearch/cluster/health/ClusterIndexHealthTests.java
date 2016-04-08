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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.empty;

public class ClusterIndexHealthTests extends ESTestCase {
    public void testClusterIndexHealth() {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        int numberOfShards = randomInt(3) + 1;
        int numberOfReplicas = randomInt(4);
        IndexMetaData indexMetaData = IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(numberOfShards).numberOfReplicas(numberOfReplicas).build();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetaData, counter);

        ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetaData, indexRoutingTable);
        logger.info("index status: {}, expected {}", indexHealth.getStatus(), counter.status());
        assertIndexHealth(indexHealth, counter, indexMetaData);
    }


    private void assertIndexHealth(ClusterIndexHealth indexHealth, RoutingTableGenerator.ShardCounter counter, IndexMetaData indexMetaData) {
        assertThat(indexHealth.getStatus(), equalTo(counter.status()));
        assertThat(indexHealth.getNumberOfShards(), equalTo(indexMetaData.getNumberOfShards()));
        assertThat(indexHealth.getNumberOfReplicas(), equalTo(indexMetaData.getNumberOfReplicas()));
        assertThat(indexHealth.getActiveShards(), equalTo(counter.active));
        assertThat(indexHealth.getRelocatingShards(), equalTo(counter.relocating));
        assertThat(indexHealth.getInitializingShards(), equalTo(counter.initializing));
        assertThat(indexHealth.getUnassignedShards(), equalTo(counter.unassigned));
        assertThat(indexHealth.getShards().size(), equalTo(indexMetaData.getNumberOfShards()));
        assertThat(indexHealth.getValidationFailures(), empty());
        int totalShards = 0;
        for (ClusterShardHealth shardHealth : indexHealth.getShards().values()) {
            totalShards += shardHealth.getActiveShards() + shardHealth.getInitializingShards() + shardHealth.getUnassignedShards();
        }

        assertThat(totalShards, equalTo(indexMetaData.getNumberOfShards() * (1 + indexMetaData.getNumberOfReplicas())));
    }
}