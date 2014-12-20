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

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterState.ClusterStateDiff;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.lessThan;

public class ClusterStateDiffTests extends ElasticsearchIntegrationTest {

    @Before
    public void indexData() throws Exception {
        //TODO: randomize this
        index("foo", "bar", "1", XContentFactory.jsonBuilder().startObject().field("foo", "foo").endObject());
        index("fuu", "buu", "1", XContentFactory.jsonBuilder().startObject().field("fuu", "fuu").endObject());
        index("baz", "baz", "1", XContentFactory.jsonBuilder().startObject().field("baz", "baz").endObject());
        refresh();
    }

    @Test
    public void testSimpleClusterStateModifications() throws Exception {
        ClusterState clusterState0 = client().admin().cluster().prepareState().get().getState();
        int clusterNameSize = clusterState0.getClusterName().value().length();

        // Assert that we don't send entire cluster state
        ClusterState clusterState1 = ClusterState.builder(clusterState0).incrementVersion().build();
        assertThat(Builder.toDiffBytes(clusterState0, clusterState1).length, lessThan(clusterNameSize + 100));

        ClusterState clusterState2 = ClusterState.builder(clusterState1).incrementVersion()
                .blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).build();

        int fullClusterStateSize = Builder.toBytes(clusterState2).length;
        int diffSize = Builder.toDiffBytes(clusterState1, clusterState2).length;
        logger.info("Complete cluster state {}, diff size {}", fullClusterStateSize, diffSize );
        assertThat(diffSize, lessThan(fullClusterStateSize / 20));

        ClusterState clusterState3 = Builder.diff(clusterState1, clusterState2).apply(clusterState1);
        assertThat(clusterState3.blocks().global(), contains(MetaData.CLUSTER_READ_ONLY_BLOCK));

    }

}
