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

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ClusterStateResponseTests extends AbstractWireSerializingTestCase<ClusterStateResponse> {

    @Override
    protected ClusterStateResponse createTestInstance() {
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(4));
        ClusterState clusterState = null;
        if (randomBoolean()) {
            ClusterState.Builder clusterStateBuilder = ClusterState.builder(clusterName)
                .version(randomNonNegativeLong());
            if (randomBoolean()) {
                clusterStateBuilder.nodes(DiscoveryNodes.builder().masterNodeId(randomAlphaOfLength(4)).build());
            }
            clusterState = clusterStateBuilder.build();
        }
        return new ClusterStateResponse(clusterName, clusterState, randomBoolean());
    }

    @Override
    protected Writeable.Reader<ClusterStateResponse> instanceReader() {
        return ClusterStateResponse::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }
}
