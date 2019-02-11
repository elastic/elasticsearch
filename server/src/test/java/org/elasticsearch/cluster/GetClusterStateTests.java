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

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for the get cluster state API.
 *
 * See: {@link org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction}
 *      {@link org.elasticsearch.rest.action.admin.cluster.RestClusterStateAction}
 */
public class GetClusterStateTests extends ESSingleNodeTestCase {

    public void testGetClusterState() {
        ClusterStateResponse response = client().admin().cluster().prepareState().get();
        assertNotNull(response.getState());
        assertNotNull(response.getClusterName());
        // assume the cluster state size is 50 bytes or more, just so we aren't testing against size of 0
        assertThat(response.getTotalCompressedSize().getBytes(), greaterThanOrEqualTo(50L));
    }

    public void testSizeDerivedFromFullClusterState() {
        ClusterStateResponse response = client().admin().cluster().prepareState().get();
        final ClusterState clusterState = response.getState();
        final long totalCompressedSize = response.getTotalCompressedSize().getBytes();
        // exclude the nodes from being returned, the total size returned should still be
        // the same as when no filtering was applied to the cluster state retrieved
        response = client().admin().cluster().prepareState().setNodes(false).get();
        assertEquals(totalCompressedSize, response.getTotalCompressedSize().getBytes());
        assertNotEquals(clusterState, response.getState());
        assertEquals(0, response.getState().nodes().getSize());
    }
}
