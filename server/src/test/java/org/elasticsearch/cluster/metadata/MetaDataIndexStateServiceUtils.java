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
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.Index;

import java.util.Map;
import java.util.Set;

public class MetaDataIndexStateServiceUtils {

    private MetaDataIndexStateServiceUtils(){
    }

    /**
     * Allows to call {@link MetaDataIndexStateService#addIndexClosedBlocks(Index[], ClusterState, Set)} which is a protected method.
     */
    public static ClusterState addIndexClosedBlocks(final Index[] indices, final ClusterState state, final Set<Index> blockedIndices) {
        return MetaDataIndexStateService.addIndexClosedBlocks(indices, state, blockedIndices);
    }

    /**
     * Allows to call {@link MetaDataIndexStateService#closeRoutingTable(ClusterState, Map)} which is a protected method.
     */
    public static ClusterState closeRoutingTable(final ClusterState state, final Map<Index, AcknowledgedResponse> results) {
        return MetaDataIndexStateService.closeRoutingTable(state, results);
    }
}
