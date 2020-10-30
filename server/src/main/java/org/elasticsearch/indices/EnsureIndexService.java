/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.gateway.GatewayService;

import java.io.IOException;

public abstract class EnsureIndexService {
    private final ClusterService clusterService;
    private final String index;
    private final Client client;

    public EnsureIndexService(ClusterService clusterService, String index, Client client) {
        this.clusterService = clusterService;
        this.index = index;
        this.client = client;
    }

    public void createIndexIfNecessary(ActionListener<String> listener) {
        ClusterState state = clusterService.state();

        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have the index templates,
            // while they actually do exist
            listener.onFailure(new Exception("Node not recovered yet"));
            return;
        }

        // no master node, exit immediately
        DiscoveryNode masterNode = state.getNodes().getMasterNode();
        if (masterNode == null) {
            listener.onFailure(new Exception("No master node"));
            return;
        }

        if (state.routingTable().hasIndex(index)) {
            listener.onResponse(index);
            return;
        }

        // This service may require to run on a master node.
        // If not a master node, exit.
        if (requiresMasterNode() && state.nodes().isLocalNodeElectedMaster() == false) {
            listener.onFailure(new Exception("Not running on master node"));
            return;
        }

        try {
            client.admin()
                .indices()
                .prepareCreate(index)
                .setSettings(indexSettings())
                .setMapping(mappings())
                .execute(new ActionListener<>() {
                    @Override
                    public void onResponse(CreateIndexResponse createIndexResponse) {
                        assert createIndexResponse.index().equals(index);
                        listener.onResponse(createIndexResponse.index());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof ResourceAlreadyExistsException
                            || ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                            listener.onResponse(index);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Whether the service should only apply changes when running on the master node.
     * This is useful for plugins where certain actions are performed on master nodes
     * and the index should match the respective version.
     */
    protected boolean requiresMasterNode() {
        return false;
    }

    protected abstract XContentBuilder mappings() throws IOException;

    protected abstract Settings indexSettings();
}
