/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indexer.routing;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indexer.cluster.IndexerClusterService;
import org.elasticsearch.indexer.cluster.IndexerClusterState;
import org.elasticsearch.indexer.cluster.IndexerClusterStateUpdateTask;

/**
 * @author kimchy (shay.banon)
 */
public class IndexersRouter extends AbstractComponent implements ClusterStateListener {

    private final IndexerClusterService indexerClusterService;

    @Inject public IndexersRouter(Settings settings, ClusterService clusterService, IndexerClusterService indexerClusterService) {
        super(settings);
        this.indexerClusterService = indexerClusterService;
        clusterService.add(this);
    }

    @Override public void clusterChanged(final ClusterChangedEvent event) {
        if (event.nodesChanged()) {
            indexerClusterService.submitStateUpdateTask("reroute_indexers_node_changed", new IndexerClusterStateUpdateTask() {
                @Override public IndexerClusterState execute(IndexerClusterState currentState) {
                    return null;  //To change body of implemented methods use File | Settings | File Templates.
                }
            });
        }
    }
}
