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
package org.elasticsearch.discovery;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ElectMasterService;

/**
 * A {@link Discovery} implementation that is used by {@link org.elasticsearch.tribe.TribeService}. This implementation
 * doesn't support any clustering features. Most notably {@link #startInitialJoin()} does nothing and
 * {@link #publish(ClusterChangedEvent, AckListener)} is not supported.
 */
public class NoneDiscovery extends AbstractLifecycleComponent implements Discovery {

    private final ClusterService clusterService;
    private final DiscoverySettings discoverySettings;

    @Inject
    public NoneDiscovery(Settings settings, ClusterService clusterService, ClusterSettings clusterSettings) {
        super(settings);
        this.clusterService = clusterService;
        this.discoverySettings = new DiscoverySettings(settings, clusterSettings);
    }

    @Override
    public DiscoveryNode localNode() {
        return clusterService.localNode();
    }

    @Override
    public String nodeDescription() {
        return clusterService.getClusterName().value() + "/" + clusterService.localNode().getId();
    }

    @Override
    public void setAllocationService(AllocationService allocationService) {

    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, AckListener ackListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DiscoveryStats stats() {
        return null;
    }

    @Override
    public DiscoverySettings getDiscoverySettings() {
        return discoverySettings;
    }

    @Override
    public void startInitialJoin() {

    }

    @Override
    public int getMinimumMasterNodes() {
        return ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.get(settings);
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }
}
