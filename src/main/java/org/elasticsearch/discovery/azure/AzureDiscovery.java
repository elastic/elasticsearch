/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.discovery.azure;

import org.elasticsearch.Version;
import org.elasticsearch.cloud.azure.AzureComputeService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class AzureDiscovery extends ZenDiscovery {

    @Inject
    public AzureDiscovery(Settings settings, ClusterName clusterName, ThreadPool threadPool, TransportService transportService,
                          ClusterService clusterService, NodeSettingsService nodeSettingsService, ZenPingService pingService,
                          DiscoveryNodeService discoveryNodeService, AzureComputeService azureService) {
        super(settings, clusterName, threadPool, transportService, clusterService, nodeSettingsService, discoveryNodeService, pingService, Version.CURRENT);
        if (settings.getAsBoolean("cloud.enabled", true)) {
            ImmutableList<? extends ZenPing> zenPings = pingService.zenPings();
            UnicastZenPing unicastZenPing = null;
            for (ZenPing zenPing : zenPings) {
                if (zenPing instanceof UnicastZenPing) {
                    unicastZenPing = (UnicastZenPing) zenPing;
                    break;
                }
            }

            if (unicastZenPing != null) {
                // update the unicast zen ping to add cloud hosts provider
                // and, while we are at it, use only it and not the multicast for example
                unicastZenPing.addHostsProvider(new AzureUnicastHostsProvider(settings, azureService.nodes()));
                pingService.zenPings(ImmutableList.of(unicastZenPing));
            } else {
                logger.warn("failed to apply azure unicast discovery, no unicast ping found");
            }
        }
    }
}
