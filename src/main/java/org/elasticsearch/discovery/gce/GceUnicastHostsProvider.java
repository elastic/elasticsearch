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

package org.elasticsearch.discovery.gce;

import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.NetworkInterface;
import org.elasticsearch.cloud.gce.GceComputeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

/**
 *
 */
public class GceUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    static final class Fields {
        private static final String PROJECT = "project_id";
        private static final String ZONE = "zone";
        private static final String REFRESH = "refresh_interval";
    }

    static final class Status {
        private static final String TERMINATED = "TERMINATED";
    }

    private final GceComputeService gceComputeService;
    private TransportService transportService;
    private NetworkService networkService;

    private final String project;
    private final String zone;
    private final String[] tags;

    private final TimeValue refreshInterval;
    private long lastRefresh;
    private List<DiscoveryNode> cachedDiscoNodes;

    @Inject
    public GceUnicastHostsProvider(Settings settings, GceComputeService gceComputeService,
            TransportService transportService,
            NetworkService networkService) {
        super(settings);
        this.gceComputeService = gceComputeService;
        this.transportService = transportService;
        this.networkService = networkService;

        this.refreshInterval = componentSettings.getAsTime(Fields.REFRESH,
                settings.getAsTime("cloud.gce." + Fields.REFRESH, TimeValue.timeValueSeconds(5)));

        this.project = componentSettings.get(Fields.PROJECT, settings.get("cloud.gce." + Fields.PROJECT));
        this.zone = componentSettings.get(Fields.ZONE, settings.get("cloud.gce." + Fields.ZONE));

        // Check that we have all needed properties
        checkProperty(Fields.PROJECT, project);
        checkProperty(Fields.ZONE, zone);

        this.tags = settings.getAsArray("discovery.gce.tags");
        if (logger.isDebugEnabled()) {
            logger.debug("using tags {}", Lists.newArrayList(this.tags));
        }
    }

    /**
     * We build the list of Nodes from GCE Management API
     * Information are cached for 5 seconds by default. Modify `plugins.refresh_interval` property if needed.
     * Setting `plugins.refresh_interval` to `-1` will cause infinite caching.
     * Setting `plugins.refresh_interval` to `0` will disable caching.
     */
    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        if (refreshInterval.millis() != 0) {
            if (cachedDiscoNodes != null &&
                    (refreshInterval.millis() < 0 || (System.currentTimeMillis() - lastRefresh) < refreshInterval.millis())) {
                if (logger.isTraceEnabled()) logger.trace("using cache to retrieve node list");
                return cachedDiscoNodes;
            }
            lastRefresh = System.currentTimeMillis();
        }
        logger.debug("start building nodes list using GCE API");

        cachedDiscoNodes = Lists.newArrayList();
        String ipAddress = null;
        try {
            InetAddress inetAddress = networkService.resolvePublishHostAddress(null);
            if (inetAddress != null) {
                ipAddress = inetAddress.getHostAddress();
            }
        } catch (IOException e) {
            // We can't find the publish host address... Hmmm. Too bad :-(
            // We won't simply filter it
        }

        try {
            Compute.Instances.List list = gceComputeService.client().instances().list(project, zone);

            InstanceList instanceList = list.execute();

            for (Instance instance : instanceList.getItems()) {
                String name = instance.getName();
                String type = instance.getMachineType();
                String image = instance.getImage();

                String status = instance.getStatus();

                // We don't want to connect to TERMINATED status instances
                // See https://github.com/elasticsearch/elasticsearch-cloud-gce/issues/3
                if (Status.TERMINATED.equals(status)) {
                    logger.debug("node {} is TERMINATED. Ignoring", name);
                    continue;
                }

                // see if we need to filter by tag
                boolean filterByTag = false;
                if (tags.length > 0) {
                    if (instance.getTags() == null || instance.getTags().isEmpty()) {
                        // If this instance have no tag, we filter it
                        filterByTag = true;
                    } else {
                        // check that all tags listed are there on the instance
                        for (String tag : tags) {
                            boolean found = false;
                            for (String instancetag : instance.getTags().getItems()) {
                                if (instancetag.equals(tag)) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                filterByTag = true;
                                break;
                            }
                        }
                    }
                }
                if (filterByTag) {
                    logger.trace("filtering out instance {} based tags {}, not part of {}", name, tags,
                            instance.getTags().getItems());
                    continue;
                }

                String ip_public = null;
                String ip_private = null;

                List<NetworkInterface> interfaces = instance.getNetworkInterfaces();

                for (NetworkInterface networkInterface : interfaces) {
                    if (ip_public == null) {
                        // Trying to get Public IP Address (For future use)
                        for (AccessConfig accessConfig : networkInterface.getAccessConfigs()) {
                            if (Strings.hasText(accessConfig.getNatIP())) {
                                ip_public = accessConfig.getNatIP();
                                break;
                            }
                        }
                    }

                    if (ip_private == null) {
                        ip_private = networkInterface.getNetworkIP();
                    }

                    // If we have both public and private, we can stop here
                    if (ip_private != null && ip_public != null) break;
                }

                try {
                    if (ip_private.equals(ipAddress)) {
                        // We found the current node.
                        // We can ignore it in the list of DiscoveryNode
                        logger.trace("current node found. Ignoring {} - {}", name, ip_private);
                    } else {
                        TransportAddress[] addresses = transportService.addressesFromString(ip_private);
                        // we only limit to 1 addresses, makes no sense to ping 100 ports
                        for (int i = 0; (i < addresses.length && i < UnicastZenPing.LIMIT_PORTS_COUNT); i++) {
                            logger.trace("adding {}, type {}, image {}, address {}, transport_address {}, status {}", name, type,
                                    image, ip_private, addresses[i], status);
                            cachedDiscoNodes.add(new DiscoveryNode("#cloud-" + name + "-" + i, addresses[i]));
                        }
                    }
                } catch (Exception e) {
                    logger.warn("failed to add {}, address {}", e, name, ip_private);
                }

            }
        } catch (IOException e) {
            logger.warn("Exception caught during discovery {} : {}", e.getClass().getName(), e.getMessage());
            logger.trace("Exception caught during discovery", e);
        }

        logger.debug("using dynamic discovery nodes {}", cachedDiscoNodes);
        return cachedDiscoNodes;
    }

    private boolean checkProperty(String name, String value) {
        if (!Strings.hasText(value)) {
            logger.warn("cloud.gce.{} is not set.", name);
            return false;
        }
        return true;
    }
}
