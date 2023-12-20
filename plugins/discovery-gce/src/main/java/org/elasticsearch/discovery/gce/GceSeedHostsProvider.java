/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.gce;

import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.NetworkInterface;

import org.elasticsearch.cloud.gce.GceInstancesService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

public class GceSeedHostsProvider implements SeedHostsProvider {

    private static final Logger logger = LogManager.getLogger(GceSeedHostsProvider.class);

    /**
     * discovery.gce.tags: The gce discovery can filter machines to include in the cluster based on tags.
     */
    public static final Setting<List<String>> TAGS_SETTING = Setting.stringListSetting("discovery.gce.tags", Property.NodeScope);

    static final class Status {
        private static final String TERMINATED = "TERMINATED";
    }

    private final Settings settings;
    private final GceInstancesService gceInstancesService;
    private TransportService transportService;
    private NetworkService networkService;

    private final String project;
    private final List<String> zones;
    private final List<String> tags;

    private final TimeValue refreshInterval;
    private long lastRefresh;
    private List<TransportAddress> cachedDynamicHosts;

    public GceSeedHostsProvider(
        Settings settings,
        GceInstancesService gceInstancesService,
        TransportService transportService,
        NetworkService networkService
    ) {
        this.settings = settings;
        this.gceInstancesService = gceInstancesService;
        this.transportService = transportService;
        this.networkService = networkService;

        this.refreshInterval = GceInstancesService.REFRESH_SETTING.get(settings);
        this.project = gceInstancesService.projectId();
        this.zones = gceInstancesService.zones();

        this.tags = TAGS_SETTING.get(settings);
        if (logger.isDebugEnabled()) {
            logger.debug("using tags {}", this.tags);
        }
    }

    /**
     * We build the list of Nodes from GCE Management API
     * Information can be cached using `cloud.gce.refresh_interval` property if needed.
     */
    @Override
    public List<TransportAddress> getSeedAddresses(HostsResolver hostsResolver) {
        // We check that needed properties have been set
        if (this.project == null || this.project.isEmpty() || this.zones == null || this.zones.isEmpty()) {
            throw new IllegalArgumentException(
                "one or more gce discovery settings are missing. "
                    + "Check elasticsearch.yml file. Should have ["
                    + GceInstancesService.PROJECT_SETTING.getKey()
                    + "] and ["
                    + GceInstancesService.ZONE_SETTING.getKey()
                    + "]."
            );
        }

        if (refreshInterval.millis() != 0) {
            if (cachedDynamicHosts != null
                && (refreshInterval.millis() < 0 || (System.currentTimeMillis() - lastRefresh) < refreshInterval.millis())) {
                if (logger.isTraceEnabled()) logger.trace("using cache to retrieve node list");
                return cachedDynamicHosts;
            }
            lastRefresh = System.currentTimeMillis();
        }
        logger.debug("start building nodes list using GCE API");

        cachedDynamicHosts = new ArrayList<>();
        String ipAddress = null;
        try {
            InetAddress inetAddress = networkService.resolvePublishHostAddresses(
                NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY)
            );
            if (inetAddress != null) {
                ipAddress = NetworkAddress.format(inetAddress);
            }
        } catch (IOException e) {
            // We can't find the publish host address... Hmmm. Too bad :-(
            // We won't simply filter it
        }

        try {
            Collection<Instance> instances = gceInstancesService.instances();

            if (instances == null) {
                logger.trace("no instance found for project [{}], zones [{}].", this.project, this.zones);
                return cachedDynamicHosts;
            }

            for (Instance instance : instances) {
                String name = instance.getName();
                String type = instance.getMachineType();

                String status = instance.getStatus();
                logger.trace("gce instance {} with status {} found.", name, status);

                // We don't want to connect to TERMINATED status instances
                // See https://github.com/elastic/elasticsearch-cloud-gce/issues/3
                if (Status.TERMINATED.equals(status)) {
                    logger.debug("node {} is TERMINATED. Ignoring", name);
                    continue;
                }

                // see if we need to filter by tag
                boolean filterByTag = false;
                if (tags.isEmpty() == false) {
                    logger.trace("start filtering instance {} with tags {}.", name, tags);
                    if (instance.getTags() == null
                        || instance.getTags().isEmpty()
                        || instance.getTags().getItems() == null
                        || instance.getTags().getItems().isEmpty()) {
                        // If this instance have no tag, we filter it
                        logger.trace("no tags for this instance but we asked for tags. {} won't be part of the cluster.", name);
                        filterByTag = true;
                    } else {
                        // check that all tags listed are there on the instance
                        logger.trace("comparing instance tags {} with tags filter {}.", instance.getTags().getItems(), tags);
                        for (String tag : tags) {
                            boolean found = false;
                            for (String instancetag : instance.getTags().getItems()) {
                                if (instancetag.equals(tag)) {
                                    found = true;
                                    break;
                                }
                            }
                            if (found == false) {
                                filterByTag = true;
                                break;
                            }
                        }
                    }
                }
                if (filterByTag) {
                    logger.trace(
                        "filtering out instance {} based tags {}, not part of {}",
                        name,
                        tags,
                        instance.getTags() == null || instance.getTags().getItems() == null ? "" : instance.getTags()
                    );
                    continue;
                } else {
                    logger.trace("instance {} with tags {} is added to discovery", name, tags);
                }

                String ip_public = null;
                String ip_private = null;

                List<NetworkInterface> interfaces = instance.getNetworkInterfaces();

                for (NetworkInterface networkInterface : interfaces) {
                    if (ip_public == null) {
                        // Trying to get Public IP Address (For future use)
                        if (networkInterface.getAccessConfigs() != null) {
                            for (AccessConfig accessConfig : networkInterface.getAccessConfigs()) {
                                if (Strings.hasText(accessConfig.getNatIP())) {
                                    ip_public = accessConfig.getNatIP();
                                    break;
                                }
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
                        String address = ip_private;
                        // Test if we have es_port metadata defined here
                        if (instance.getMetadata() != null && instance.getMetadata().containsKey("es_port")) {
                            Object es_port = instance.getMetadata().get("es_port");
                            logger.trace("es_port is defined with {}", es_port);
                            if (es_port instanceof String string) {
                                address = address.concat(":").concat(string);
                            } else {
                                // Ignoring other values
                                logger.trace("es_port is instance of {}. Ignoring...", es_port.getClass().getName());
                            }
                        }

                        // ip_private is a single IP Address. We need to build a TransportAddress from it
                        // If user has set `es_port` metadata, we don't need to ping all ports
                        TransportAddress[] addresses = transportService.addressesFromString(address);

                        for (TransportAddress transportAddress : addresses) {
                            logger.trace(
                                "adding {}, type {}, address {}, transport_address {}, status {}",
                                name,
                                type,
                                ip_private,
                                transportAddress,
                                status
                            );
                            cachedDynamicHosts.add(transportAddress);
                        }
                    }
                } catch (Exception e) {
                    final String finalIpPrivate = ip_private;
                    logger.warn(() -> format("failed to add %s, address %s", name, finalIpPrivate), e);
                }

            }
        } catch (Exception e) {
            logger.warn("exception caught during discovery", e);
        }

        logger.debug("{} addresses added", cachedDynamicHosts.size());
        logger.debug("using transport addresses {}", cachedDynamicHosts);

        return cachedDynamicHosts;
    }
}
