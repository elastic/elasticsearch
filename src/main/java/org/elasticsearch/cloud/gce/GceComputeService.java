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

package org.elasticsearch.cloud.gce;

import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.NetworkInterface;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 *
 */
public class GceComputeService extends AbstractLifecycleComponent<GceComputeService> {

    static final class Fields {
        private static final String PROJECT = "project_id";
        private static final String ZONE = "zone";
        private static final String VERSION = "Elasticsearch/GceCloud/1.0";
    }

    private List<DiscoveryNode> discoNodes;
    private TransportService transportService;
    private NetworkService networkService;
    private Compute compute;

    /** Global instance of the HTTP transport. */
    private static HttpTransport HTTP_TRANSPORT;

    /** Global instance of the JSON factory. */
    private static JsonFactory JSON_FACTORY;

    @Inject
    public GceComputeService(Settings settings, SettingsFilter settingsFilter, TransportService transportService,
                             NetworkService networkService) {
        super(settings);
        settingsFilter.addFilter(new GceSettingsFilter());
        this.transportService = transportService;
        this.networkService = networkService;
    }

    /**
     * We build the list of Nodes from GCE Management API
     * @param project
     * @param zone
     */
    private List<DiscoveryNode> buildNodes(String project, String zone) throws IOException {

        List<DiscoveryNode> discoNodes = Lists.newArrayList();
        String ipAddress = null;
        try {
            InetAddress inetAddress = networkService.resolvePublishHostAddress(null);
            if (inetAddress != null) {
                ipAddress = inetAddress.getHostAddress();
            }
        } catch (IOException e) {
            // We can't find the publish host address... Hmmm. Too bad :-(
        }

        Compute.Instances.List list = compute.instances().list(project, zone);

        InstanceList instanceList = list.execute();

        for (Instance instance : instanceList.getItems()) {
            String name = instance.getName();
            String type = instance.getMachineType();
            String image = instance.getImage();

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
                    logger.debug("current node found. Ignoring {}", ip_private);
                } else {
                    TransportAddress[] addresses = transportService.addressesFromString(ip_private);
                    // we only limit to 1 addresses, makes no sense to ping 100 ports
                    for (int i = 0; (i < addresses.length && i < UnicastZenPing.LIMIT_PORTS_COUNT); i++) {
                        logger.trace("adding {}, address {}, transport_address {}", name, ip_private, addresses[i]);
                        discoNodes.add(new DiscoveryNode("#cloud-" + name + "-" + i, addresses[i]));
                    }
                }
            } catch (Exception e) {
                logger.warn("failed to add {}, address {}", e, name, ip_private);
            }

        }


        return discoNodes;
    }

    public synchronized List<DiscoveryNode> nodes() {
        if (this.discoNodes != null) {
            return this.discoNodes;
        }
        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        } catch (GeneralSecurityException e) {

        } catch (IOException e) {

        }
        JSON_FACTORY = new JacksonFactory();


        String project = componentSettings.get(Fields.PROJECT, settings.get("cloud." + Fields.PROJECT));
        String zone = componentSettings.get(Fields.ZONE, settings.get("cloud." + Fields.ZONE));

        // Check that we have all needed properties
        if (!checkProperty(Fields.PROJECT, project)) return null;
        if (!checkProperty(Fields.ZONE, zone)) return null;

        try {
            logger.debug("starting GCE discovery service for project [{}] on zone [{}]", project, zone);
            ComputeCredential credential = new ComputeCredential.Builder(HTTP_TRANSPORT, JSON_FACTORY).build();
            credential.refreshToken();

            logger.debug("token [{}] will expire in [{}] s", credential.getAccessToken(), credential.getExpiresInSeconds());

            // Once done, let's use this token
            compute = new Compute.Builder(HTTP_TRANSPORT, JSON_FACTORY, null)
                    .setApplicationName(Fields.VERSION)
                    .setHttpRequestInitializer(credential)
                    .build();

            this.discoNodes = buildNodes(project, zone);
        } catch (Throwable t) {
            logger.warn("error while trying to find nodes for GCE service [{}]: {}: {}", project, t.getClass().getName(),
                    t.getMessage());
            logger.debug("error found is: ", t);
            // We create an empty list in that specific case.
            // So discovery process won't fail with NPE but this node won't join any cluster
            this.discoNodes = Lists.newArrayList();
        }

        logger.debug("using dynamic discovery nodes {}", discoNodes);
        return this.discoNodes;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    private boolean checkProperty(String name, String value) {
        if (!Strings.hasText(value)) {
            logger.warn("cloud.gce.{} is not set. Disabling gce discovery.", name);
            return false;
        }
        return true;
    }
}
