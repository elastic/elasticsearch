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

package org.elasticsearch.cloud.azure;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.Version;
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
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.azure.management.AzureManagementApi;
import org.jclouds.azure.management.AzureManagementApiMetadata;
import org.jclouds.azure.management.AzureManagementAsyncApi;
import org.jclouds.azure.management.config.AzureManagementProperties;
import org.jclouds.azure.management.domain.Deployment;
import org.jclouds.azure.management.domain.HostedServiceWithDetailedProperties;
import org.jclouds.logging.LoggingModules;
import org.jclouds.rest.RestContext;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import static org.elasticsearch.common.Strings.cleanPath;

/**
 *
 */
public class AzureComputeService extends AbstractLifecycleComponent<AzureComputeService> {

    static final class Fields {
        private static final String ENDPOINT = "https://management.core.windows.net/";
        private static final String VERSION = "2012-08-01";
        private static final String SUBSCRIPTION_ID = "subscription_id";
        private static final String PASSWORD = "password";
        private static final String CERTIFICATE = "certificate";
        private static final String PRIVATE_KEY = "private_key";
    }

    private List<DiscoveryNode> discoNodes;
    private TransportService transportService;
    private NetworkService networkService;

    @Inject
    public AzureComputeService(Settings settings, SettingsFilter settingsFilter, TransportService transportService,
                               NetworkService networkService) {
        super(settings);
        settingsFilter.addFilter(new AzureSettingsFilter());
        this.transportService = transportService;
        this.networkService = networkService;
    }

    /**
     * We build the list of Nodes from Azure Management API
     * @param client Azure Client
     */
    private List<DiscoveryNode> buildNodes(RestContext<AzureManagementApi, AzureManagementAsyncApi> client) {
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

        Set<HostedServiceWithDetailedProperties> response = client.getApi().getHostedServiceApi().list();

        for (HostedServiceWithDetailedProperties hostedService : response) {
            // Ask Azure for each IP address
            Deployment deployment = client.getApi().getHostedServiceApi().getDeployment(hostedService.getName(), hostedService.getName());
            if (deployment != null) {
                try {
                    if (deployment.getPrivateIpAddress().equals(ipAddress) ||
                            deployment.getPublicIpAddress().equals(ipAddress)) {
                        // We found the current node.
                        // We can ignore it in the list of DiscoveryNode
                        // We can now set the public Address as the publish address (if not already set)
                        String publishHost = settings.get("transport.publish_host", settings.get("transport.host"));
                        if (!Strings.hasText(publishHost) || !deployment.getPublicIpAddress().equals(publishHost)) {
                            logger.info("you should define publish_host with {}", deployment.getPublicIpAddress());
                        }
                    } else {
                        TransportAddress[] addresses = transportService.addressesFromString(deployment.getPublicIpAddress());
                        // we only limit to 1 addresses, makes no sense to ping 100 ports
                        for (int i = 0; (i < addresses.length && i < UnicastZenPing.LIMIT_PORTS_COUNT); i++) {
                            logger.trace("adding {}, address {}, transport_address {}", hostedService.getName(), deployment.getPublicIpAddress(), addresses[i]);
                            discoNodes.add(new DiscoveryNode("#cloud-" + hostedService.getName() + "-" + i, addresses[i], Version.CURRENT));
                        }
                    }
                } catch (Exception e) {
                    logger.warn("failed to add {}, address {}", e, hostedService.getName(), deployment.getPublicIpAddress());
                }
            } else {
                logger.trace("ignoring {}", hostedService.getName());
            }
        }

        return discoNodes;
    }

    public synchronized List<DiscoveryNode> nodes() {
        if (this.discoNodes != null) {
            return this.discoNodes;
        }

        String PK8_PATH = componentSettings.get(Fields.PRIVATE_KEY, settings.get("cloud." + Fields.PRIVATE_KEY));
        String CERTIFICATE_PATH = componentSettings.get(Fields.CERTIFICATE, settings.get("cloud." + Fields.CERTIFICATE));
        String PASSWORD = componentSettings.get(Fields.PASSWORD, settings.get("cloud" + Fields.PASSWORD, ""));
        String SUBSCRIPTION_ID = componentSettings.get(Fields.SUBSCRIPTION_ID, settings.get("cloud." + Fields.SUBSCRIPTION_ID));

        // Check that we have all needed properties
        if (!checkProperty(Fields.SUBSCRIPTION_ID, SUBSCRIPTION_ID)) return null;
        if (!checkProperty(Fields.CERTIFICATE, CERTIFICATE_PATH)) return null;
        if (!checkProperty(Fields.PRIVATE_KEY, PK8_PATH)) return null;

        // Reading files from local disk
        String pk8 = readFromFile(cleanPath(PK8_PATH));
        String cert = readFromFile(cleanPath(CERTIFICATE_PATH));

        // Check file content
        if (!checkProperty(Fields.CERTIFICATE, cert)) return null;
        if (!checkProperty(Fields.PRIVATE_KEY, pk8)) return null;

        String IDENTITY = pk8 + cert;

        // We set properties used to create an Azure client
        Properties overrides = new Properties();
        overrides.setProperty(Constants.PROPERTY_TRUST_ALL_CERTS, "true");
        overrides.setProperty(Constants.PROPERTY_RELAX_HOSTNAME, "true");
        overrides.setProperty("azure-management.identity", IDENTITY);
        overrides.setProperty("azure-management.credential", PASSWORD);
        overrides.setProperty("azure-management.endpoint", Fields.ENDPOINT + SUBSCRIPTION_ID);
        overrides.setProperty("azure-management.api-version", Fields.VERSION);
        overrides.setProperty("azure-management.build-version", "");
        overrides.setProperty(AzureManagementProperties.SUBSCRIPTION_ID, SUBSCRIPTION_ID);

        RestContext<AzureManagementApi, AzureManagementAsyncApi> client = null;

        try {
            client = ContextBuilder.newBuilder("azure-management")
                    .modules(ImmutableSet.<Module>of(LoggingModules.firstOrJDKLoggingModule()))
                    .overrides(overrides)
                    .build(AzureManagementApiMetadata.CONTEXT_TOKEN);
            logger.debug("starting Azure discovery service for [{}]", SUBSCRIPTION_ID);

            this.discoNodes = buildNodes(client);
        } catch (Throwable t) {
            logger.warn("error while trying to find nodes for azure service [{}]: {}", SUBSCRIPTION_ID, t.getMessage());
            logger.debug("error found is: ", t);
            // We create an empty list in that specific case.
            // So discovery process won't fail with NPE but this node won't join any cluster
            this.discoNodes = Lists.newArrayList();
        } finally {
            if (client != null) client.close();
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

    private String readFromFile(String path) {
        try {
            logger.trace("reading file content from [{}]", path);

            StringBuilder text = new StringBuilder();
            String NL = System.getProperty("line.separator");
            Scanner scanner = new Scanner(new FileInputStream(path), "UTF-8");
            try {
                while (scanner.hasNextLine()){
                    text.append(scanner.nextLine() + NL);
                }
                return text.toString();
            }
            finally{
                scanner.close();
            }
        } catch (FileNotFoundException e) {
            logger.trace("file does not exist [{}]", path);
        }

        return null;
    }

    private boolean checkProperty(String name, String value) {
        if (!Strings.hasText(value)) {
            logger.warn("cloud.azure.{} is not set. Disabling azure discovery.", name);
            return false;
        }
        return true;
    }
}
