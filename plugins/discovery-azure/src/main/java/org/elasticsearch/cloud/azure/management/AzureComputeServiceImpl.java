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

package org.elasticsearch.cloud.azure.management;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.utils.KeyStoreType;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.management.compute.ComputeManagementClient;
import com.microsoft.windowsazure.management.compute.ComputeManagementService;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import com.microsoft.windowsazure.management.compute.models.HostedServiceListResponse;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cloud.azure.AzureServiceDisableException;
import org.elasticsearch.cloud.azure.AzureServiceRemoteException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.elasticsearch.cloud.azure.management.AzureComputeService.Management.*;

/**
 *
 */
public class AzureComputeServiceImpl implements AzureComputeService {

    static final class Azure {
        private static final String ENDPOINT = "https://management.core.windows.net/";
    }

    private final ESLogger logger;
    private ComputeManagementClient computeManagementClient = null;
    private final String serviceName;
    private final String subscriptionId;
    private final String keystorePath;
    private final String keystorePassword;
    private final KeyStoreType keystoreType;

    public AzureComputeServiceImpl(Settings settings) {
        this.logger = Loggers.getLogger(getClass(), settings);
        subscriptionId = settings.get(SUBSCRIPTION_ID);
        serviceName = settings.get(Management.SERVICE_NAME);
        keystorePath = settings.get(KEYSTORE_PATH);
        keystorePassword = settings.get(KEYSTORE_PASSWORD);
        String strKeyStoreType = settings.get(KEYSTORE_TYPE, KeyStoreType.pkcs12.name());
        KeyStoreType tmpKeyStoreType = KeyStoreType.pkcs12;
        try {
            tmpKeyStoreType = KeyStoreType.fromString(strKeyStoreType);
        } catch (Exception e) {
            logger.warn("wrong value for [{}]: [{}]. falling back to [{}]...", KEYSTORE_TYPE,
                    strKeyStoreType, KeyStoreType.pkcs12.name());
        }
        keystoreType = tmpKeyStoreType;
    }

    @Override
    public HostedServiceGetDetailedResponse getServiceDetails() {
        if (computeManagementClient == null) {
            // Azure plugin is disabled
            throw new AzureServiceDisableException("azure plugin is disabled.");
        }

        try {
            return computeManagementClient.getHostedServicesOperations().getDetailed(serviceName);
        } catch (Exception e) {
            throw new AzureServiceRemoteException("can not get list of azure nodes", e);
        }
    }

    public void configure() {
        logger.trace("creating new Azure client for [{}], [{}]", subscriptionId, serviceName);

        try {
            Configuration configuration = ManagementConfiguration.configure(new URI(Azure.ENDPOINT),
                subscriptionId, keystorePath, keystorePassword, keystoreType);
            computeManagementClient = ComputeManagementService.create(configuration);
        } catch (IOException|URISyntaxException e) {
            logger.error("can not start azure client: {}", e.getMessage());
            throw new ElasticsearchException("can not start azure client", e);
        }

        // That's a shame. Apparently ComputeManagementService catches and traces a FileNotFoundException
        // but swallows it.
        // So for now we send a request which will throw an error and stop elasticsearch
        // TODO Remove this if https://github.com/Azure/azure-sdk-for-java/issues/565 is fixed
        try {
            logger.debug("checking azure client...");
            HostedServiceListResponse list = computeManagementClient.getHostedServicesOperations().list();
            if (list.getHostedServices().isEmpty()) {
                // We should have at least one service.
                throw new ElasticsearchException("no hosted service seems to be available for your azure account.");
            }
            logger.debug("Known hosted services for your azure account:");
            boolean serviceFound = false;
            for (HostedServiceListResponse.HostedService hostedService : list) {
                logger.debug("- {}", hostedService.getServiceName());
                if (hostedService.getServiceName().equals(serviceName)) {
                    serviceFound = true;
                }
            }

            if (serviceFound == false) {
                throw new ElasticsearchException("[{}] does not exist for your azure account. " +
                    "To list available services, run in debug mode or check your azure console at https://portal.azure.com/.",
                    serviceName);
            }
        } catch (ServiceException|ParserConfigurationException|URISyntaxException|SAXException|IOException e) {
            // Logging here the message only. The full stacktrace is then printed because
            // AzureDiscoveryModule won't start and guice will print the full stack.
            logger.error("can not start azure client. Check your elasticsearch configuration. {}", e.getMessage());
            throw new ElasticsearchException("can not start azure client. Check your elasticsearch configuration. {}", e, e.getMessage());
        }
    }

    @Override
    public void stop() {
        if (computeManagementClient != null) {
            try {
                computeManagementClient.close();
            } catch (IOException e) {
                logger.error("error while closing Azure client", e);
            }
        }
    }
}
