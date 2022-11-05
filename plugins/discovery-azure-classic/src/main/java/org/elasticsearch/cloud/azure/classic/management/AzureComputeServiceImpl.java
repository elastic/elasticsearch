/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cloud.azure.classic.management;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.Builder;
import com.microsoft.windowsazure.core.DefaultBuilder;
import com.microsoft.windowsazure.core.utils.KeyStoreType;
import com.microsoft.windowsazure.management.compute.ComputeManagementClient;
import com.microsoft.windowsazure.management.compute.ComputeManagementService;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cloud.azure.classic.AzureServiceRemoteException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ServiceLoader;

public class AzureComputeServiceImpl extends AbstractLifecycleComponent implements AzureComputeService {
    private static final Logger logger = LogManager.getLogger(AzureComputeServiceImpl.class);

    private final ComputeManagementClient client;
    private final String serviceName;

    public AzureComputeServiceImpl(Settings settings) {
        String subscriptionId = getRequiredSetting(settings, Management.SUBSCRIPTION_ID_SETTING);

        serviceName = getRequiredSetting(settings, Management.SERVICE_NAME_SETTING);
        String keystorePath = getRequiredSetting(settings, Management.KEYSTORE_PATH_SETTING);
        String keystorePassword = getRequiredSetting(settings, Management.KEYSTORE_PASSWORD_SETTING);
        KeyStoreType keystoreType = Management.KEYSTORE_TYPE_SETTING.get(settings);

        logger.trace("creating new Azure client for [{}], [{}]", subscriptionId, serviceName);
        try {
            // Azure SDK configuration uses DefaultBuilder which uses java.util.ServiceLoader to load the
            // various Azure services. By default, this will use the current thread's context classloader
            // to load services. Since the current thread refers to the main application classloader it
            // won't find any Azure service implementation.

            // Here we basically create a new DefaultBuilder that uses the current class classloader to load services.
            DefaultBuilder builder = new DefaultBuilder();
            for (Builder.Exports exports : ServiceLoader.load(Builder.Exports.class, getClass().getClassLoader())) {
                exports.register(builder);
            }

            // And create a new blank configuration based on the previous DefaultBuilder
            Configuration configuration = new Configuration(builder);
            configuration.setProperty(Configuration.PROPERTY_LOG_HTTP_REQUESTS, logger.isTraceEnabled());

            Configuration managementConfig = ManagementConfiguration.configure(
                null,
                configuration,
                Management.ENDPOINT_SETTING.get(settings),
                subscriptionId,
                keystorePath,
                keystorePassword,
                keystoreType
            );

            logger.debug("creating new Azure client for [{}], [{}]", subscriptionId, serviceName);
            client = ComputeManagementService.create(managementConfig);
        } catch (IOException e) {
            throw new ElasticsearchException("Unable to configure Azure compute service", e);
        }
    }

    private static String getRequiredSetting(Settings settings, Setting<String> setting) {
        String value = setting.get(settings);
        if (value == null || Strings.hasLength(value) == false) {
            throw new IllegalArgumentException("Missing required setting " + setting.getKey() + " for azure");
        }
        return value;
    }

    @Override
    public HostedServiceGetDetailedResponse getServiceDetails() {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<HostedServiceGetDetailedResponse>) () -> client.getHostedServicesOperations()
                    .getDetailed(serviceName)
            );
        } catch (PrivilegedActionException e) {
            throw new AzureServiceRemoteException("can not get list of azure nodes", e.getCause());
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {}

    @Override
    protected void doStop() throws ElasticsearchException {}

    @Override
    protected void doClose() throws ElasticsearchException {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                logger.error("error while closing Azure client", e);
            }
        }
    }
}
