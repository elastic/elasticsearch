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

import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;

/**
 *
 */
public interface AzureComputeService {

    static public final class Management {
        public static final String API_IMPLEMENTATION = "cloud.azure.management.api.impl";

        public static final String SUBSCRIPTION_ID = "cloud.azure.management.subscription.id";
        public static final String SERVICE_NAME = "cloud.azure.management.cloud.service.name";

        // Keystore settings
        public static final String KEYSTORE_PATH = "cloud.azure.management.keystore.path";
        public static final String KEYSTORE_PASSWORD = "cloud.azure.management.keystore.password";
        public static final String KEYSTORE_TYPE = "cloud.azure.management.keystore.type";
    }

    static public final class Discovery {
        public static final String REFRESH = "discovery.azure.refresh_interval";

        public static final String HOST_TYPE = "discovery.azure.host.type";
        public static final String ENDPOINT_NAME = "discovery.azure.endpoint.name";
        public static final String DEPLOYMENT_NAME = "discovery.azure.deployment.name";
        public static final String DEPLOYMENT_SLOT = "discovery.azure.deployment.slot";
    }
    public HostedServiceGetDetailedResponse getServiceDetails();
}
