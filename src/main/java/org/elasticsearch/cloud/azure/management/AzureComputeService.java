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

    static public final class Fields {
        // Deprecated azure management cloud settings
        @Deprecated
        public static final String SUBSCRIPTION_ID_DEPRECATED = "subscription_id";
        @Deprecated
        public static final String SERVICE_NAME_DEPRECATED = "service_name";
        @Deprecated
        public static final String KEYSTORE_DEPRECATED = "keystore";
        @Deprecated
        public static final String PASSWORD_DEPRECATED = "password";
        @Deprecated
        public static final String PORT_NAME_DEPRECATED = "port_name";
        @Deprecated
        public static final String HOST_TYPE_DEPRECATED = "host_type";

        public static final String SUBSCRIPTION_ID = "subscription.id";
        public static final String SERVICE_NAME = "cloud.service.name";

        // Keystore settings
        public static final String KEYSTORE_PATH = "keystore.path";
        public static final String KEYSTORE_PASSWORD = "keystore.password";
        public static final String KEYSTORE_TYPE = "keystore.type";

        public static final String REFRESH = "refresh_interval";

        public static final String HOST_TYPE = "host.type";
        public static final String ENDPOINT_NAME = "endpoint.name";
        public static final String DEPLOYMENT_NAME = "deployment.name";
        public static final String DEPLOYMENT_SLOT = "deployment.slot";
    }

    public HostedServiceGetDetailedResponse getServiceDetails();
}
