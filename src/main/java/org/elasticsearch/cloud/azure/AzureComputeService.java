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

package org.elasticsearch.cloud.azure;

import java.util.Set;

/**
 *
 */
public interface AzureComputeService {

    static public final class Fields {
        public static final String SUBSCRIPTION_ID = "subscription_id";
        public static final String SERVICE_NAME = "service_name";
        public static final String KEYSTORE = "keystore";
        public static final String PASSWORD = "password";
        public static final String REFRESH = "refresh_interval";
        public static final String PORT_NAME = "port_name";
        public static final String HOST_TYPE = "host_type";
    }

    public Set<Instance> instances();
}
