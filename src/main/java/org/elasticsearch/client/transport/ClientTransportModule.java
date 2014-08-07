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

package org.elasticsearch.client.transport;

import org.elasticsearch.client.support.Headers;
import org.elasticsearch.client.transport.support.InternalTransportAdminClient;
import org.elasticsearch.client.transport.support.InternalTransportClient;
import org.elasticsearch.client.transport.support.InternalTransportClusterAdminClient;
import org.elasticsearch.client.transport.support.InternalTransportIndicesAdminClient;
import org.elasticsearch.common.inject.AbstractModule;

/**
 *
 */
public class ClientTransportModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Headers.class).asEagerSingleton();
        bind(InternalTransportClient.class).asEagerSingleton();
        bind(InternalTransportAdminClient.class).asEagerSingleton();
        bind(InternalTransportIndicesAdminClient.class).asEagerSingleton();
        bind(InternalTransportClusterAdminClient.class).asEagerSingleton();
        bind(TransportClientNodesService.class).asEagerSingleton();
    }
}
