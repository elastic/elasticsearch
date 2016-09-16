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
package org.elasticsearch.plugins;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;

import java.util.Collections;
import java.util.List;

/**
 * Plugin for extending network and transport related classes
 */
public interface NetworkPlugin {

    /**
     * Returns a list of {@link TransportInterceptor} instances that are used to intercept incoming and outgoing
     * transport (inter-node) requests. This must not return <code>null</code>
     */
    default List<TransportInterceptor> getTransportInterceptors() {
        return Collections.emptyList();
    }

    /**
     * Returns a list of {@link TransportFactory} instances that are used to create the nodes {@link Transport} implementation.
     * See {@link org.elasticsearch.common.network.NetworkModule#TRANSPORT_TYPE_KEY} to configure a specific implementation.
     */
    default List<TransportFactory<Transport>> getTransportFactory() {
        return Collections.emptyList();
    }

    /**
     * Returns a list of {@link TransportFactory} instances that are used to create the nodes {@link HttpServerTransport} implementation.
     * See {@link org.elasticsearch.common.network.NetworkModule#HTTP_TYPE_SETTING} to configure a specific implementation.
     */
    default List<TransportFactory<HttpServerTransport>> getHttpTransportFactory() {
        return Collections.emptyList();
    }

    abstract class TransportFactory<Transport> {
        private final String type;

        protected TransportFactory(String type) {
            this.type = type;
        }

        /**
         * The transport type name that use used to configure the transport implementation.
         * @see org.elasticsearch.common.network.NetworkModule#HTTP_TYPE_SETTING
         * @see org.elasticsearch.common.network.NetworkModule#TRANSPORT_TYPE_SETTING
         */
        public final String getType() {
            return this.type;
        }

        /**
         * Creates a new transport implementation. This method is only invoked for the configure transport type.
         */
        public abstract Transport createTransport(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                  CircuitBreakerService circuitBreakerService,
                                                  NamedWriteableRegistry namedWriteableRegistry,
                                                  NetworkService networkService);
    }
}
