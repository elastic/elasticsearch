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

package org.elasticsearch.client;

import org.apache.http.HttpHost;

/**
 * Simplest representation of a connection to an elasticsearch node.
 * It doesn't have any mutable state. It holds the host that the connection points to.
 * Allows the transport to deal with very simple connection objects that are immutable.
 * Any change to the state of connections should be made through the connection pool
 * which is aware of the connection object that it supports.
 */
public class Connection {
    private final HttpHost host;

    /**
     * Creates a new connection pointing to the provided {@link HttpHost} argument
     */
    public Connection(HttpHost host) {
            this.host = host;
        }

    /**
     * Returns the {@link HttpHost} that the connection points to
     */
    public HttpHost getHost() {
            return host;
        }
}
