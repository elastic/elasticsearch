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

import java.io.IOException;
import java.util.List;

/**
 * Static implementation of {@link ConnectionPool}. Its underlying list of connections is immutable.
 */
public class StaticConnectionPool extends AbstractStaticConnectionPool {

    private final List<Connection> connections;

    public StaticConnectionPool(HttpHost... hosts) {
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("no hosts provided");
        }
        this.connections = createConnections(hosts);
    }

    @Override
    protected List<Connection> getConnections() {
        return connections;
    }

    @Override
    public void close() throws IOException {
        //no-op nothing to close
    }
}
