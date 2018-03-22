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

package org.elasticsearch.client.sniff;

import org.apache.http.HttpHost;
import org.elasticsearch.client.HostMetadata;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of sniffing hosts.
 */
public final class SnifferResult {
    /**
     * List of all nodes in the cluster.
     */
    private final List<HttpHost> hosts;
    /**
     * Map from each address that each node is listening on to metadata about
     * the node.
     */
    private final Map<HttpHost, HostMetadata> hostMetadata;

    public SnifferResult(List<HttpHost> hosts, Map<HttpHost, HostMetadata> hostMetadata) {
        this.hosts = Objects.requireNonNull(hosts, "hosts is required");
        this.hostMetadata = Objects.requireNonNull(hostMetadata, "hostMetadata is required");
    }

    /**
     * List of all nodes in the cluster.
     */
    public List<HttpHost> hosts() {
        return hosts;
    }

    /**
     * Map from each address that each node is listening on to metadata about
     * the node.
     */
    public Map<HttpHost, HostMetadata> hostMetadata() {
        return hostMetadata;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        SnifferResult other = (SnifferResult) obj;
        return hosts.equals(other.hosts)
            && hostMetadata.equals(other.hostMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hosts, hostMetadata);
    }
}
