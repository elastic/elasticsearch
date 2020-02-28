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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Metadata about an {@link HttpHost} running Elasticsearch.
 */
public class Node {
    /**
     * Address that this host claims is its primary contact point.
     */
    private final HttpHost host;
    /**
     * Addresses on which the host is listening. These are useful to have
     * around because they allow you to find a host based on any address it
     * is listening on.
     */
    private final Set<HttpHost> boundHosts;
    /**
     * Name of the node as configured by the {@code node.name} attribute.
     */
    private final String name;
    /**
     * Version of Elasticsearch that the node is running or {@code null}
     * if we don't know the version.
     */
    private final String version;
    /**
     * Roles that the Elasticsearch process on the host has or {@code null}
     * if we don't know what roles the node has.
     */
    private final Roles roles;
    /**
     * Attributes declared on the node.
     */
    private final Map<String, List<String>> attributes;

    /**
     * Create a {@linkplain Node} with metadata. All parameters except
     * {@code host} are nullable and implementations of {@link NodeSelector}
     * need to decide what to do in their absence.
     */
    public Node(HttpHost host, Set<HttpHost> boundHosts, String name, String version,
            Roles roles, Map<String, List<String>> attributes) {
        if (host == null) {
            throw new IllegalArgumentException("host cannot be null");
        }
        this.host = host;
        this.boundHosts = boundHosts;
        this.name = name;
        this.version = version;
        this.roles = roles;
        this.attributes = attributes;
    }

    /**
     * Create a {@linkplain Node} without any metadata.
     */
    public Node(HttpHost host) {
        this(host, null, null, null, null, null);
    }

    /**
     * Contact information for the host.
     */
    public HttpHost getHost() {
        return host;
    }

    /**
     * Addresses on which the host is listening. These are useful to have
     * around because they allow you to find a host based on any address it
     * is listening on.
     */
    public Set<HttpHost> getBoundHosts() {
        return boundHosts;
    }

    /**
     * The {@code node.name} of the node.
     */
    public String getName() {
        return name;
    }

    /**
     * Version of Elasticsearch that the node is running or {@code null}
     * if we don't know the version.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Roles that the Elasticsearch process on the host has or {@code null}
     * if we don't know what roles the node has.
     */
    public Roles getRoles() {
        return roles;
    }

    /**
     * Attributes declared on the node.
     */
    public Map<String, List<String>> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[host=").append(host);
        if (boundHosts != null) {
            b.append(", bound=").append(boundHosts);
        }
        if (name != null) {
            b.append(", name=").append(name);
        }
        if (version != null) {
            b.append(", version=").append(version);
        }
        if (roles != null) {
            b.append(", roles=").append(roles);
        }
        if (attributes != null) {
            b.append(", attributes=").append(attributes);
        }
        return b.append(']').toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Node other = (Node) obj;
        return host.equals(other.host)
            && Objects.equals(boundHosts, other.boundHosts)
            && Objects.equals(name, other.name)
            && Objects.equals(version, other.version)
            && Objects.equals(roles, other.roles)
            && Objects.equals(attributes, other.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, boundHosts, name, version, roles, attributes);
    }

    /**
     * Role information about an Elasticsearch process.
     */
    public static final class Roles {

        private final Set<String> roles;

        public Roles(final Set<String> roles) {
            this.roles = new TreeSet<>(roles);
        }

        /**
         * Teturns whether or not the node <strong>could</strong> be elected master.
         */
        public boolean isMasterEligible() {
            return roles.contains("master");
        }
        /**
         * Teturns whether or not the node stores data.
         */
        public boolean isData() {
            return roles.contains("data");
        }
        /**
         * Teturns whether or not the node runs ingest pipelines.
         */
        public boolean isIngest() {
            return roles.contains("ingest");
        }

        @Override
        public String toString() {
            return String.join(",", roles);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Roles other = (Roles) obj;
            return roles.equals(other.roles);
        }

        @Override
        public int hashCode() {
            return roles.hashCode();
        }

    }
}
