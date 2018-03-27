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

import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.http.HttpHost;

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
    private final List<HttpHost> boundHosts;
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
     * Create a {@linkplain Node} with metadata. All parameters except
     * {@code host} are nullable and implementations of {@link NodeSelector}
     * need to decide what to do in their absence.
     */
    public Node(HttpHost host, List<HttpHost> boundHosts, String version, Roles roles) {
        if (host == null) {
            throw new IllegalArgumentException("host cannot be null");
        }
        this.host = host;
        this.boundHosts = boundHosts;
        this.version = version;
        this.roles = roles;
    }

    /**
     * Create a {@linkplain Node} without any metadata.
     */
    public Node(HttpHost host) {
        this(host, null, null, null);
    }

    /**
     * Make a copy of this {@link Node} but replacing its
     * {@link #getHost() host}. Use this when the sniffing implementation
     * returns returns a {@link #getHost() host} that is not useful to the
     * client.
     */
    public Node withHost(HttpHost host) {
        /*
         * If the new host isn't in the bound hosts list we add it so the
         * result looks sane.
         */
        List<HttpHost> boundHosts = this.boundHosts;
        if (false == boundHosts.contains(host)) {
            boundHosts = new ArrayList<>(boundHosts);
            boundHosts.add(host);
            boundHosts = unmodifiableList(boundHosts);
        }
        return new Node(host, boundHosts, version, roles);
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
    public List<HttpHost> getBoundHosts() {
        return boundHosts;
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

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[host=").append(host);
        if (boundHosts != null) {
            b.append(", bound=").append(boundHosts);
        }
        if (version != null) {
            b.append(", version=").append(version);
        }
        if (roles != null) {
            b.append(", roles=").append(roles);
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
            && Objects.equals(version, other.version)
            && Objects.equals(roles, other.roles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, boundHosts, version, roles);
    }

    /**
     * Role information about an Elasticsearch process.
     */
    public static final class Roles {
        private final boolean masterEligible;
        private final boolean data;
        private final boolean ingest;

        public Roles(boolean masterEligible, boolean data, boolean ingest) {
            this.masterEligible = masterEligible;
            this.data = data;
            this.ingest = ingest;
        }

        /**
         * The node <strong>could</strong> be elected master.
         */
        public boolean hasMasterEligible() {
            return masterEligible;
        }
        /**
         * The node stores data.
         */
        public boolean hasData() {
            return data;
        }
        /**
         * The node runs ingest pipelines.
         */
        public boolean hasIngest() {
            return ingest;
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder(3);
            if (masterEligible) result.append('m');
            if (data) result.append('d');
            if (ingest) result.append('i');
            return result.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Roles other = (Roles) obj;
            return masterEligible == other.masterEligible
                && data == other.data
                && ingest == other.ingest;
        }

        @Override
        public int hashCode() {
            return Objects.hash(masterEligible, data, ingest);
        }
    }
}
