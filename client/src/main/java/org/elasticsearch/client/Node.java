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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents an Elasticsearch node.
 * Holds its http address as an {@link HttpHost} instance, as well as its optional set of roles and attributes.
 * Roles and attributes can be populated in one of the two following ways:
 * 1) using a connection pool that supports sniffing, so that all the info is retrieved from elasticsearch itself
 * 2) manually passing the info through the {@link #Node(HttpHost, Set, Map)} constructor
 * Roles and attributes may be taken into account as part of connection selection by the connection pool, which
 * can be customized by passing in a predicate at connection pool creation.
 */
public class Node {
    private final HttpHost httpHost;
    private final Set<Role> roles;
    private final Map<String, String> attributes;

    /**
     * Creates a node given its http address as an {@link HttpHost} instance.
     * Roles are not provided hence all possible roles will be assumed, as that is the default in Elasticsearch.
     * No attributes will be associated with the node.
     *
     * @param httpHost the http address of the node
     */
    public Node(HttpHost httpHost) {
        this(httpHost, new HashSet<>(Arrays.asList(Role.values())), Collections.emptyMap());
    }

    /**
     * Creates a node given its http address as an {@link HttpHost} instance, its set or roles and attributes.
     *
     * @param httpHost the http address of the node
     * @param roles the set of roles that the node fulfills within the cluster
     * @param attributes the attributes associated with the node
     */
    public Node(HttpHost httpHost, Set<Role> roles, Map<String, String> attributes) {
        Objects.requireNonNull(httpHost, "host cannot be null");
        Objects.requireNonNull(roles, "roles cannot be null");
        Objects.requireNonNull(attributes, "attributes cannot be null");
        this.httpHost = httpHost;
        this.roles = Collections.unmodifiableSet(roles);
        this.attributes = Collections.unmodifiableMap(attributes);
    }

    /**
     * Returns the http address of the node
     */
    public HttpHost getHttpHost() {
        return httpHost;
    }

    /**
     * Returns the set of roles associated with the node
     */
    public Set<Role> getRoles() {
        return roles;
    }

    /**
     * Returns the set of attributes associated with the node
     */
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return "Node{" +
                "httpHost=" + httpHost +
                ", roles=" + roles +
                ", attributes=" + attributes +
                '}';
    }

    /**
     * Holds all the potential roles that a node can fulfill within a cluster
     */
    public enum Role {
        /**
         * Data node
         */
        DATA,
        /**
         * Master eligible node
         */
        MASTER,
        /**
         * Ingest node
         */
        INGEST;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
