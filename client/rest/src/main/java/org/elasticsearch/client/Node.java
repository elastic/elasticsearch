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

import java.util.Objects;

import org.apache.http.HttpHost;

public final class Node {
    interface Roles {
        boolean master();
        boolean data();
        boolean ingest();
    }

    public static Node build(HttpHost host) {
        return build(host, null);
    }

    public static Node build(HttpHost host, Roles roles) {
        return new Node(host, roles);
    }

    private final HttpHost host;
    private final Roles roles;

    private Node(HttpHost host, Roles roles) {
        this.host = Objects.requireNonNull(host, "host cannot be null");
        this.roles = roles;
    }

    public HttpHost host() {
        return host;
    }

    public Roles roles() {
        return roles;
    }

    @Override
    public String toString() {
        return host.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Node other = (Node) obj;
        return host.equals(other.host);
    }

    @Override
    public int hashCode() {
        return host.hashCode();
    }
}
