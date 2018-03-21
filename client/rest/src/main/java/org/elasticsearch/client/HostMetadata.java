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

public class HostMetadata {
    public static final HostMetadataResolver EMPTY_RESOLVER = new HostMetadataResolver() {
        @Override
        public HostMetadata resolveMetadata(HttpHost host) {
            return null;
        }
    };
    /**
     * Look up metadata about the provided host. Implementers should not make network
     * calls, instead, they should look up previously fetched data. See Elasticsearch's
     * Sniffer for an example implementation.
     */
    public interface HostMetadataResolver {
        /**
         * @return {@link HostMetadat} about the provided host if we have any
         * metadata, {@code null} otherwise
         */
        HostMetadata resolveMetadata(HttpHost host);
    }

    private final String version;
    private final Roles roles;

    public HostMetadata(String version, Roles roles) {
        this.version = Objects.requireNonNull(version, "version is required");
        this.roles = Objects.requireNonNull(roles, "roles is required");
    }

    /**
     * Version of the node.
     */
    public String version() {
        return version;
    }

    /**
     * Roles the node is implementing.
     */
    public Roles roles() {
        return roles;
    }

    @Override
    public String toString() {
        return "[version=" + version + ", roles=" + roles + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        HostMetadata other = (HostMetadata) obj;
        return version.equals(other.version)
            && roles.equals(other.roles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, roles);
    }

    public static final class Roles {
        private final boolean master;
        private final boolean data;
        private final boolean ingest;

        public Roles(boolean master, boolean data, boolean ingest) {
            this.master = master;
            this.data = data;
            this.ingest = ingest;
        }

        /**
         * The node <strong>could</strong> be elected master.
         */
        public boolean master() {
            return master;
        }
        /**
         * The node stores data.
         */
        public boolean data() {
            return data;
        }
        /**
         * The node runs ingest pipelines.
         */
        public boolean ingest() {
            return ingest;
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder(3);
            if (master) result.append('m');
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
            return master == other.master
                && data == other.data
                && ingest == other.ingest;
        }

        @Override
        public int hashCode() {
            return Objects.hash(master, data, ingest);
        }
    }
}
