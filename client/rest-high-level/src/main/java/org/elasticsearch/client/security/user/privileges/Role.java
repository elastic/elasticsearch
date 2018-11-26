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

package org.elasticsearch.client.security.user.privileges;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents an aggregation of privileges.
 */
public final class Role {

    public static final ParseField CLUSTER = new ParseField("cluster");
    public static final ParseField GLOBAL = new ParseField("global");
    public static final ParseField INDICES = new ParseField("indices");
    public static final ParseField APPLICATIONS = new ParseField("applications");
    public static final ParseField RUN_AS = new ParseField("run_as");
    public static final ParseField METADATA = new ParseField("metadata");
    public static final ParseField TRANSIENT_METADATA = new ParseField("transient_metadata");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<Role, String> PARSER = new ConstructingObjectParser<>("role_descriptor", false,
        (constructorObjects, roleName) -> {
                // Don't ignore unknown fields. It is dangerous if the object we parse is also
                // part of a request that we build later on, and the fields that we now ignore
                // will end up being implicitly set to null in that request.
                int i = 0;
                final Collection<String> clusterPrivileges = (Collection<String>) constructorObjects[i++];
                final GlobalPrivileges globalApplicationPrivileges = (GlobalPrivileges) constructorObjects[i++];
                final Collection<IndicesPrivileges> indicesPrivileges = (Collection<IndicesPrivileges>) constructorObjects[i++];
                final Collection<ApplicationResourcePrivileges> applicationResourcePrivileges =
                        (Collection<ApplicationResourcePrivileges>) constructorObjects[i++];
                final Collection<String> runAsPrivilege = (Collection<String>) constructorObjects[i++];
                final Map<String, Object> metadata = (Map<String, Object>) constructorObjects[i++];
                final Map<String, Object> transientMetadata = (Map<String, Object>) constructorObjects[i];
            return new Role(roleName, clusterPrivileges, globalApplicationPrivileges, indicesPrivileges, applicationResourcePrivileges,
                    runAsPrivilege, metadata, transientMetadata);
            });

    static {
        PARSER.declareStringArray(optionalConstructorArg(), CLUSTER);
        PARSER.declareObject(optionalConstructorArg(), (parser,c)-> GlobalPrivileges.PARSER.parse(parser,null), GLOBAL);
        PARSER.declareFieldArray(optionalConstructorArg(), (parser,c)->IndicesPrivileges.PARSER.parse(parser,null), INDICES,
            ValueType.OBJECT_ARRAY);
        PARSER.declareFieldArray(optionalConstructorArg(), (parser,c)->ApplicationResourcePrivileges.PARSER.parse(parser,null),
            APPLICATIONS, ValueType.OBJECT_ARRAY);
        PARSER.declareStringArray(optionalConstructorArg(), RUN_AS);
        PARSER.declareObject(constructorArg(), (parser, c) -> parser.map(), METADATA);
        PARSER.declareObject(constructorArg(), (parser, c) -> parser.map(), TRANSIENT_METADATA);
    }

    private final String name;
    private final Set<String> clusterPrivileges;
    private final @Nullable GlobalPrivileges globalApplicationPrivileges;
    private final Set<IndicesPrivileges> indicesPrivileges;
    private final Set<ApplicationResourcePrivileges> applicationResourcePrivileges;
    private final Set<String> runAsPrivilege;
    private final Map<String, Object> metadata;
    private final Map<String, Object> transientMetadata;

    private Role(String name, @Nullable Collection<String> clusterPrivileges,
                 @Nullable GlobalPrivileges globalApplicationPrivileges,
                 @Nullable Collection<IndicesPrivileges> indicesPrivileges,
                 @Nullable Collection<ApplicationResourcePrivileges> applicationResourcePrivileges,
                 @Nullable Collection<String> runAsPrivilege, @Nullable Map<String, Object> metadata,
                 @Nullable Map<String, Object> transientMetadata) {
        if (Strings.hasText(name) == false){
            throw new IllegalArgumentException("role name must be provided");
        } else {
            this.name = name;
        }
        // no cluster privileges are granted unless otherwise specified
        this.clusterPrivileges = Collections
                .unmodifiableSet(clusterPrivileges != null ? new HashSet<>(clusterPrivileges) : Collections.emptySet());
        this.globalApplicationPrivileges = globalApplicationPrivileges;
        // no indices privileges are granted unless otherwise specified
        this.indicesPrivileges = Collections
                .unmodifiableSet(indicesPrivileges != null ? new HashSet<>(indicesPrivileges) : Collections.emptySet());
        // no application resource privileges are granted unless otherwise specified
        this.applicationResourcePrivileges = Collections.unmodifiableSet(
                applicationResourcePrivileges != null ? new HashSet<>(applicationResourcePrivileges) : Collections.emptySet());
        // no run as privileges are granted unless otherwise specified
        this.runAsPrivilege = Collections.unmodifiableSet(runAsPrivilege != null ? new HashSet<>(runAsPrivilege) : Collections.emptySet());
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.transientMetadata = transientMetadata != null ? Collections.unmodifiableMap(transientMetadata) : Collections.emptyMap();
    }

    public String getName() {
        return name;
    }

    public Set<String> getClusterPrivileges() {
        return clusterPrivileges;
    }

    public GlobalPrivileges getGlobalApplicationPrivileges() {
        return globalApplicationPrivileges;
    }

    public Set<IndicesPrivileges> getIndicesPrivileges() {
        return indicesPrivileges;
    }

    public Set<ApplicationResourcePrivileges> getApplicationResourcePrivileges() {
        return applicationResourcePrivileges;
    }

    public Set<String> getRunAsPrivilege() {
        return runAsPrivilege;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Role that = (Role) o;
        return name.equals(that.name)
            && clusterPrivileges.equals(that.clusterPrivileges)
            && Objects.equals(globalApplicationPrivileges, that.globalApplicationPrivileges)
            && indicesPrivileges.equals(that.indicesPrivileges)
            && applicationResourcePrivileges.equals(that.applicationResourcePrivileges)
            && runAsPrivilege.equals(that.runAsPrivilege)
            && metadata.equals(that.metadata)
            && transientMetadata.equals(that.transientMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, clusterPrivileges, globalApplicationPrivileges, indicesPrivileges, applicationResourcePrivileges,
            runAsPrivilege, metadata, transientMetadata);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append("Name=").append(name).append(",");
        if (false == clusterPrivileges.isEmpty()) {
            sb.append("ClusterPrivileges=");
            sb.append(clusterPrivileges.toString());
            sb.append(", ");
        }
        if (globalApplicationPrivileges != null) {
            sb.append("GlobalApplcationPrivileges=");
            sb.append(globalApplicationPrivileges.toString());
            sb.append(", ");
        }
        if (false == indicesPrivileges.isEmpty()) {
            sb.append("IndicesPrivileges=");
            sb.append(indicesPrivileges.toString());
            sb.append(", ");
        }
        if (false == applicationResourcePrivileges.isEmpty()) {
            sb.append("ApplicationPrivileges=");
            sb.append(applicationResourcePrivileges.toString());
            sb.append(", ");
        }
        if (false == runAsPrivilege.isEmpty()) {
            sb.append("RunAsPrivilege=");
            sb.append(runAsPrivilege.toString());
            sb.append(", ");
        }
        if (false == metadata.isEmpty()) {
            sb.append("Metadata=[");
            sb.append(metadata.toString());
            sb.append("], ");
        }
        if (false == transientMetadata.isEmpty()) {
            sb.append("TransientMetadata=[");
            sb.append(transientMetadata.toString());
            sb.append("] ");
        }
        sb.append("}");
        return sb.toString();
    }

    public static Role fromXContent(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private @Nullable String name = null;
        private @Nullable Collection<String> clusterPrivileges = null;
        private @Nullable GlobalPrivileges globalApplicationPrivileges = null;
        private @Nullable Collection<IndicesPrivileges> indicesPrivileges = null;
        private @Nullable Collection<ApplicationResourcePrivileges> applicationResourcePrivileges = null;
        private @Nullable Collection<String> runAsPrivilege = null;
        private @Nullable Map<String, Object> metadata = null;
        private @Nullable Map<String, Object> transientMetadata = null;

        private Builder() {
        }

        public Builder name(String name) {
            if (Strings.hasText(name) == false){
                throw new IllegalArgumentException("role name must be provided");
            } else {
                this.name = name;
            }
            return this;
        }

        public Builder clusterPrivileges(String... clusterPrivileges) {
            return clusterPrivileges(Arrays
                    .asList(Objects.requireNonNull(clusterPrivileges, "Cluster privileges cannot be null. Pass an empty array instead.")));
        }

        public Builder clusterPrivileges(Collection<String> clusterPrivileges) {
            this.clusterPrivileges = Objects.requireNonNull(clusterPrivileges,
                    "Cluster privileges cannot be null. Pass an empty collection instead.");
            return this;
        }

        public Builder globalApplicationPrivileges(GlobalPrivileges globalApplicationPrivileges) {
            this.globalApplicationPrivileges = globalApplicationPrivileges;
            return this;
        }

        public Builder indicesPrivileges(IndicesPrivileges... indicesPrivileges) {
            return indicesPrivileges(Arrays
                    .asList(Objects.requireNonNull(indicesPrivileges, "Indices privileges cannot be null. Pass an empty array instead.")));
        }

        public Builder indicesPrivileges(Collection<IndicesPrivileges> indicesPrivileges) {
            this.indicesPrivileges = Objects.requireNonNull(indicesPrivileges,
                    "Indices privileges cannot be null. Pass an empty collection instead.");
            return this;
        }

        public Builder applicationResourcePrivileges(ApplicationResourcePrivileges... applicationResourcePrivileges) {
            return applicationResourcePrivileges(Arrays.asList(Objects.requireNonNull(applicationResourcePrivileges,
                    "Application resource privileges cannot be null. Pass an empty array instead.")));
        }

        public Builder applicationResourcePrivileges(Collection<ApplicationResourcePrivileges> applicationResourcePrivileges) {
            this.applicationResourcePrivileges = Objects.requireNonNull(applicationResourcePrivileges,
                    "Application resource privileges cannot be null. Pass an empty collection instead.");
            return this;
        }

        public Builder runAsPrivilege(String... runAsPrivilege) {
            return runAsPrivilege(Arrays
                    .asList(Objects.requireNonNull(runAsPrivilege, "Run as privilege cannot be null. Pass an empty array instead.")));
        }

        public Builder runAsPrivilege(Collection<String> runAsPrivilege) {
            this.runAsPrivilege = Objects.requireNonNull(runAsPrivilege,
                    "Run as privilege cannot be null. Pass an empty collection instead.");
            return this;
        }

        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = Objects.requireNonNull(metadata, "Metadata cannot be null. Pass an empty map instead.");
            return this;
        }

        public Builder transientMetadata(Map<String, Object> transientMetadata) {
            this.transientMetadata =
                Objects.requireNonNull(transientMetadata, "Transient metadata cannot be null. Pass an empty map instead.");
            return this;
        }

        public Role build() {
            return new Role(name, clusterPrivileges, globalApplicationPrivileges, indicesPrivileges, applicationResourcePrivileges,
                runAsPrivilege, metadata, transientMetadata);
        }
    }

    /**
     * Canonical cluster privilege names. There is no enforcement to only use these.
     */
    public static class ClusterPrivilegeName {
        public static final String NONE = "none";
        public static final String ALL = "all";
        public static final String MONITOR = "monitor";
        public static final String MONITOR_ML = "monitor_ml";
        public static final String MONITOR_WATCHER = "monitor_watcher";
        public static final String MONITOR_ROLLUP = "monitor_rollup";
        public static final String MANAGE = "manage";
        public static final String MANAGE_ML = "manage_ml";
        public static final String MANAGE_WATCHER = "manage_watcher";
        public static final String MANAGE_ROLLUP = "manage_rollup";
        public static final String MANAGE_INDEX_TEMPLATES = "manage_index_templates";
        public static final String MANAGE_INGEST_PIPELINES = "manage_ingest_pipelines";
        public static final String TRANSPORT_CLIENT = "transport_client";
        public static final String MANAGE_SECURITY = "manage_security";
        public static final String MANAGE_SAML = "manage_saml";
        public static final String MANAGE_TOKEN = "manage_token";
        public static final String MANAGE_PIPELINE = "manage_pipeline";
        public static final String MANAGE_CCR = "manage_ccr";
        public static final String READ_CCR = "read_ccr";
    }

    /**
     * Canonical index privilege names. There is no enforcement to only use these.
     */
    public static class IndexPrivilegeName {
        public static final String NONE = "none";
        public static final String ALL = "all";
        public static final String READ = "read";
        public static final String READ_CROSS = "read_cross_cluster";
        public static final String CREATE = "create";
        public static final String INDEX = "index";
        public static final String DELETE = "delete";
        public static final String WRITE = "write";
        public static final String MONITOR = "monitor";
        public static final String MANAGE = "manage";
        public static final String DELETE_INDEX = "delete_index";
        public static final String CREATE_INDEX = "create_index";
        public static final String VIEW_INDEX_METADATA = "view_index_metadata";
        public static final String MANAGE_FOLLOW_INDEX = "manage_follow_index";
    }

}
