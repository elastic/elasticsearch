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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class Role implements ToXContentObject {

    public static final ParseField CLUSTER = new ParseField("cluster");
    public static final ParseField GLOBAL = new ParseField("global");
    public static final ParseField INDICES = new ParseField("indices");
    public static final ParseField APPLICATIONS = new ParseField("applications");
    public static final ParseField RUN_AS = new ParseField("run_as");
    public static final ParseField METADATA = new ParseField("metadata");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Role, Void> PARSER = new ConstructingObjectParser<>("role_descriptor", false,
            constructorObjects -> {
                int i = 0;
                final Collection<ClusterPrivilege> clusterPrivileges = (Collection<ClusterPrivilege>) constructorObjects[i++];
                final ManageApplicationsPrivilege manageApplicationPrivileges = (ManageApplicationsPrivilege) constructorObjects[i++];
                final Collection<IndicesPrivileges> indicesPrivileges = (Collection<IndicesPrivileges>) constructorObjects[i++];
                final Collection<ApplicationResourcePrivileges> applicationResourcePrivileges =
                        (Collection<ApplicationResourcePrivileges>) constructorObjects[i++];
                final Collection<String> runAs = (Collection<String>) constructorObjects[i++];
                final Map<String, Object> metadata = (Map<String, Object>) constructorObjects[i];
                return new Role(clusterPrivileges, manageApplicationPrivileges, indicesPrivileges, applicationResourcePrivileges,
                        runAs, metadata);
            });

    static {
        PARSER.declareFieldArray(optionalConstructorArg(), (p, c) -> ClusterPrivilege.fromString(p.text()), CLUSTER,
                ValueType.STRING_ARRAY);
        PARSER.declareObject(optionalConstructorArg(), ManageApplicationsPrivilege.PARSER, GLOBAL);
        PARSER.declareFieldArray(optionalConstructorArg(), IndicesPrivileges.PARSER, INDICES, ValueType.OBJECT_ARRAY);
        PARSER.declareFieldArray(optionalConstructorArg(), ApplicationResourcePrivileges.PARSER, APPLICATIONS, ValueType.OBJECT_ARRAY);
        PARSER.declareStringArray(optionalConstructorArg(), RUN_AS);
        PARSER.<Map<String, Object>>declareObject(constructorArg(), (parser, c) -> parser.map(), METADATA);
    }

    private final Set<ClusterPrivilege> clusterPrivileges;
    private final @Nullable ManageApplicationsPrivilege manageApplicationPrivileges;
    private final Set<IndicesPrivileges> indicesPrivileges;
    private final Set<ApplicationResourcePrivileges> applicationResourcePrivileges;
    private final Set<String> runAs;
    private final Map<String, Object> metadata;

    private Role(Collection<ClusterPrivilege> clusterPrivileges,
            @Nullable ManageApplicationsPrivilege manageApplicationPrivileges, Collection<IndicesPrivileges> indicesPrivileges,
            Collection<ApplicationResourcePrivileges> applicationResourcePrivileges, Collection<String> runAs,
            Map<String, Object> metadata) {
        // we do all null checks inside the constructor
        // no cluster privileges are granted unless otherwise specified
        this.clusterPrivileges = Collections
                .unmodifiableSet(clusterPrivileges != null ? new HashSet<>(clusterPrivileges) : Collections.emptySet());
        this.manageApplicationPrivileges = manageApplicationPrivileges;
        // no indices privileges are granted unless otherwise specified
        this.indicesPrivileges = Collections
                .unmodifiableSet(indicesPrivileges != null ? new HashSet<>(indicesPrivileges) : Collections.emptySet());
        // no application resource privileges are granted unless otherwise specified
        this.applicationResourcePrivileges = Collections.unmodifiableSet(
                applicationResourcePrivileges != null ? new HashSet<>(applicationResourcePrivileges) : Collections.emptySet());
        // no run as privileges are granted unless otherwise specified
        this.runAs = Collections.unmodifiableSet(runAs != null ? new HashSet<>(runAs) : Collections.emptySet());
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
    }

    public Set<ClusterPrivilege> getClusterPrivileges() {
        return clusterPrivileges;
    }

    public ManageApplicationsPrivilege getManageApplicationPrivileges() {
        return manageApplicationPrivileges;
    }

    public Set<IndicesPrivileges> getIndicesPrivileges() {
        return indicesPrivileges;
    }

    public Set<ApplicationResourcePrivileges> getApplicationResourcePrivileges() {
        return applicationResourcePrivileges;
    }

    public Set<String> getRunAs() {
        return runAs;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Role that = (Role) o;
        return clusterPrivileges.equals(that.clusterPrivileges)
                && Objects.equals(manageApplicationPrivileges, that.manageApplicationPrivileges)
                && indicesPrivileges.equals(that.indicesPrivileges)
                && applicationResourcePrivileges.equals(that.applicationResourcePrivileges) && runAs.equals(that.runAs)
                && metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterPrivileges, manageApplicationPrivileges, indicesPrivileges, applicationResourcePrivileges, runAs,
                metadata);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("[");
        sb.append("cluster=[").append(Strings.collectionToCommaDelimitedString(clusterPrivileges));
        sb.append("], global[").append(manageApplicationPrivileges);
        sb.append("], indicesPrivileges=[").append(Strings.collectionToCommaDelimitedString(indicesPrivileges));
        sb.append("], applicationResourcePrivileges=[").append(Strings.collectionToCommaDelimitedString(applicationResourcePrivileges));
        sb.append("], runAs=[").append(Strings.collectionToCommaDelimitedString(runAs));
        sb.append("], metadata=[").append(metadata);
        sb.append("]]");
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (false == clusterPrivileges.isEmpty()) {
            builder.field(CLUSTER.getPreferredName(), clusterPrivileges);
        }
        if (null != manageApplicationPrivileges) {
            builder.field(GLOBAL.getPreferredName(), manageApplicationPrivileges);
        }
        if (false == indicesPrivileges.isEmpty()) {
            builder.field(INDICES.getPreferredName(), indicesPrivileges);
        }
        if (false == applicationResourcePrivileges.isEmpty()) {
            builder.field(APPLICATIONS.getPreferredName(), applicationResourcePrivileges);
        }
        if (false == runAs.isEmpty()) {
            builder.field(RUN_AS.getPreferredName(), runAs);
        }
        if (false == metadata.isEmpty()) {
            builder.field(METADATA.getPreferredName(), metadata);
        }
        return builder.endObject();
    }

    public static Role fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private @Nullable Collection<ClusterPrivilege> clusterPrivileges = null;
        private @Nullable ManageApplicationsPrivilege manageApplicationPrivileges = null;
        private @Nullable Collection<IndicesPrivileges> indicesPrivileges = null;
        private @Nullable Collection<ApplicationResourcePrivileges> applicationResourcePrivileges = null;
        private @Nullable Collection<String> runAs = null;
        private @Nullable Map<String, Object> metadata = null;

        private Builder() {
        }

        public Builder clusterPrivileges(@Nullable ClusterPrivilege... clusterPrivileges) {
            if (clusterPrivileges == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
            return clusterPrivileges(Arrays.asList(clusterPrivileges));
        }

        public Builder clusterPrivileges(@Nullable Collection<ClusterPrivilege> clusterPrivileges) {
            this.clusterPrivileges = clusterPrivileges;
            return this;
        }

        public Builder manageApplicationPrivileges(@Nullable ManageApplicationsPrivilege manageApplicationPrivileges) {
            this.manageApplicationPrivileges = manageApplicationPrivileges;
            return this;
        }

        public Builder indicesPrivileges(@Nullable IndicesPrivileges... indicesPrivileges) {
            if (indicesPrivileges == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
            return indicesPrivileges(Arrays.asList(indicesPrivileges));
        }

        public Builder indicesPrivileges(@Nullable Collection<IndicesPrivileges> indicesPrivileges) {
            this.indicesPrivileges = indicesPrivileges;
            return this;
        }

        public Builder applicationResourcePrivileges(@Nullable ApplicationResourcePrivileges... applicationResourcePrivileges) {
            if (applicationResourcePrivileges == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
            return applicationResourcePrivileges(Arrays.asList(applicationResourcePrivileges));
        }

        public Builder applicationResourcePrivileges(@Nullable Collection<ApplicationResourcePrivileges> applicationResourcePrivileges) {
            this.applicationResourcePrivileges = applicationResourcePrivileges;
            return this;
        }

        public Builder runAs(@Nullable String... runAs) {
            if (runAs == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
            return runAs(Arrays.asList(runAs));
        }

        public Builder runAs(@Nullable Collection<String> runAs) {
            this.runAs = runAs;
            return this;
        }

        public Builder metadata(@Nullable Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Role build() {
            return new Role(clusterPrivileges, manageApplicationPrivileges, indicesPrivileges, applicationResourcePrivileges, runAs,
                    metadata);
        }
    }

}
