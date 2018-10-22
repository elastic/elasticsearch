/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivileges;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Response for a {@link GetUserPrivilegesRequest}
 */
public final class GetUserPrivilegesResponse extends ActionResponse {

    private Set<String> cluster;
    private Set<ConditionalClusterPrivilege> conditionalCluster;
    private Set<Indices> index;
    private Set<RoleDescriptor.ApplicationResourcePrivileges> application;
    private Set<String> runAs;

    public GetUserPrivilegesResponse() {
        this(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    public GetUserPrivilegesResponse(Set<String> cluster, Set<ConditionalClusterPrivilege> conditionalCluster,
                                     Set<Indices> index,
                                     Set<RoleDescriptor.ApplicationResourcePrivileges> application,
                                     Set<String> runAs) {
        this.cluster = Collections.unmodifiableSet(cluster);
        this.conditionalCluster = Collections.unmodifiableSet(conditionalCluster);
        this.index = Collections.unmodifiableSet(index);
        this.application = Collections.unmodifiableSet(application);
        this.runAs = Collections.unmodifiableSet(runAs);
    }

    public Set<String> getClusterPrivileges() {
        return cluster;
    }

    public Set<ConditionalClusterPrivilege> getConditionalClusterPrivileges() {
        return conditionalCluster;
    }

    public Set<Indices> getIndexPrivileges() {
        return index;
    }

    public Set<RoleDescriptor.ApplicationResourcePrivileges> getApplicationPrivileges() {
        return application;
    }

    public Set<String> getRunAs() {
        return runAs;
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cluster = Collections.unmodifiableSet(in.readSet(StreamInput::readString));
        conditionalCluster = Collections.unmodifiableSet(in.readSet(ConditionalClusterPrivileges.READER));
        index = Collections.unmodifiableSet(in.readSet(Indices::new));
        application = Collections.unmodifiableSet(in.readSet(RoleDescriptor.ApplicationResourcePrivileges::new));
        runAs = Collections.unmodifiableSet(in.readSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(cluster, StreamOutput::writeString);
        out.writeCollection(conditionalCluster, ConditionalClusterPrivileges.WRITER);
        out.writeCollection(index, (o, p) -> p.writeTo(o));
        out.writeCollection(application, (o, p) -> p.writeTo(o));
        out.writeCollection(runAs, StreamOutput::writeString);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final GetUserPrivilegesResponse that = (GetUserPrivilegesResponse) other;
        return Objects.equals(cluster, that.cluster) &&
            Objects.equals(conditionalCluster, that.conditionalCluster) &&
            Objects.equals(index, that.index) &&
            Objects.equals(application, that.application) &&
            Objects.equals(runAs, that.runAs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, conditionalCluster, index, application, runAs);
    }

    /**
     * This is modelled on {@link RoleDescriptor.IndicesPrivileges}, with support for multiple DLS and FLS field sets.
     */
    public static class Indices implements ToXContentObject, Writeable {

        private final Set<String> indices;
        private final Set<String> privileges;
        private final Set<FieldPermissionsDefinition.FieldGrantExcludeGroup> fieldSecurity;
        private final Set<BytesReference> queries;

        public Indices(Collection<String> indices, Collection<String> privileges,
                       Set<FieldPermissionsDefinition.FieldGrantExcludeGroup> fieldSecurity, Set<BytesReference> queries) {
            // The use of TreeSet is to provide a consistent order that can be relied upon in tests
            this.indices = Collections.unmodifiableSet(new TreeSet<>(Objects.requireNonNull(indices)));
            this.privileges = Collections.unmodifiableSet(new TreeSet<>(Objects.requireNonNull(privileges)));
            this.fieldSecurity = Collections.unmodifiableSet(Objects.requireNonNull(fieldSecurity));
            this.queries = Collections.unmodifiableSet(Objects.requireNonNull(queries));
        }

        public Indices(StreamInput in) throws IOException {
            // The use of TreeSet is to provide a consistent order that can be relied upon in tests
            indices = Collections.unmodifiableSet(new TreeSet<>(in.readSet(StreamInput::readString)));
            privileges = Collections.unmodifiableSet(new TreeSet<>(in.readSet(StreamInput::readString)));
            fieldSecurity = Collections.unmodifiableSet(in.readSet(input -> {
                final String[] grant = input.readOptionalStringArray();
                final String[] exclude = input.readOptionalStringArray();
                return new FieldPermissionsDefinition.FieldGrantExcludeGroup(grant, exclude);
            }));
            queries = Collections.unmodifiableSet(in.readSet(StreamInput::readBytesReference));
        }

        public Set<String> getIndices() {
            return indices;
        }

        public Set<String> getPrivileges() {
            return privileges;
        }

        public Set<FieldPermissionsDefinition.FieldGrantExcludeGroup> getFieldSecurity() {
            return fieldSecurity;
        }

        public Set<BytesReference> getQueries() {
            return queries;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(getClass().getSimpleName())
                .append("[")
                .append("indices=[").append(Strings.collectionToCommaDelimitedString(indices))
                .append("], privileges=[").append(Strings.collectionToCommaDelimitedString(privileges))
                .append("]");
            if (fieldSecurity.isEmpty() == false) {
                sb.append(", fls=[").append(Strings.collectionToCommaDelimitedString(fieldSecurity)).append("]");
            }
            if (queries.isEmpty() == false) {
                sb.append(", dls=[")
                    .append(queries.stream().map(BytesReference::utf8ToString).collect(Collectors.joining(",")))
                    .append("]");
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Indices that = (Indices) o;

            return this.indices.equals(that.indices)
                && this.privileges.equals(that.privileges)
                && this.fieldSecurity.equals(that.fieldSecurity)
                && this.queries.equals(that.queries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indices, privileges, fieldSecurity, queries);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RoleDescriptor.Fields.NAMES.getPreferredName(), indices);
            builder.field(RoleDescriptor.Fields.PRIVILEGES.getPreferredName(), privileges);
            if (fieldSecurity.stream().anyMatch(g -> nonEmpty(g.getGrantedFields()) || nonEmpty(g.getExcludedFields()))) {
                builder.startArray(RoleDescriptor.Fields.FIELD_PERMISSIONS.getPreferredName());
                for (FieldPermissionsDefinition.FieldGrantExcludeGroup group : this.fieldSecurity) {
                    builder.startObject();
                    if (nonEmpty(group.getGrantedFields())) {
                        builder.array(RoleDescriptor.Fields.GRANT_FIELDS.getPreferredName(), group.getGrantedFields());
                    }
                    if (nonEmpty(group.getExcludedFields())) {
                        builder.array(RoleDescriptor.Fields.EXCEPT_FIELDS.getPreferredName(), group.getExcludedFields());
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            if (queries.isEmpty() == false) {
                builder.startArray(RoleDescriptor.Fields.QUERY.getPreferredName());
                for (BytesReference q : queries) {
                    builder.value(q.utf8ToString());
                }
                builder.endArray();
            }
            return builder.endObject();
        }

        private boolean nonEmpty(String[] grantedFields) {
            return grantedFields != null && grantedFields.length != 0;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(indices, StreamOutput::writeString);
            out.writeCollection(privileges, StreamOutput::writeString);
            out.writeCollection(fieldSecurity, (output, fields) -> {
                output.writeOptionalStringArray(fields.getGrantedFields());
                output.writeOptionalStringArray(fields.getExcludedFields());
            });
            out.writeCollection(queries, StreamOutput::writeBytesReference);
        }
    }
}
