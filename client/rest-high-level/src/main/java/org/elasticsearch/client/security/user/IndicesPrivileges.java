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

package org.elasticsearch.client.security.user;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class IndicesPrivileges implements ToXContentObject {
    
    public static final ParseField NAMES = new ParseField("names");
    public static final ParseField PRIVILEGES = new ParseField("privileges");
    public static final ParseField FIELD_PERMISSIONS = new ParseField("field_security");
    public static final ParseField GRANT_FIELDS = new ParseField("grant");
    public static final ParseField EXCEPT_FIELDS = new ParseField("except");
    public static final ParseField QUERY = new ParseField("query");

    private final Set<String> indices;
    private final Set<IndexPrivilege> privileges;
    // '*' means all fields (default value), empty means no fields
    private final Set<String> grantedFields;
    // empty means no field is denied
    private final Set<String> deniedFields;
    // missing query means all documents, i.e. no restrictions
    private final Optional<String> query;

    private IndicesPrivileges(Set<String> indices, Set<IndexPrivilege> privileges, Set<String> grantedFields, Set<String> deniedFields,
            Optional<String> query) {
        assert false == indices.isEmpty();
        assert false == privileges.isEmpty();
        this.indices = Collections.unmodifiableSet(indices);
        this.privileges = Collections.unmodifiableSet(privileges);
        this.grantedFields = Collections.unmodifiableSet(grantedFields);
        this.deniedFields = Collections.unmodifiableSet(deniedFields);
        this.query = query;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Set<String> getIndices() {
        return this.indices;
    }

    public Set<IndexPrivilege> getPrivileges() {
        return this.privileges;
    }

    public Set<String> getGrantedFields() {
        return this.grantedFields;
    }

    public Set<String> getDeniedFields() {
        return this.deniedFields;
    }

    public Optional<String> getQuery() {
        return this.query;
    }

    public boolean isUsingDocumentLevelSecurity() {
        return query.isPresent();
    }

    public boolean isUsingFieldLevelSecurity() {
        return hasDeniedFields() || hasGrantedFields();
    }

    private boolean hasDeniedFields() {
        return !deniedFields.isEmpty();
    }

    private boolean hasGrantedFields() {
        // we treat just '*' as no FLS since that's what the UI defaults to
        if (grantedFields.size() == 1 && grantedFields.iterator().next().equals("*")) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndicesPrivileges[");
        sb.append(NAMES.getPreferredName()).append("=[").append(Strings.collectionToCommaDelimitedString(indices)).append("], ");
        sb.append(PRIVILEGES.getPreferredName()).append("=[").append(Strings.collectionToCommaDelimitedString(privileges)).append("], ");
        sb.append(FIELD_PERMISSIONS).append("=[");
        sb.append(GRANT_FIELDS).append("=[").append(Strings.collectionToCommaDelimitedString(grantedFields)).append("], ");
        sb.append(EXCEPT_FIELDS).append("=[").append(Strings.collectionToCommaDelimitedString(deniedFields)).append("]");
        sb.append("]");
        if (query.isPresent()) {
            sb.append(", ").append(QUERY.getPreferredName()).append("=[").append(query).append("]");
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndicesPrivileges that = (IndicesPrivileges) o;

        return indices.equals(that.indices) && privileges.equals(that.privileges) && grantedFields.equals(that.grantedFields)
                && deniedFields.equals(that.deniedFields) && query.equals(that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices, privileges, grantedFields, deniedFields, query);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAMES.getPreferredName(), indices);
        builder.field(PRIVILEGES.getPreferredName(), privileges);
        builder.startObject(FIELD_PERMISSIONS.getPreferredName());
        builder.field(GRANT_FIELDS.getPreferredName(), grantedFields);
        builder.field(EXCEPT_FIELDS.getPreferredName(), deniedFields);
        builder.endObject();
        if (query.isPresent()) {
            builder.field("query", query.get());
        }
        return builder.endObject();
    }

    public static class Builder {

        private Optional<Set<String>> indices = Optional.empty();
        private Optional<Set<IndexPrivilege>> privileges = Optional.empty();
        private Optional<Set<String>> grantedFields = Optional.empty();
        private Optional<Set<String>> deniedFields = Optional.empty();
        private Optional<String> query = Optional.empty();

        private Builder() {
        }

        public Builder indices(String... indices) {
            return indices(Arrays.asList(indices));
        }
        
        public Builder indices(Collection<String> indices) {
            this.indices = Optional.of(new HashSet<>(indices));
            return this;
        }

        public Builder privileges(IndexPrivilege... privileges) {
            return privileges(Arrays.asList(privileges));
        }
        
        public Builder privileges(Collection<IndexPrivilege> privileges) {
            this.privileges = Optional.of(new HashSet<>(privileges));
            return this;
        }
        
        public Builder grantedFields(Collection<String> grantedFields) {
            this.grantedFields = Optional.of(new HashSet<>(grantedFields));
            return this;
        }

        public Builder grantedFields(String... grantedFields) {
            return grantedFields(Arrays.asList(grantedFields));
        }
        
        public Builder deniedFields(Collection<String> deniedFields) {
            this.deniedFields = Optional.of(new HashSet<>(deniedFields));
            return this;
        }

        public Builder deniedFields(String... deniedFields) {
            return deniedFields(Arrays.asList(deniedFields));
        }

        public Builder query(String query) {
            this.query = Optional.of(query);
            return this;
        }

        public IndicesPrivileges build() {
            if (false == indices.isPresent() || indices.get().isEmpty()) {
                throw new IllegalArgumentException("indices privileges must refer to at least one index name or index name pattern");
            }
            if (false == privileges.isPresent() || privileges.get().isEmpty()) {
                throw new IllegalArgumentException("indices privileges must define at least one privilege");
            }
            return new IndicesPrivileges(indices.get(), privileges.get(),
                    grantedFields.orElse(Collections.singleton("*")), // all fields granted unless otherwise set
                    deniedFields.orElse(Collections.emptySet()), // no fields denied unless otherwise set
                    query);
        }
    }

}
