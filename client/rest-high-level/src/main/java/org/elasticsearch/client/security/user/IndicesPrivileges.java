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

import org.elasticsearch.common.Nullable;
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
import java.util.Set;

public final class IndicesPrivileges implements ToXContentObject {

    public static final ParseField NAMES = new ParseField("names");
    public static final ParseField PRIVILEGES = new ParseField("privileges");
    public static final ParseField FIELD_PERMISSIONS = new ParseField("field_security");
    public static final ParseField GRANT_FIELDS = new ParseField("grant");
    public static final ParseField EXCEPT_FIELDS = new ParseField("except");
    public static final ParseField QUERY = new ParseField("query");

    private final Collection<String> indices;
    private final Collection<IndexPrivilege> privileges;
    // '*' means all fields (default value), empty means no fields
    private final Collection<String> grantedFields;
    // empty means no field is denied
    private final Collection<String> deniedFields;
    // missing query means all documents, i.e. no restrictions
    private final @Nullable String query;

    private IndicesPrivileges(Collection<String> indices, Collection<IndexPrivilege> privileges, Collection<String> grantedFields,
            Collection<String> deniedFields, @Nullable String query) {
        assert false == indices.isEmpty();
        assert false == privileges.isEmpty();
        this.indices = Collections.unmodifiableCollection(indices);
        this.privileges = Collections.unmodifiableCollection(privileges);
        this.grantedFields = Collections.unmodifiableCollection(grantedFields);
        this.deniedFields = Collections.unmodifiableCollection(deniedFields);
        this.query = query;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Collection<String> getIndices() {
        return this.indices;
    }

    public Collection<IndexPrivilege> getPrivileges() {
        return this.privileges;
    }

    public Collection<String> getGrantedFields() {
        return this.grantedFields;
    }

    public Collection<String> getDeniedFields() {
        return this.deniedFields;
    }

    public @Nullable String getQuery() {
        return this.query;
    }

    public boolean isUsingDocumentLevelSecurity() {
        return query != null;
    }

    public boolean isUsingFieldLevelSecurity() {
        return hasDeniedFields() || hasGrantedFields();
    }

    private boolean hasDeniedFields() {
        return false == deniedFields.isEmpty();
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
        if (query != null) {
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
        if (query != null) {
            builder.field("query", query);
        }
        return builder.endObject();
    }

    public static class Builder {

        private @Nullable Set<String> indices = null;
        private @Nullable Set<IndexPrivilege> privileges = null;
        private @Nullable Set<String> grantedFields = null;
        private @Nullable Set<String> deniedFields = null;
        private @Nullable String query = null;

        private Builder() {
        }

        public Builder indices(String... indices) {
            Objects.requireNonNull(indices, "indices name or pattern cannot be null");
            return indices(Arrays.asList(indices));
        }
        
        public Builder indices(Collection<String> indices) {
            Objects.requireNonNull(indices, "indices name or pattern cannot be null");
            this.indices = new HashSet<>(indices);
            return this;
        }

        public Builder privileges(IndexPrivilege... privileges) {
            Objects.requireNonNull(privileges, "privileges cannot be null");
            return privileges(Arrays.asList(privileges));
        }

        public Builder privileges(Collection<IndexPrivilege> privileges) {
            Objects.requireNonNull(privileges, "privileges cannot be null");
            this.privileges = new HashSet<>(privileges);
            return this;
        }

        public Builder grantedFields(String... grantedFields) {
            Objects.requireNonNull(grantedFields, "granted fields cannot be null");
            return grantedFields(Arrays.asList(grantedFields));
        }

        public Builder grantedFields(Collection<String> grantedFields) {
            Objects.requireNonNull(grantedFields, "granted fields cannot be null");
            this.grantedFields = new HashSet<>(grantedFields);
            return this;
        }

        public Builder deniedFields(String... deniedFields) {
            Objects.requireNonNull(deniedFields, "denied fields cannot be null");
            return deniedFields(Arrays.asList(deniedFields));
        }

        public Builder deniedFields(Collection<String> deniedFields) {
            Objects.requireNonNull(deniedFields, "denied fields cannot be null");
            this.deniedFields = new HashSet<>(deniedFields);
            return this;
        }

        public Builder query(@Nullable String query) {
            this.query = query;
            return this;
        }

        public IndicesPrivileges build() {
            if (null == indices || indices.isEmpty()) {
                throw new IllegalArgumentException("indices privileges must refer to at least one index name or index name pattern");
            }
            if (null == privileges || privileges.isEmpty()) {
                throw new IllegalArgumentException("indices privileges must define at least one privilege");
            }
            if (grantedFields == null) {
                // all fields granted unless otherwise specified
                grantedFields = Collections.singleton("*");
            }
            if (deniedFields == null) {
                deniedFields = Collections.emptySet();
            }
            return new IndicesPrivileges(indices, privileges, grantedFields, deniedFields, query);
        }
    }

}
