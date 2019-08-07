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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents privileges over indices. There is a canonical set of privilege
 * names (eg. {@code IndicesPrivileges#READ_PRIVILEGE_NAME}) but there is
 * flexibility in the definition of finer grained, more specialized, privileges.
 * This also encapsulates field and document level security privileges. These
 * allow to control what fields or documents are readable or queryable.
 */
public final class IndicesPrivileges extends AbstractIndicesPrivileges implements ToXContentObject {

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<IndicesPrivileges, Void> PARSER =
        new ConstructingObjectParser<>("indices_privileges", false, constructorObjects -> {
            int i = 0;
            final List<String> indices = (List<String>) constructorObjects[i++];
            final List<String> privileges = (List<String>) constructorObjects[i++];
            final boolean allowRestrictedIndices = (Boolean) constructorObjects[i++];
            final FieldSecurity fields = (FieldSecurity) constructorObjects[i++];
            final String query = (String) constructorObjects[i];
            return new IndicesPrivileges(indices, privileges, allowRestrictedIndices, fields, query);
        });

    static {
        PARSER.declareStringArray(constructorArg(), NAMES);
        PARSER.declareStringArray(constructorArg(), PRIVILEGES);
        PARSER.declareBoolean(constructorArg(), ALLOW_RESTRICTED_INDICES);
        PARSER.declareObject(optionalConstructorArg(), FieldSecurity::parse, FIELD_PERMISSIONS);
        PARSER.declareStringOrNull(optionalConstructorArg(), QUERY);
    }

    private final FieldSecurity fieldSecurity;
    // missing query means all documents, i.e. no restrictions
    private final @Nullable String query;

    private IndicesPrivileges(List<String> indices, List<String> privileges, boolean allowRestrictedIndices,
                              @Nullable FieldSecurity fieldSecurity, @Nullable String query) {
        super(indices, privileges, allowRestrictedIndices);
        this.fieldSecurity = fieldSecurity;
        this.query = query;
    }

    /**
     * The combination of the {@link FieldSecurity#getGrantedFields() granted} and
     * {@link FieldSecurity#getDeniedFields() denied} document fields.
     * May be null, in which case no field level security is applicable, and all the document's fields are granted access to.
     */
    public FieldSecurity getFieldSecurity() {
        return fieldSecurity;
    }

    /**
     * A query limiting the visible documents in the indices. Can be null, in which
     * case all documents are visible.
     */
    public @Nullable String getQuery() {
        return this.query;
    }

    /**
     * If {@code true} some documents might not be visible. Only the documents
     * matching {@code query} will be readable.
     */
    @Override
    public boolean isUsingDocumentLevelSecurity() {
        return query != null;
    }

    /**
     * If {@code true} some document fields might not be visible.
     */
    @Override
    public boolean isUsingFieldLevelSecurity() {
        return fieldSecurity != null && fieldSecurity.isUsingFieldLevelSecurity();
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
        return indices.equals(that.indices)
            && privileges.equals(that.privileges)
            && allowRestrictedIndices == that.allowRestrictedIndices
            && Objects.equals(this.fieldSecurity, that.fieldSecurity)
            && Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices, privileges, allowRestrictedIndices, fieldSecurity, query);
    }

    @Override
    public String toString() {
        try {
            return XContentHelper.toXContent(this, XContentType.JSON, true).utf8ToString();
        } catch (IOException e) {
            throw new RuntimeException("Unexpected", e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAMES.getPreferredName(), indices);
        builder.field(PRIVILEGES.getPreferredName(), privileges);
        builder.field(ALLOW_RESTRICTED_INDICES.getPreferredName(), allowRestrictedIndices);
        if (fieldSecurity != null) {
            builder.field(FIELD_PERMISSIONS.getPreferredName(), fieldSecurity, params);
        }
        if (isUsingDocumentLevelSecurity()) {
            builder.field("query", query);
        }
        return builder.endObject();
    }

    public static IndicesPrivileges fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private @Nullable
        List<String> indices = null;
        private @Nullable
        List<String> privileges = null;
        private @Nullable
        List<String> grantedFields = null;
        private @Nullable
        List<String> deniedFields = null;
        private @Nullable
        String query = null;
        boolean allowRestrictedIndices = false;

        public Builder() {
        }

        public Builder indices(String... indices) {
            return indices(Arrays.asList(Objects.requireNonNull(indices, "indices required")));
        }

        public Builder indices(List<String> indices) {
            this.indices = Objects.requireNonNull(indices, "indices required");
            return this;
        }

        public Builder privileges(String... privileges) {
            return privileges(Arrays.asList(Objects.requireNonNull(privileges, "privileges required")));
        }

        public Builder privileges(List<String> privileges) {
            this.privileges = Objects.requireNonNull(privileges, "privileges required");
            return this;
        }

        public Builder grantedFields(@Nullable String... grantedFields) {
            if (grantedFields == null) {
                this.grantedFields = null;
                return this;
            }
            return grantedFields(Arrays.asList(grantedFields));
        }

        public Builder grantedFields(@Nullable List<String> grantedFields) {
            this.grantedFields = grantedFields;
            return this;
        }

        public Builder deniedFields(@Nullable String... deniedFields) {
            if (deniedFields == null) {
                this.deniedFields = null;
                return this;
            }
            return deniedFields(Arrays.asList(deniedFields));
        }

        public Builder deniedFields(@Nullable List<String> deniedFields) {
            this.deniedFields = deniedFields;
            return this;
        }

        public Builder query(@Nullable String query) {
            this.query = query;
            return this;
        }

        public Builder allowRestrictedIndices(boolean allow) {
            this.allowRestrictedIndices = allow;
            return this;
        }

        public IndicesPrivileges build() {
            final FieldSecurity fieldSecurity;
            if (grantedFields == null && deniedFields == null) {
                fieldSecurity = null;
            } else {
                fieldSecurity = new FieldSecurity(grantedFields, deniedFields);
            }
            return new IndicesPrivileges(indices, privileges, allowRestrictedIndices, fieldSecurity, query);
        }
    }

}
