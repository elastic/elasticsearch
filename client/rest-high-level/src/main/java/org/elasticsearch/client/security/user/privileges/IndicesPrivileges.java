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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents privileges over indices. There is a canonical set of privilege
 * names (eg. {@code IndicesPrivileges#READ_PRIVILEGE_NAME}) but there is
 * flexibility in the definition of finer grained, more specialized, privileges.
 * This also encapsulates field and document level security privileges. These
 * allow to control what fields or documents are readable or queryable.
 */
public final class IndicesPrivileges implements ToXContentObject {

    public static final ParseField NAMES = new ParseField("names");
    public static final ParseField PRIVILEGES = new ParseField("privileges");
    public static final ParseField FIELD_PERMISSIONS = new ParseField("field_security");
    public static final ParseField GRANT_FIELDS = new ParseField("grant");
    public static final ParseField EXCEPT_FIELDS = new ParseField("except");
    public static final ParseField QUERY = new ParseField("query");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<IndicesPrivileges, Void> PARSER =
        new ConstructingObjectParser<>("indices_privileges", false, constructorObjects -> {
                int i = 0;
                final Collection<String> indices = (Collection<String>) constructorObjects[i++];
                final Collection<String> privileges = (Collection<String>) constructorObjects[i++];
                final Tuple<Collection<String>, Collection<String>> fields =
                        (Tuple<Collection<String>, Collection<String>>) constructorObjects[i++];
                final Collection<String> grantFields = fields != null ? fields.v1() : null;
                final Collection<String> exceptFields = fields != null ? fields.v2() : null;
                final String query = (String) constructorObjects[i];
                return new IndicesPrivileges(indices, privileges, grantFields, exceptFields, query);
            });

    static {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<Tuple<Collection<String>, Collection<String>>, Void> fls_parser =
                new ConstructingObjectParser<>( "field_level_parser", false, constructorObjects -> {
                        int i = 0;
                        final Collection<String> grantFields = (Collection<String>) constructorObjects[i++];
                        final Collection<String> exceptFields = (Collection<String>) constructorObjects[i];
                        return new Tuple<>(grantFields, exceptFields);
                    });
        fls_parser.declareStringArray(optionalConstructorArg(), GRANT_FIELDS);
        fls_parser.declareStringArray(optionalConstructorArg(), EXCEPT_FIELDS);

        PARSER.declareStringArray(constructorArg(), NAMES);
        PARSER.declareStringArray(constructorArg(), PRIVILEGES);
        PARSER.declareObject(optionalConstructorArg(), fls_parser, FIELD_PERMISSIONS);
        PARSER.declareStringOrNull(optionalConstructorArg(), QUERY);
    }

    private final Set<String> indices;
    private final Set<String> privileges;
    // null or singleton '*' means all fields are granted, empty means no fields are granted
    private final @Nullable Set<String> grantedFields;
    // null or empty means no fields are denied
    private final @Nullable Set<String> deniedFields;
    // missing query means all documents, i.e. no restrictions
    private final @Nullable String query;

    private IndicesPrivileges(Collection<String> indices, Collection<String> privileges, @Nullable Collection<String> grantedFields,
            @Nullable Collection<String> deniedFields, @Nullable String query) {
        if (null == indices || indices.isEmpty()) {
            throw new IllegalArgumentException("indices privileges must refer to at least one index name or index name pattern");
        }
        if (null == privileges || privileges.isEmpty()) {
            throw new IllegalArgumentException("indices privileges must define at least one privilege");
        }
        this.indices = Collections.unmodifiableSet(new HashSet<>(indices));
        this.privileges = Collections.unmodifiableSet(new HashSet<>(privileges));
        // unspecified granted fields means no restriction
        this.grantedFields = grantedFields == null ? null : Collections.unmodifiableSet(new HashSet<>(grantedFields));
        // unspecified denied fields means no restriction
        this.deniedFields = deniedFields == null ? null : Collections.unmodifiableSet(new HashSet<>(deniedFields));
        this.query = query;
    }

    /**
     * The indices names covered by the privileges.
     */
    public Set<String> getIndices() {
        return this.indices;
    }

    /**
     * The privileges acting over indices. There is a canonical predefined set of
     * such privileges, but the {@code String} datatype allows for flexibility in defining
     * finer grained privileges.
     */
    public Set<String> getPrivileges() {
        return this.privileges;
    }

    /**
     * The document fields that can be read or queried. Can be null, in this case
     * all the document's fields are granted access to. Can also be empty, in which
     * case no fields are granted access to.
     */
    public @Nullable Set<String> getGrantedFields() {
        return this.grantedFields;
    }

    /**
     * The document fields that cannot be accessed or queried. Can be null or empty,
     * in which case no fields are denied.
     */
    public @Nullable Set<String> getDeniedFields() {
        return this.deniedFields;
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
    public boolean isUsingDocumentLevelSecurity() {
        return query != null;
    }

    /**
     * If {@code true} some document fields might not be visible.
     */
    public boolean isUsingFieldLevelSecurity() {
        return limitsGrantedFields() || hasDeniedFields();
    }

    private boolean hasDeniedFields() {
        return deniedFields != null && false == deniedFields.isEmpty();
    }

    private boolean limitsGrantedFields() {
        // we treat just '*' as no FLS since that's what the UI defaults to
        if (grantedFields == null || (grantedFields.size() == 1 && grantedFields.iterator().next().equals("*"))) {
            return false;
        }
        return true;
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
                && Objects.equals(grantedFields, that.grantedFields)
                && Objects.equals(deniedFields, that.deniedFields)
                && Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices, privileges, grantedFields, deniedFields, query);
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
        if (isUsingFieldLevelSecurity()) {
            builder.startObject(FIELD_PERMISSIONS.getPreferredName());
            if (grantedFields != null) {
                builder.field(GRANT_FIELDS.getPreferredName(), grantedFields);
            }
            if (hasDeniedFields()) {
                builder.field(EXCEPT_FIELDS.getPreferredName(), deniedFields);
            }
            builder.endObject();
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

        private @Nullable Collection<String> indices = null;
        private @Nullable Collection<String> privileges = null;
        private @Nullable Collection<String> grantedFields = null;
        private @Nullable Collection<String> deniedFields = null;
        private @Nullable String query = null;

        public Builder() {
        }

        public Builder indices(String... indices) {
            return indices(Arrays.asList(Objects.requireNonNull(indices, "indices required")));
        }
        
        public Builder indices(Collection<String> indices) {
            this.indices = Objects.requireNonNull(indices, "indices required");
            return this;
        }

        public Builder privileges(String... privileges) {
            return privileges(Arrays.asList(Objects.requireNonNull(privileges, "privileges required")));
        }

        public Builder privileges(Collection<String> privileges) {
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

        public Builder grantedFields(@Nullable Collection<String> grantedFields) {
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

        public Builder deniedFields(@Nullable Collection<String> deniedFields) {
            this.deniedFields = deniedFields;
            return this;
        }

        public Builder query(@Nullable String query) {
            this.query = query;
            return this;
        }

        public IndicesPrivileges build() {
            return new IndicesPrivileges(indices, privileges, grantedFields, deniedFields, query);
        }
    }

}
