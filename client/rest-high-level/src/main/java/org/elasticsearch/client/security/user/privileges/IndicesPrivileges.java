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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class IndicesPrivileges implements ToXContentObject {

    public static final ParseField NAMES = new ParseField("names");
    public static final ParseField PRIVILEGES = new ParseField("privileges");
    public static final ParseField FIELD_PERMISSIONS = new ParseField("field_security");
    public static final ParseField GRANT_FIELDS = new ParseField("grant");
    public static final ParseField EXCEPT_FIELDS = new ParseField("except");
    public static final ParseField QUERY = new ParseField("query");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndicesPrivileges, Void> PARSER =
        new ConstructingObjectParser<>("indices_privileges", false, constructorObjects -> {
                int i = 0;
                final List<String> indices = (List<String>) constructorObjects[i++];
                final List<String> privilegeNames = (List<String>) constructorObjects[i++];
                final List<IndexPrivilege> privileges = privilegeNames.stream().map(IndexPrivilege::fromString)
                        .collect(Collectors.toList());
                final Tuple<List<String>, List<String>> fields =
                        (Tuple<List<String>, List<String>>) constructorObjects[i++];
                final String query = (String) constructorObjects[i];
                return new IndicesPrivileges(indices, privileges, fields.v1(), fields.v2(), query);
            });

    static {
        @SuppressWarnings("unchecked")
        final ConstructingObjectParser<Tuple<List<String>, List<String>>, Void> fls_parser =
                new ConstructingObjectParser<>( "field_level_parser", false, constructorObjects -> {
                        int i = 0;
                        final List<String> grantFields = (List<String>) constructorObjects[i++];
                        final List<String> exceptFields = (List<String>) constructorObjects[i];
                        return new Tuple<>(grantFields, exceptFields);
                    });
        fls_parser.declareStringArray(optionalConstructorArg(), GRANT_FIELDS);
        fls_parser.declareStringArray(optionalConstructorArg(), EXCEPT_FIELDS);

        PARSER.declareStringArray(constructorArg(), NAMES);
        PARSER.declareStringArray(constructorArg(), PRIVILEGES);
        PARSER.declareObject(optionalConstructorArg(), fls_parser, FIELD_PERMISSIONS);
        PARSER.declareStringOrNull(optionalConstructorArg(), QUERY);
    }

    private final List<String> indices;
    private final List<IndexPrivilege> privileges;
    // '*' means all fields (default value), empty means no fields
    private final List<String> grantedFields;
    // empty means no field is denied
    private final List<String> deniedFields;
    // missing query means all documents, i.e. no restrictions
    private final @Nullable String query;

    private IndicesPrivileges(List<String> indices, List<IndexPrivilege> privileges, @Nullable List<String> grantedFields,
            @Nullable List<String> deniedFields, @Nullable String query) {
        // we do all null checks inside the constructor
        if (null == indices || indices.isEmpty()) {
            throw new IllegalArgumentException("indices privileges must refer to at least one index name or index name pattern");
        }
        if (null == privileges || privileges.isEmpty()) {
            throw new IllegalArgumentException("indices privileges must define at least one privilege");
        }
        this.indices = Collections.unmodifiableList(indices);
        this.privileges = Collections.unmodifiableList(privileges);
        // all fields granted unless otherwise specified
        this.grantedFields = grantedFields != null ? Collections.unmodifiableList(grantedFields) : Collections.singletonList("*");
        // no fields are denied unless otherwise specified
        this.deniedFields = deniedFields != null ? Collections.unmodifiableList(deniedFields) : Collections.emptyList();
        this.query = query;
    }

    public List<String> getIndices() {
        return this.indices;
    }

    public List<IndexPrivilege> getPrivileges() {
        return this.privileges;
    }

    public List<String> getGrantedFields() {
        return this.grantedFields;
    }

    public List<String> getDeniedFields() {
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndicesPrivileges that = (IndicesPrivileges) o;
        return indices.equals(that.indices) && privileges.equals(that.privileges) && grantedFields.equals(that.grantedFields)
                && deniedFields.equals(that.deniedFields) && Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices, privileges, grantedFields, deniedFields, query);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("[");
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

    public static IndicesPrivileges fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private @Nullable List<String> indices = null;
        private @Nullable List<IndexPrivilege> privileges = null;
        private @Nullable List<String> grantedFields = null;
        private @Nullable List<String> deniedFields = null;
        private @Nullable String query = null;

        private Builder() {
        }

        public Builder indices(@Nullable String... indices) {
            if (indices == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
            return indices(Arrays.asList(indices));
        }
        
        public Builder indices(@Nullable List<String> indices) {
            this.indices = indices;
            return this;
        }

        public Builder privileges(@Nullable IndexPrivilege... privileges) {
            if (privileges == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
            return privileges(Arrays.asList(privileges));
        }

        public Builder privileges(@Nullable List<IndexPrivilege> privileges) {
            this.privileges = privileges;
            return this;
        }

        public Builder grantedFields(@Nullable String... grantedFields) {
            if (grantedFields == null) {
                // null is a no-op to be programmer friendly
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
                // null is a no-op to be programmer friendly
                return this;
            }
            return deniedFields(Arrays.asList(deniedFields));
        }

        public Builder deniedFields(@Nullable List<String> deniedFields) {
            if (deniedFields == null) {
                // null is a no-op to be programmer friendly
                return this;
            }
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
