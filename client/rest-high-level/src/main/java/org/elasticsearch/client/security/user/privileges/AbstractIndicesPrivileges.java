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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public abstract class AbstractIndicesPrivileges {
    static final ParseField NAMES = new ParseField("names");
    static final ParseField ALLOW_RESTRICTED_INDICES = new ParseField("allow_restricted_indices");
    static final ParseField PRIVILEGES = new ParseField("privileges");
    static final ParseField FIELD_PERMISSIONS = new ParseField("field_security");
    static final ParseField QUERY = new ParseField("query");

    protected final List<String> indices;
    protected final List<String> privileges;
    protected final boolean allowRestrictedIndices;

    AbstractIndicesPrivileges(Collection<String> indices, Collection<String> privileges, boolean allowRestrictedIndices) {
        if (null == indices || indices.isEmpty()) {
            throw new IllegalArgumentException("indices privileges must refer to at least one index name or index name pattern");
        }
        if (null == privileges || privileges.isEmpty()) {
            throw new IllegalArgumentException("indices privileges must define at least one privilege");
        }
        this.indices = List.copyOf(indices);
        this.privileges = List.copyOf(privileges);
        this.allowRestrictedIndices = allowRestrictedIndices;
    }

    /**
     * The indices names covered by the privileges.
     */
    public List<String> getIndices() {
        return this.indices;
    }

    /**
     * The privileges acting over indices. There is a canonical predefined set of
     * such privileges, but the {@code String} datatype allows for flexibility in defining
     * finer grained privileges.
     */
    public List<String> getPrivileges() {
        return this.privileges;
    }

    /**
     * True if the privileges cover restricted internal indices too. Certain indices are reserved for internal services and should be
     * transparent to ordinary users. For that matter, when granting privileges, you also have to toggle this flag to confirm that all
     * indices, including restricted ones, are in the scope of this permission. By default this is false.
     */
    public boolean allowRestrictedIndices() {
        return this.allowRestrictedIndices;
    }

    /**
     * If {@code true} some documents might not be visible. Only the documents
     * matching {@code query} will be readable.
     */
    public abstract boolean isUsingDocumentLevelSecurity();

    /**
     * If {@code true} some document fields might not be visible.
     */
    public abstract boolean isUsingFieldLevelSecurity();

    public static class FieldSecurity implements ToXContentObject {
        static final ParseField GRANT_FIELDS = new ParseField("grant");
        static final ParseField EXCEPT_FIELDS = new ParseField("except");

        private static final ConstructingObjectParser<IndicesPrivileges.FieldSecurity, Void> PARSER = new ConstructingObjectParser<>(
            FIELD_PERMISSIONS.getPreferredName(), true, FieldSecurity::buildObjectFromParserArgs);

        @SuppressWarnings("unchecked")
        private static FieldSecurity buildObjectFromParserArgs(Object[] args) {
            return new FieldSecurity(
                (List<String>) args[0],
                (List<String>) args[1]
            );
        }

        static {
            PARSER.declareStringArray(optionalConstructorArg(), GRANT_FIELDS);
            PARSER.declareStringArray(optionalConstructorArg(), EXCEPT_FIELDS);
        }

        static FieldSecurity parse(XContentParser parser, Void context) throws IOException {
            return PARSER.parse(parser, context);
        }

        // null or singleton '*' means all fields are granted, empty means no fields are granted
        private final List<String> grantedFields;
        // null or empty means no fields are denied
        private final List<String> deniedFields;

        FieldSecurity(Collection<String> grantedFields, Collection<String> deniedFields) {
            // unspecified granted fields means no restriction
            this.grantedFields = grantedFields == null ? null : List.copyOf(grantedFields);
            // unspecified denied fields means no restriction
            this.deniedFields = deniedFields == null ? null : List.copyOf(deniedFields);
        }

        /**
         * The document fields that can be read or queried. Can be null, in this case
         * all the document's fields are granted access to. Can also be empty, in which
         * case no fields are granted access to.
         */
        public List<String> getGrantedFields() {
            return grantedFields;
        }

        /**
         * The document fields that cannot be accessed or queried. Can be null or empty,
         * in which case no fields are denied.
         */
        public List<String> getDeniedFields() {
            return deniedFields;
        }

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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (grantedFields == null) {
                // The role parser will reject a field_security object that doesn't have a "granted" field
                builder.field(GRANT_FIELDS.getPreferredName(), Collections.singletonList("*"));
            } else {
                builder.field(GRANT_FIELDS.getPreferredName(), grantedFields);
            }
            if (deniedFields != null) {
                builder.field(EXCEPT_FIELDS.getPreferredName(), deniedFields);
            }
            return builder.endObject();
        }

        @Override
        public String toString() {
            try {
                return XContentHelper.toXContent(this, XContentType.JSON, true).utf8ToString();
            } catch (IOException e) {
                throw new UncheckedIOException("Unexpected", e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final FieldSecurity that = (FieldSecurity) o;
            return Objects.equals(this.grantedFields, that.grantedFields) &&
                Objects.equals(this.deniedFields, that.deniedFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(grantedFields, deniedFields);
        }
    }

}
