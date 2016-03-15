/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.shield.support.Validation;
import org.elasticsearch.xpack.common.xcontent.XContentUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A holder for a Role that contains user-readable information about the Role
 * without containing the actual Role object.
 */
public class RoleDescriptor implements ToXContent {

    private final String name;
    private final String[] clusterPrivileges;
    private final IndicesPrivileges[] indicesPrivileges;
    private final String[] runAs;

    public RoleDescriptor(String name,
                          @Nullable String[] clusterPrivileges,
                          @Nullable IndicesPrivileges[] indicesPrivileges,
                          @Nullable String[] runAs) {

        this.name = name;
        this.clusterPrivileges = clusterPrivileges != null ? clusterPrivileges : Strings.EMPTY_ARRAY;
        this.indicesPrivileges = indicesPrivileges != null ? indicesPrivileges : IndicesPrivileges.NONE;
        this.runAs = runAs != null ? runAs : Strings.EMPTY_ARRAY;
    }

    public String getName() {
        return this.name;
    }

    public String[] getClusterPrivileges() {
        return this.clusterPrivileges;
    }

    public IndicesPrivileges[] getIndicesPrivileges() {
        return this.indicesPrivileges;
    }

    public String[] getRunAs() {
        return this.runAs;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Role[");
        sb.append("name=").append(name);
        sb.append(", cluster=[").append(Strings.arrayToCommaDelimitedString(clusterPrivileges));
        sb.append("], indicesPrivileges=[");
        for (IndicesPrivileges group : indicesPrivileges) {
            sb.append(group.toString()).append(",");
        }
        sb.append("], runAs=[").append(Strings.arrayToCommaDelimitedString(runAs));
        sb.append("]]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RoleDescriptor that = (RoleDescriptor) o;

        if (!name.equals(that.name)) return false;
        if (!Arrays.equals(clusterPrivileges, that.clusterPrivileges)) return false;
        if (!Arrays.equals(indicesPrivileges, that.indicesPrivileges)) return false;
        return Arrays.equals(runAs, that.runAs);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + Arrays.hashCode(clusterPrivileges);
        result = 31 * result + Arrays.hashCode(indicesPrivileges);
        result = 31 * result + Arrays.hashCode(runAs);
        return result;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("cluster", (Object[]) clusterPrivileges);
        builder.field("indices", (Object[]) indicesPrivileges);
        if (runAs != null) {
            builder.field("run_as", runAs);
        }
        return builder.endObject();
    }

    public static RoleDescriptor readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        String[] clusterPrivileges = in.readStringArray();
        int size = in.readVInt();
        IndicesPrivileges[] indicesPrivileges = new IndicesPrivileges[size];
        for (int i = 0; i < size; i++) {
            indicesPrivileges[i] = IndicesPrivileges.createFrom(in);
        }
        String[] runAs = in.readStringArray();
        return new RoleDescriptor(name, clusterPrivileges, indicesPrivileges, runAs);
    }

    public static void writeTo(RoleDescriptor descriptor, StreamOutput out) throws IOException {
        out.writeString(descriptor.name);
        out.writeStringArray(descriptor.clusterPrivileges);
        out.writeVInt(descriptor.indicesPrivileges.length);
        for (IndicesPrivileges group : descriptor.indicesPrivileges) {
            group.writeTo(out);
        }
        out.writeStringArray(descriptor.runAs);
    }

    public static RoleDescriptor parse(String name, BytesReference source) throws IOException {
        assert name != null;
        try (XContentParser parser = XContentHelper.createParser(source)) {
            return parse(name, parser);
        }
    }

    public static RoleDescriptor parse(String name, XContentParser parser) throws IOException {
        // validate name
        Validation.Error validationError = Validation.Roles.validateRoleName(name);
        if (validationError != null) {
            ValidationException ve = new ValidationException();
            ve.addValidationError(validationError.toString());
            throw ve;
        }

        // advance to the START_OBJECT token if needed
        XContentParser.Token token = parser.currentToken() == null ? parser.nextToken() : parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse role [{}]. expected an object but found [{}] instead", name, token);
        }
        String currentFieldName = null;
        IndicesPrivileges[] indicesPrivileges = null;
        String[] clusterPrivileges = null;
        String[] runAsUsers = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Fields.INDICES)) {
                indicesPrivileges = parseIndices(name, parser);
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Fields.RUN_AS)) {
                runAsUsers = readStringArray(name, parser, true);
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Fields.CLUSTER)) {
                clusterPrivileges = readStringArray(name, parser, true);
            } else {
                throw new ElasticsearchParseException("failed to parse role [{}]. unexpected field [{}]", name, currentFieldName);
            }
        }
        return new RoleDescriptor(name, clusterPrivileges, indicesPrivileges, runAsUsers);
    }

    private static String[] readStringArray(String roleName, XContentParser parser, boolean allowNull) throws IOException {
        try {
            return XContentUtils.readStringArray(parser, allowNull);
        } catch (ElasticsearchParseException e) {
            // re-wrap in order to add the role name
            throw new ElasticsearchParseException("failed to parse role [{}]", e, roleName);
        }
    }

    private static RoleDescriptor.IndicesPrivileges[] parseIndices(String roleName, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. expected field [{}] value " +
                    "to be an array, but found [{}] instead", roleName, parser.currentName(), parser.currentToken());
        }
        List<RoleDescriptor.IndicesPrivileges> privileges = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            privileges.add(parseIndex(roleName, parser));
        }
        return privileges.toArray(new IndicesPrivileges[privileges.size()]);
    }

    private static RoleDescriptor.IndicesPrivileges parseIndex(String roleName, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. expected field [{}] value to " +
                    "be an array of objects, but found an array element of type [{}]", roleName, parser.currentName(), token);
        }
        String currentFieldName = null;
        String[] names = null;
        String query = null;
        String[] privileges = null;
        String[] fields = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Fields.NAMES)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    names = new String[] { parser.text() };
                } else if (token == XContentParser.Token.START_ARRAY) {
                    names = readStringArray(roleName, parser, false);
                    if (names.length == 0) {
                        throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. [{}] cannot be an empty " +
                                "array", roleName, currentFieldName);
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. expected field [{}] " +
                            "value to be a string or an array of strings, but found [{}] instead", roleName, currentFieldName, token);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Fields.QUERY)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    XContentBuilder builder = JsonXContent.contentBuilder();
                    XContentHelper.copyCurrentStructure(builder.generator(), parser);
                    query = builder.string();
                } else {
                    query = parser.textOrNull();
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Fields.PRIVILEGES)) {
                privileges = readStringArray(roleName, parser, true);
                if (names.length == 0) {
                    throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. [{}] cannot be an empty " +
                            "array", roleName, currentFieldName);
                }
            } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Fields.FIELDS)) {
                fields = readStringArray(roleName, parser, true);
            } else {
                throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. unexpected field [{}]",
                        roleName, currentFieldName);
            }
        }
        if (names == null) {
            throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. missing required [{}] field",
                    roleName, Fields.NAMES.getPreferredName());
        }
        if (privileges == null) {
            throw new ElasticsearchParseException("failed to parse indices privileges for role [{}]. missing required [{}] field",
                    roleName, Fields.PRIVILEGES.getPreferredName());
        }
        return RoleDescriptor.IndicesPrivileges.builder()
                .indices(names)
                .privileges(privileges)
                .fields(fields)
                .query(query)
                .build();
    }

    /**
     * A class representing permissions for a group of indices mapped to
     * privileges, fields, and a query.
     */
    public static class IndicesPrivileges implements ToXContent, Streamable {

        private static final IndicesPrivileges[] NONE = new IndicesPrivileges[0];

        private String[] indices;
        private String[] privileges;
        private String[] fields;
        private BytesReference query;

        private IndicesPrivileges() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public String[] getIndices() {
            return this.indices;
        }

        public String[] getPrivileges() {
            return this.privileges;
        }

        @Nullable
        public String[] getFields() {
            return this.fields;
        }

        @Nullable
        public BytesReference getQuery() {
            return this.query;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("IndicesPrivileges[");
            sb.append("indices=[").append(Strings.arrayToCommaDelimitedString(indices));
            sb.append("], privileges=[").append(Strings.arrayToCommaDelimitedString(privileges));
            sb.append("], fields=[").append(Strings.arrayToCommaDelimitedString(fields));
            if (query != null) {
                sb.append("], query=").append(query.toUtf8());
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IndicesPrivileges that = (IndicesPrivileges) o;

            if (!Arrays.equals(indices, that.indices)) return false;
            if (!Arrays.equals(privileges, that.privileges)) return false;
            if (!Arrays.equals(fields, that.fields)) return false;
            return !(query != null ? !query.equals(that.query) : that.query != null);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(indices);
            result = 31 * result + Arrays.hashCode(privileges);
            result = 31 * result + Arrays.hashCode(fields);
            result = 31 * result + (query != null ? query.hashCode() : 0);
            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.array("names", indices);
            builder.array("privileges", privileges);
            if (fields != null) {
                builder.array("fields", fields);
            }
            if (query != null) {
                builder.field("query", query.toUtf8());
            }
            return builder.endObject();
        }

        public static IndicesPrivileges createFrom(StreamInput in) throws IOException {
            IndicesPrivileges ip = new IndicesPrivileges();
            ip.readFrom(in);
            return ip;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.indices = in.readStringArray();
            this.fields = in.readOptionalStringArray();
            this.privileges = in.readStringArray();
            if (in.readBoolean()) {
                this.query = new BytesArray(in.readByteArray());
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(indices);
            out.writeOptionalStringArray(fields);
            out.writeStringArray(privileges);
            if (query != null) {
                out.writeBoolean(true);
                out.writeByteArray(query.array());
            } else {
                out.writeBoolean(false);
            }
        }

        public static class Builder {

            private IndicesPrivileges indicesPrivileges = new IndicesPrivileges();

            private Builder() {
            }

            public Builder indices(String... indices) {
                indicesPrivileges.indices = indices;
                return this;
            }

            public Builder privileges(String... privileges) {
                indicesPrivileges.privileges = privileges;
                return this;
            }

            public Builder fields(@Nullable String... fields) {
                indicesPrivileges.fields = fields;
                return this;
            }

            public Builder query(@Nullable String query) {
                return query(query == null ? null : new BytesArray(query));
            }

            public Builder query(@Nullable BytesReference query) {
                indicesPrivileges.query = query;
                return this;
            }

            public IndicesPrivileges build() {
                if (indicesPrivileges.indices == null || indicesPrivileges.indices.length == 0) {
                    throw new IllegalArgumentException("indices privileges must refer to at least one index name or index name pattern");
                }
                if (indicesPrivileges.privileges == null || indicesPrivileges.privileges.length == 0) {
                    throw new IllegalArgumentException("indices privileges must define at least one privilege");
                }
                return indicesPrivileges;
            }
        }
    }

    public interface Fields {
        ParseField CLUSTER = new ParseField("cluster");
        ParseField INDICES = new ParseField("indices");
        ParseField RUN_AS = new ParseField("run_as");
        ParseField NAMES = new ParseField("names");
        ParseField QUERY = new ParseField("query");
        ParseField PRIVILEGES = new ParseField("privileges");
        ParseField FIELDS = new ParseField("fields");
    }
}
