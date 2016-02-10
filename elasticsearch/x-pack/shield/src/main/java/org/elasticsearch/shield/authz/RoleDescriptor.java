/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A holder for a Role that contains user-readable information about the Role
 * without containing the actual Role object.
 */
public class RoleDescriptor implements ToXContent {

    private final String name;
    private final String[] clusterPattern;
    private final List<IndicesPrivileges> indicesPrivileges;
    private final String[] runAs;

    public RoleDescriptor(String name, String[] clusterPattern,
                          List<IndicesPrivileges> indicesPrivileges, String[] runAs) {
        this.name = name;
        this.clusterPattern = clusterPattern;
        this.indicesPrivileges = indicesPrivileges;
        this.runAs = runAs;
    }

    public String getName() {
        return this.name;
    }

    public String[] getClusterPattern() {
        return this.clusterPattern;
    }

    public List<IndicesPrivileges> getIndicesPrivileges() {
        return this.indicesPrivileges;
    }

    public String[] getRunAs() {
        return this.runAs;
    }

    private static void validateIndexName(String idxName) throws ElasticsearchParseException {
        if (idxName == null) {
            return;
        }

        if (idxName.indexOf(",") != -1) {
            throw new ElasticsearchParseException("index name [" + idxName + "] may not contain ','");
        }
    }

    private static RoleDescriptor.IndicesPrivileges parseIndex(XContentParser parser) throws Exception {
        XContentParser.Token token;
        String currentFieldName = null;
        String[] idxNames = null;
        String query = null;
        List<String> privs = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("names".equals(currentFieldName)) {
                    String idxName = parser.text();
                    validateIndexName(idxName);
                    idxNames = new String[]{idxName};
                } else if ("query".equals(currentFieldName)) {
                    query = parser.text();
                } else {
                    throw new ElasticsearchParseException("unexpected field in add role request [{}]", currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                // expected
            } else if (token == XContentParser.Token.START_ARRAY && "names".equals(currentFieldName)) {
                List<String> idxNameList = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token.isValue()) {
                        String idxName = parser.text();
                        validateIndexName(idxName);
                        idxNameList.add(idxName);
                    } else {
                        throw new ElasticsearchParseException("unexpected object while parsing index names [{}]", token);
                    }
                }
                idxNames = idxNameList.toArray(Strings.EMPTY_ARRAY);
            } else if (token == XContentParser.Token.START_ARRAY && "privileges".equals(currentFieldName)) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token.isValue()) {
                        privs.add(parser.text());
                    } else {
                        throw new ElasticsearchParseException("unexpected object while parsing index privileges [{}]", token);
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY && "fields".equals(currentFieldName)) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token.isValue()) {
                        fields.add(parser.text());
                    } else {
                        throw new ElasticsearchParseException("unexpected object while parsing index fields [{}]", token);
                    }
                }
            } else {
                throw new ElasticsearchParseException("failed to parse add role request indices, got value with wrong type [{}]",
                        currentFieldName);
            }
        }
        if (idxNames == null || idxNames.length == 0) {
            throw new ElasticsearchParseException("'name' is a required field for index permissions");
        }
        if (privs.isEmpty()) {
            throw new ElasticsearchParseException("'privileges' is a required field for index permissions");
        }
        return RoleDescriptor.IndicesPrivileges.builder()
                .indices(idxNames)
                .privileges(privs.toArray(Strings.EMPTY_ARRAY))
                .fields(fields.isEmpty() ? null : fields.toArray(Strings.EMPTY_ARRAY))
                .query(query == null ? null : new BytesArray(query))
                .build();
    }

    private static List<RoleDescriptor.IndicesPrivileges> parseIndices(XContentParser parser) throws Exception {
        XContentParser.Token token;
        List<RoleDescriptor.IndicesPrivileges> tempIndices = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                tempIndices.add(parseIndex(parser));
            } else {
                throw new ElasticsearchParseException("unexpected type parsing index sub object [{}]", token);
            }
        }
        return tempIndices;
    }

    public static RoleDescriptor source(BytesReference source) throws Exception {
        try (XContentParser parser = XContentHelper.createParser(source)) {
            XContentParser.Token token;
            String currentFieldName = null;
            String roleName = null;
            List<IndicesPrivileges> indicesPrivileges = new ArrayList<>();
            List<String> runAsUsers = new ArrayList<>();
            List<String> tempClusterPriv = new ArrayList<>();
            parser.nextToken(); // remove object wrapping
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("name".equals(currentFieldName)) {
                        roleName = parser.text();
                    } else {
                        throw new ElasticsearchParseException("unexpected field in add role request [{}]", currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_ARRAY && "indices".equals(currentFieldName)) {
                    indicesPrivileges = parseIndices(parser);
                } else if (token == XContentParser.Token.START_ARRAY && "run_as".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token.isValue()) {
                            runAsUsers.add(parser.text());
                        } else {
                            throw new ElasticsearchParseException("unexpected value parsing run_as users [{}]", token);
                        }
                    }
                } else if (token == XContentParser.Token.START_ARRAY && "cluster".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token.isValue()) {
                            tempClusterPriv.add(parser.text());
                        } else {
                            throw new ElasticsearchParseException("unexpected value parsing cluster privileges [{}]", token);
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse add role request, got value with wrong type [{}]",
                            currentFieldName);
                }
            }
            if (roleName == null) {
                throw new ElasticsearchParseException("field [name] required for role description");
            }
            return new RoleDescriptor(roleName, tempClusterPriv.toArray(Strings.EMPTY_ARRAY),
                    indicesPrivileges, runAsUsers.toArray(Strings.EMPTY_ARRAY));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Role[");
        sb.append("name=").append(name);
        sb.append(", cluster=[").append(Strings.arrayToCommaDelimitedString(clusterPattern));
        sb.append("], indicesPrivileges=[");
        for (IndicesPrivileges group : indicesPrivileges) {
            sb.append(group.toString()).append(",");
        }
        sb.append("], runAs=[").append(Strings.arrayToCommaDelimitedString(runAs));
        sb.append("]]");
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("cluster", clusterPattern);
        builder.field("indices", indicesPrivileges);
        if (runAs != null) {
            builder.field("run_as", runAs);
        }
        builder.endObject();
        return builder;
    }

    public static RoleDescriptor readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        String[] clusterPattern = in.readStringArray();
        int size = in.readVInt();
        List<IndicesPrivileges> indicesPrivileges = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            IndicesPrivileges group = new IndicesPrivileges();
            group.readFrom(in);
            indicesPrivileges.add(group);
        }
        String[] runAs = in.readStringArray();
        return new RoleDescriptor(name, clusterPattern, indicesPrivileges, runAs);
    }

    public static void writeTo(RoleDescriptor descriptor, StreamOutput out) throws IOException {
        out.writeString(descriptor.getName());
        out.writeStringArray(descriptor.getClusterPattern());
        out.writeVInt(descriptor.getIndicesPrivileges().size());
        for (IndicesPrivileges group : descriptor.getIndicesPrivileges()) {
            group.writeTo(out);
        }
        out.writeStringArray(descriptor.getRunAs());
    }

    public static class IndicesPrivilegesBuilder {
        private String[] privileges;
        private String[] indices;
        private String[] fields;
        private BytesReference query;

        IndicesPrivilegesBuilder() {
        }

        public IndicesPrivilegesBuilder indices(String[] indices) {
            this.indices = indices;
            return this;
        }

        public IndicesPrivilegesBuilder privileges(String[] privileges) {
            this.privileges = privileges;
            return this;
        }

        public IndicesPrivilegesBuilder fields(@Nullable String[] fields) {
            this.fields = fields;
            return this;
        }

        public IndicesPrivilegesBuilder query(@Nullable BytesReference query) {
            this.query = query;
            return this;
        }

        public IndicesPrivileges build() {
            return new IndicesPrivileges(privileges, indices, fields, query);
        }
    }

    /**
     * A class representing permissions for a group of indices mapped to
     * privileges, fields, and a query.
     */
    public static class IndicesPrivileges implements ToXContent, Streamable {

        private String[] privileges;
        private String[] indices;
        private String[] fields;
        private BytesReference query;

        private IndicesPrivileges() {
        }

        IndicesPrivileges(String[] privileges, String[] indices,
                          @Nullable String[] fields, @Nullable BytesReference query) {
            this.privileges = privileges;
            this.indices = indices;
            this.fields = fields;
            this.query = query;
        }

        public static IndicesPrivilegesBuilder builder() {
            return new IndicesPrivilegesBuilder();
        }

        public String[] getPrivileges() {
            return this.privileges;
        }

        public String[] getIndices() {
            return this.indices;
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
            sb.append("privileges=[").append(Strings.arrayToCommaDelimitedString(privileges));
            sb.append("], indices=[").append(Strings.arrayToCommaDelimitedString(indices));
            sb.append("], fields=[").append(Strings.arrayToCommaDelimitedString(fields));
            if (query != null) {
                sb.append("], query=").append(query.toUtf8());
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(); // start
            builder.array("names", indices);
            builder.array("privileges", privileges);
            if (fields != null) {
                builder.array("fields", fields);
            }
            if (query != null) {
                builder.field("query", query.toUtf8());
            }
            builder.endObject(); // end start
            return builder;
        }

        public static IndicesPrivileges readIndicesPrivileges(StreamInput in) throws IOException {
            IndicesPrivileges ip = new IndicesPrivileges();
            ip.readFrom(in);
            return ip;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.privileges = in.readStringArray();
            this.indices = in.readStringArray();
            this.fields = in.readOptionalStringArray();
            if (in.readBoolean()) {
                this.query = new BytesArray(in.readByteArray());
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(privileges);
            out.writeStringArray(indices);
            out.writeOptionalStringArray(fields);
            if (query != null) {
                out.writeBoolean(true);
                out.writeByteArray(query.array());
            } else {
                out.writeBoolean(false);
            }
        }
    }
}
