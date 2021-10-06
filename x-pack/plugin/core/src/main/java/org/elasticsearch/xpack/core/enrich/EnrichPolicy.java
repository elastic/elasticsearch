/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.enrich;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an enrich policy including its configuration.
 */
public final class EnrichPolicy implements Writeable, ToXContentFragment {

    public static final String ENRICH_INDEX_NAME_BASE = ".enrich-";
    public static final String ENRICH_INDEX_PATTERN = ENRICH_INDEX_NAME_BASE + "*";

    public static final String MATCH_TYPE = "match";
    public static final String GEO_MATCH_TYPE = "geo_match";
    public static final String RANGE_TYPE = "range";
    public static final String[] SUPPORTED_POLICY_TYPES = new String[]{
        MATCH_TYPE,
        GEO_MATCH_TYPE,
        RANGE_TYPE
    };

    private static final ParseField QUERY = new ParseField("query");
    private static final ParseField INDICES = new ParseField("indices");
    private static final ParseField MATCH_FIELD = new ParseField("match_field");
    private static final ParseField ENRICH_FIELDS = new ParseField("enrich_fields");
    private static final ParseField ELASTICSEARCH_VERSION = new ParseField("elasticsearch_version");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<EnrichPolicy, String> PARSER = new ConstructingObjectParser<>(
        "policy",
        false,
        (args, policyType) -> new EnrichPolicy(
            policyType,
            (QuerySource) args[0],
            (List<String>) args[1],
            (String) args[2],
            (List<String>) args[3],
            (Version) args[4]
        )
    );

    static {
        declareCommonConstructorParsingOptions(PARSER);
    }

    private static <T> void declareCommonConstructorParsingOptions(ConstructingObjectParser<T, ?> parser) {
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            XContentBuilder contentBuilder = XContentBuilder.builder(p.contentType().xContent());
            contentBuilder.generator().copyCurrentStructure(p);
            return new QuerySource(BytesReference.bytes(contentBuilder), contentBuilder.contentType());
        }, QUERY);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), INDICES);
        parser.declareString(ConstructingObjectParser.constructorArg(), MATCH_FIELD);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), ENRICH_FIELDS);
        parser.declareField(ConstructingObjectParser.optionalConstructorArg(), ((p, c) -> Version.fromString(p.text())),
            ELASTICSEARCH_VERSION, ValueType.STRING);
    }

    public static EnrichPolicy fromXContent(XContentParser parser) throws IOException {
        Token token = parser.currentToken();
        if (token != Token.START_OBJECT) {
            token = parser.nextToken();
        }
        if (token != Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token");
        }
        token = parser.nextToken();
        if (token != Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token");
        }
        String policyType = parser.currentName();
        EnrichPolicy policy = PARSER.parse(parser, policyType);
        token = parser.nextToken();
        if (token != Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token");
        }
        return policy;
    }

    private final String type;
    private final QuerySource query;
    private final List<String> indices;
    private final String matchField;
    private final List<String> enrichFields;
    private final Version elasticsearchVersion;

    public EnrichPolicy(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readOptionalWriteable(QuerySource::new),
            in.readStringList(),
            in.readString(),
            in.readStringList(),
            Version.readVersion(in)
        );
    }

    public EnrichPolicy(String type,
                        QuerySource query,
                        List<String> indices,
                        String matchField,
                        List<String> enrichFields) {
        this(type, query, indices, matchField, enrichFields, Version.CURRENT);
    }

    public EnrichPolicy(String type,
                        QuerySource query,
                        List<String> indices,
                        String matchField,
                        List<String> enrichFields,
                        Version elasticsearchVersion) {
        this.type = type;
        this.query = query;
        this.indices = indices;
        this.matchField = matchField;
        this.enrichFields = enrichFields;
        this.elasticsearchVersion = elasticsearchVersion != null ? elasticsearchVersion : Version.CURRENT;
    }

    public String getType() {
        return type;
    }

    public QuerySource getQuery() {
        return query;
    }

    public List<String> getIndices() {
        return indices;
    }

    public String getMatchField() {
        return matchField;
    }

    public List<String> getEnrichFields() {
        return enrichFields;
    }

    public Version getElasticsearchVersion() {
        return elasticsearchVersion;
    }

    public static String getBaseName(String policyName) {
        return ENRICH_INDEX_NAME_BASE + policyName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeOptionalWriteable(query);
        out.writeStringCollection(indices);
        out.writeString(matchField);
        out.writeStringCollection(enrichFields);
        Version.writeVersion(elasticsearchVersion, out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(type);
        {
            toInnerXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    private void toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        if (query != null) {
            builder.field(QUERY.getPreferredName(), query.getQueryAsMap());
        }
        builder.array(INDICES.getPreferredName(), indices.toArray(new String[0]));
        builder.field(MATCH_FIELD.getPreferredName(), matchField);
        builder.array(ENRICH_FIELDS.getPreferredName(), enrichFields.toArray(new String[0]));
        if (params.paramAsBoolean("include_version", false) && elasticsearchVersion != null) {
            builder.field(ELASTICSEARCH_VERSION.getPreferredName(), elasticsearchVersion.toString());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichPolicy policy = (EnrichPolicy) o;
        return type.equals(policy.type) &&
            Objects.equals(query, policy.query) &&
            indices.equals(policy.indices) &&
            matchField.equals(policy.matchField) &&
            enrichFields.equals(policy.enrichFields) &&
            elasticsearchVersion.equals(policy.elasticsearchVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            type,
            query,
            indices,
            matchField,
            enrichFields,
            elasticsearchVersion
        );
    }

    public String toString() {
        return Strings.toString(this);
    }

    public static class QuerySource implements Writeable {

        private final BytesReference query;
        private final XContentType contentType;

        QuerySource(StreamInput in) throws IOException {
            this(in.readBytesReference(), in.readEnum(XContentType.class));
        }

        public QuerySource(BytesReference query, XContentType contentType) {
            this.query = query;
            this.contentType = contentType;
        }

        public BytesReference getQuery() {
            return query;
        }

        public Map<String, Object> getQueryAsMap() {
            return XContentHelper.convertToMap(query, true, contentType).v2();
        }

        public XContentType getContentType() {
            return contentType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(query);
            XContentHelper.writeTo(out, contentType);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QuerySource that = (QuerySource) o;
            return query.equals(that.query) &&
                contentType == that.contentType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, contentType);
        }
    }

    public static class NamedPolicy implements Writeable, ToXContentFragment {

        static final ParseField NAME = new ParseField("name");
        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<NamedPolicy, String> PARSER = new ConstructingObjectParser<>(
            "named_policy",
            false,
            (args, policyType) -> new NamedPolicy(
                (String) args[0],
                new EnrichPolicy(policyType,
                    (QuerySource) args[1],
                    (List<String>) args[2],
                    (String) args[3],
                    (List<String>) args[4],
                    (Version) args[5])
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
            declareCommonConstructorParsingOptions(PARSER);
        }

        private final String name;
        private final EnrichPolicy policy;

        public NamedPolicy(String name, EnrichPolicy policy) {
            this.name = name;
            this.policy = policy;
        }

        public NamedPolicy(StreamInput in) throws IOException {
            name = in.readString();
            policy = new EnrichPolicy(in);
        }

        public String getName() {
            return name;
        }

        public EnrichPolicy getPolicy() {
            return policy;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            policy.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(policy.type);
            {
                builder.field(NAME.getPreferredName(), name);
                policy.toInnerXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        public static NamedPolicy fromXContent(XContentParser parser) throws IOException {
            Token token = parser.currentToken();
            if (token != Token.START_OBJECT) {
                token = parser.nextToken();
            }
            if (token != Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token");
            }
            token = parser.nextToken();
            if (token != Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token");
            }
            String policyType = parser.currentName();
            token = parser.nextToken();
            if (token != Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token");
            }
            NamedPolicy policy = PARSER.parse(parser, policyType);
            token = parser.nextToken();
            if (token != Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token");
            }
            return policy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NamedPolicy that = (NamedPolicy) o;
            return name.equals(that.name) &&
                policy.equals(that.policy);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, policy);
        }
    }
}
