/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an enrich policy including its configuration.
 */
public final class EnrichPolicyDefinition implements Writeable, ToXContentFragment {

    private static final String ENRICH_INDEX_NAME_BASE = ".enrich-";

    public static final String EXACT_MATCH_TYPE = "exact_match";
    public static final String[] SUPPORTED_POLICY_TYPES = new String[]{EXACT_MATCH_TYPE};

    private static final ParseField TYPE = new ParseField("type");
    private static final ParseField QUERY = new ParseField("query");
    private static final ParseField INDICES = new ParseField("indices");
    private static final ParseField ENRICH_KEY = new ParseField("enrich_key");
    private static final ParseField ENRICH_VALUES = new ParseField("enrich_values");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<EnrichPolicyDefinition, Void> PARSER = new ConstructingObjectParser<>("policy",
        args -> new EnrichPolicyDefinition(
            (String) args[0],
            (QuerySource) args[1],
            (List<String>) args[2],
            (String) args[3],
            (List<String>) args[4]
        )
    );

    static {
        declareParserOptions(PARSER);
    }

    private static void declareParserOptions(ConstructingObjectParser<?, ?> parser) {
        parser.declareString(ConstructingObjectParser.constructorArg(), TYPE);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            XContentBuilder contentBuilder = XContentBuilder.builder(p.contentType().xContent());
            contentBuilder.generator().copyCurrentStructure(p);
            return new QuerySource(BytesReference.bytes(contentBuilder), contentBuilder.contentType());
        }, QUERY);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), INDICES);
        parser.declareString(ConstructingObjectParser.constructorArg(), ENRICH_KEY);
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), ENRICH_VALUES);
    }

    public static EnrichPolicyDefinition fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String type;
    private final QuerySource query;
    private final List<String> indices;
    private final String enrichKey;
    private final List<String> enrichValues;

    public EnrichPolicyDefinition(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readOptionalWriteable(QuerySource::new),
            in.readStringList(),
            in.readString(),
            in.readStringList()
        );
    }

    public EnrichPolicyDefinition(String type,
                                  QuerySource query,
                                  List<String> indices,
                                  String enrichKey,
                                  List<String> enrichValues) {
        this.type = type;
        this.query= query;
        this.indices = indices;
        this.enrichKey = enrichKey;
        this.enrichValues = enrichValues;
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

    public String getEnrichKey() {
        return enrichKey;
    }

    public List<String> getEnrichValues() {
        return enrichValues;
    }

    public static String getBaseName(String policyName) {
        return ENRICH_INDEX_NAME_BASE + policyName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeOptionalWriteable(query);
        out.writeStringCollection(indices);
        out.writeString(enrichKey);
        out.writeStringCollection(enrichValues);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TYPE.getPreferredName(), type);
        if (query != null) {
            builder.field(QUERY.getPreferredName(), query.getQueryAsMap());
        }
        builder.array(INDICES.getPreferredName(), indices.toArray(new String[0]));
        builder.field(ENRICH_KEY.getPreferredName(), enrichKey);
        builder.array(ENRICH_VALUES.getPreferredName(), enrichValues.toArray(new String[0]));
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichPolicyDefinition policy = (EnrichPolicyDefinition) o;
        return type.equals(policy.type) &&
            Objects.equals(query, policy.query) &&
            indices.equals(policy.indices) &&
            enrichKey.equals(policy.enrichKey) &&
            enrichValues.equals(policy.enrichValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            type,
            query,
            indices,
            enrichKey,
            enrichValues
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
            out.writeEnum(contentType);
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

    public static class NamedPolicy implements Writeable, ToXContent {

        static final ParseField NAME = new ParseField("name");
        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<NamedPolicy, Void> PARSER = new ConstructingObjectParser<>("named_policy",
            args -> new NamedPolicy(
                (String) args[0],
                new EnrichPolicyDefinition((String) args[1],
                    (QuerySource) args[2],
                    (List<String>) args[3],
                    (String) args[4],
                    (List<String>) args[5])
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
            declareParserOptions(PARSER);
        }

        private final String name;
        private final EnrichPolicyDefinition policy;

        public NamedPolicy(String name, EnrichPolicyDefinition policy) {
            this.name = name;
            this.policy = policy;
        }

        public NamedPolicy(StreamInput in) throws IOException {
            name = in.readString();
            policy = new EnrichPolicyDefinition(in);
        }

        public String getName() {
            return name;
        }

        public EnrichPolicyDefinition getPolicy() {
            return policy;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            policy.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(NAME.getPreferredName(), name);
                policy.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        public static NamedPolicy fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
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
