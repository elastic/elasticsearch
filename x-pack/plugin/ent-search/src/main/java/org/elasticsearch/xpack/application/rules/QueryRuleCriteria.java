/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class QueryRuleCriteria implements Writeable, ToXContentObject {
    private final CriteriaType criteriaType;
    private final String criteriaMetadata;
    private final Object criteriaValue;

    public enum CriteriaType {
        EXACT;

        public static CriteriaType criteriaType(String criteriaType) {
            for (CriteriaType type : values()) {
                if (type.name().equalsIgnoreCase(criteriaType)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown CriteriaType: " + criteriaType);
        }
    }

    /**
     *
     * @param criteriaType The {@link CriteriaType}, indicating how the criteria is matched
     * @param criteriaMetadata The metadata for this identifier, indicating the criteria key of what is matched against
     * @param criteriaValue The value to match against when evaluating {@link QueryRuleCriteria} against a {@link QueryRule}
     */
    public QueryRuleCriteria(CriteriaType criteriaType, String criteriaMetadata, Object criteriaValue) {

        Objects.requireNonNull(criteriaType);
        Objects.requireNonNull(criteriaMetadata);
        Objects.requireNonNull(criteriaValue);

        if ((criteriaType == CriteriaType.EXACT) == false) {
            throw new IllegalArgumentException("Invalid criteriaType " + criteriaType);
        }

        if (Strings.isNullOrEmpty(criteriaMetadata)) {
            throw new IllegalArgumentException("criteriaMetadata cannot be blank");
        }

        this.criteriaType = criteriaType;
        this.criteriaMetadata = criteriaMetadata;
        this.criteriaValue = criteriaValue;
    }

    public QueryRuleCriteria(StreamInput in) throws IOException {
        this.criteriaType = in.readEnum(CriteriaType.class);
        this.criteriaMetadata = in.readString();
        this.criteriaValue = in.readGenericValue();
    }

    private static final ConstructingObjectParser<QueryRuleCriteria, String> PARSER = new ConstructingObjectParser<>(
        "query_rule_criteria",
        false,
        (params, resourceName) -> {
            final CriteriaType type = CriteriaType.criteriaType((String) params[0]);
            final String metadata = (String) params[1];
            final Object value = params[2];
            return new QueryRuleCriteria(type, metadata, value);
        }
    );

    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField METADATA_FIELD = new ParseField("metadata");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    static {
        PARSER.declareString(constructorArg(), TYPE_FIELD);
        PARSER.declareString(constructorArg(), METADATA_FIELD);
        PARSER.declareString(constructorArg(), VALUE_FIELD);
    }

    /**
     * Parses a {@link QueryRuleCriteria} from its {@param xContentType} representation in bytes.
     *
     * @param source The bytes that represents the {@link QueryRuleCriteria}.
     * @param xContentType The format of the representation.
     *
     * @return The parsed {@link QueryRuleCriteria}.
     */
    public static QueryRuleCriteria fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return QueryRuleCriteria.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    /**
     * Parses a {@link QueryRuleCriteria} through the provided {@param parser}.
     * @param parser The {@link XContentType} parser.
     *
     * @return The parsed {@link QueryRuleCriteria}.
     */
    public static QueryRuleCriteria fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(TYPE_FIELD.getPreferredName(), criteriaType);
            builder.field(METADATA_FIELD.getPreferredName(), criteriaMetadata);
            builder.field(VALUE_FIELD.getPreferredName(), criteriaValue);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(criteriaType);
        out.writeString(criteriaMetadata);
        out.writeGenericValue(criteriaValue);
    }

    public CriteriaType criteriaType() {
        return criteriaType;
    }

    public String criteriaMetadata() {
        return criteriaMetadata;
    }

    public Object criteriaValue() {
        return criteriaValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRuleCriteria that = (QueryRuleCriteria) o;
        return criteriaType == that.criteriaType
            && Objects.equals(criteriaMetadata, that.criteriaMetadata)
            && Objects.equals(criteriaValue, that.criteriaValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(criteriaType, criteriaMetadata, criteriaValue);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
