/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.ALWAYS;

public class QueryRuleCriteria implements Writeable, ToXContentObject {

    public static final TransportVersion CRITERIA_METADATA_VALUES_TRANSPORT_VERSION = TransportVersions.V_8_10_X;
    private final QueryRuleCriteriaType criteriaType;
    private final String criteriaMetadata;
    private final List<Object> criteriaValues;

    private static final Logger logger = LogManager.getLogger(QueryRuleCriteria.class);

    /**
     *
     * @param criteriaType The {@link QueryRuleCriteriaType}, indicating how the criteria is matched
     * @param criteriaMetadata The metadata for this identifier, indicating the criteria key of what is matched against.
     *                         Required unless the CriteriaType is ALWAYS.
     * @param criteriaValues The values to match against when evaluating {@link QueryRuleCriteria} against a {@link QueryRule}
     *                      Required unless the CriteriaType is ALWAYS.
     */
    public QueryRuleCriteria(QueryRuleCriteriaType criteriaType, @Nullable String criteriaMetadata, @Nullable List<Object> criteriaValues) {

        Objects.requireNonNull(criteriaType);

        if (criteriaType != ALWAYS) {
            if (Strings.isNullOrEmpty(criteriaMetadata)) {
                throw new IllegalArgumentException("criteriaMetadata cannot be blank");
            }
            if (criteriaValues == null || criteriaValues.isEmpty()) {
                throw new IllegalArgumentException("criteriaValues cannot be null or empty");
            }
        }

        this.criteriaMetadata = criteriaMetadata;
        this.criteriaValues = criteriaValues;
        this.criteriaType = criteriaType;

    }

    public QueryRuleCriteria(StreamInput in) throws IOException {
        this.criteriaType = in.readEnum(QueryRuleCriteriaType.class);
        if (in.getTransportVersion().onOrAfter(CRITERIA_METADATA_VALUES_TRANSPORT_VERSION)) {
            this.criteriaMetadata = in.readOptionalString();
            this.criteriaValues = in.readOptionalCollectionAsList(StreamInput::readGenericValue);
        } else {
            this.criteriaMetadata = in.readString();
            this.criteriaValues = List.of(in.readGenericValue());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(criteriaType);
        if (out.getTransportVersion().onOrAfter(CRITERIA_METADATA_VALUES_TRANSPORT_VERSION)) {
            out.writeOptionalString(criteriaMetadata);
            out.writeOptionalCollection(criteriaValues, StreamOutput::writeGenericValue);
        } else {
            out.writeString(criteriaMetadata);
            out.writeGenericValue(criteriaValues().get(0));
        }
    }

    private static final ConstructingObjectParser<QueryRuleCriteria, String> PARSER = new ConstructingObjectParser<>(
        "query_rule_criteria",
        false,
        (params, resourceName) -> {
            final QueryRuleCriteriaType type = QueryRuleCriteriaType.type((String) params[0]);
            final String metadata = params.length >= 3 ? (String) params[1] : null;
            @SuppressWarnings("unchecked")
            final List<Object> values = params.length >= 3 ? (List<Object>) params[2] : null;
            return new QueryRuleCriteria(type, metadata, values);
        }
    );

    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField METADATA_FIELD = new ParseField("metadata");
    public static final ParseField VALUES_FIELD = new ParseField("values");

    static {
        PARSER.declareString(constructorArg(), TYPE_FIELD);
        PARSER.declareStringOrNull(optionalConstructorArg(), METADATA_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), VALUES_FIELD);
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
            if (criteriaMetadata != null) {
                builder.field(METADATA_FIELD.getPreferredName(), criteriaMetadata);
            }
            if (criteriaValues != null) {
                builder.array(VALUES_FIELD.getPreferredName(), criteriaValues.toArray());
            }
        }
        builder.endObject();
        return builder;
    }

    public QueryRuleCriteriaType criteriaType() {
        return criteriaType;
    }

    public String criteriaMetadata() {
        return criteriaMetadata;
    }

    public List<Object> criteriaValues() {
        return criteriaValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRuleCriteria that = (QueryRuleCriteria) o;
        return criteriaType == that.criteriaType
            && Objects.equals(criteriaMetadata, that.criteriaMetadata)
            && Objects.equals(criteriaValues, that.criteriaValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(criteriaType, criteriaMetadata, criteriaValues);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public boolean isMatch(Object matchValue, QueryRuleCriteriaType matchType) {
        return isMatch(matchValue, matchType, true);
    }

    public boolean isMatch(Object matchValue, QueryRuleCriteriaType matchType, boolean throwOnInvalidInput) {
        if (matchType == ALWAYS) {
            return true;
        }
        final String matchString = matchValue.toString();
        for (Object criteriaValue : criteriaValues) {
            boolean isValid = matchType.validateInput(matchValue, throwOnInvalidInput);
            if (isValid == false) {
                return false;
            }
            boolean matchFound = matchType.isMatch(matchString, criteriaValue);
            if (matchFound) {
                return true;
            }
        }
        return false;
    }
}
