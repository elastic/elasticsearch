/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.apache.lucene.search.spell.LevenshteinDistance;
import org.elasticsearch.ElasticsearchParseException;
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
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.GLOBAL;

public class QueryRuleCriteria implements Writeable, ToXContentObject {
    private final CriteriaType criteriaType;
    private final String criteriaMetadata;
    private final Object criteriaValue;

    private final Logger logger = LogManager.getLogger(QueryRuleCriteria.class);

    public enum CriteriaType {
        GLOBAL,
        EXACT,
        EXACT_FUZZY,
        PREFIX,
        SUFFIX,
        CONTAINS,
        LT,
        LTE,
        GT,
        GTE;

        public static CriteriaType criteriaType(String criteriaType) {
            for (CriteriaType type : values()) {
                if (type.name().equalsIgnoreCase(criteriaType)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown CriteriaType: " + criteriaType);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     *
     * @param criteriaType The {@link CriteriaType}, indicating how the criteria is matched
     * @param criteriaMetadata The metadata for this identifier, indicating the criteria key of what is matched against.
     *                         Required unless the CriteriaType is GLOBAL.
     * @param criteriaValue The value to match against when evaluating {@link QueryRuleCriteria} against a {@link QueryRule}
     *                      Required unless the CriteriaType is GLOBAL.
     */
    public QueryRuleCriteria(CriteriaType criteriaType, @Nullable String criteriaMetadata, @Nullable Object criteriaValue) {

        Objects.requireNonNull(criteriaType);

        if (criteriaType != GLOBAL) {
            if (Strings.isNullOrEmpty(criteriaMetadata)) {
                throw new IllegalArgumentException("criteriaMetadata cannot be blank");
            }
            Objects.requireNonNull(criteriaValue);
        }

        this.criteriaMetadata = criteriaMetadata;
        this.criteriaValue = criteriaValue;
        this.criteriaType = criteriaType;

    }

    public QueryRuleCriteria(StreamInput in) throws IOException {
        this.criteriaType = in.readEnum(CriteriaType.class);
        this.criteriaMetadata = in.readOptionalString();
        this.criteriaValue = in.readGenericValue();
    }

    private static final ConstructingObjectParser<QueryRuleCriteria, String> PARSER = new ConstructingObjectParser<>(
        "query_rule_criteria",
        false,
        (params, resourceName) -> {
            final CriteriaType type = CriteriaType.criteriaType((String) params[0]);
            final String metadata = params.length >= 3 ? (String) params[1] : null;
            final Object value = params.length >= 3 ? params[2] : null;
            return new QueryRuleCriteria(type, metadata, value);
        }
    );

    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField METADATA_FIELD = new ParseField("metadata");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    static {
        PARSER.declareString(constructorArg(), TYPE_FIELD);
        PARSER.declareStringOrNull(optionalConstructorArg(), METADATA_FIELD);
        PARSER.declareStringOrNull(optionalConstructorArg(), VALUE_FIELD);
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
            if (criteriaValue != null) {
                builder.field(VALUE_FIELD.getPreferredName(), criteriaValue);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(criteriaType);
        out.writeOptionalString(criteriaMetadata);
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

    public boolean isMatch(Object matchValue, CriteriaType matchType) {

        if (criteriaType == GLOBAL) {
            logger.info("Global criteria type, returning true");
            return true;
        }

        LevenshteinDistance ld = new LevenshteinDistance();

        String matchString = matchValue.toString();
        String criteriaValueString = criteriaValue != null ? criteriaValue.toString() : null;
        if (criteriaValueString != null && matchValue instanceof String) {

            float editDistance = ld.getDistance(matchString, criteriaValueString);
            logger.info("Edit distance between {} and {} is {}", matchString, criteriaValueString, editDistance);

            boolean strMatch = switch (matchType) {
                case EXACT -> matchString.equals(criteriaValueString);
                case EXACT_FUZZY -> ld.getDistance(matchString, criteriaValueString) > 0.75f; // TODO make configurable
                case PREFIX -> matchString.startsWith(criteriaValueString);
                case SUFFIX -> matchString.endsWith(criteriaValueString);
                case CONTAINS -> matchString.contains(criteriaValueString);
                default -> false;
            };
            logger.info("strMatch: " + strMatch);
            return strMatch;
        } else if (criteriaValueString != null && matchValue instanceof Number) {
            try {
                Number matchNumber = (Number) matchValue;
                boolean numMatch = switch (matchType) {
                    case EXACT -> matchNumber.doubleValue() == Double.parseDouble(criteriaValueString);
                    case LT -> matchNumber.doubleValue() < Double.parseDouble(criteriaValueString);
                    case LTE -> matchNumber.doubleValue() <= Double.parseDouble(criteriaValueString);
                    case GT -> matchNumber.doubleValue() > Double.parseDouble(criteriaValueString);
                    case GTE -> matchNumber.doubleValue() >= Double.parseDouble(criteriaValueString);
                    default -> false;
                };
                logger.info("matchType " + matchType + " matchNumber " + matchNumber + " criteriaValueString " + criteriaValueString + "; numMatch: " + numMatch);
                return numMatch;
            } catch (NumberFormatException e) {
                logger.warn("Query rule criteria [" + criteriaValue + "] type mismatch against [" + matchString + "]", e);
                return false;
            }
        }

        return false;
    }
}
