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
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteria.CriteriaType.GLOBAL;

public class QueryRuleCriteria implements Writeable, ToXContentObject {
    private final CriteriaType criteriaType;
    private final String criteriaMetadata;
    private final List<Object> criteriaValues;

    private static final Logger logger = LogManager.getLogger(QueryRuleCriteria.class);

    public enum CriteriaType {
        GLOBAL {
            @Override
            public boolean inputMatchesCriteria(Object input, Object criteriaValue) {
                return true;
            }
        },
        EXACT {
            @Override
            public boolean inputMatchesCriteria(Object input, Object criteriaValue) {
                if (input instanceof String && criteriaValue instanceof String) {
                    return input.equals(criteriaValue);
                } else {
                    return parseDouble(input) == parseDouble(criteriaValue);
                }
            }
        },
        EXACT_FUZZY {
            @Override
            public boolean inputMatchesCriteria(Object input, Object criteriaValue) {
                final LevenshteinDistance ld = new LevenshteinDistance();
                if (input instanceof String && criteriaValue instanceof String) {
                    return ld.getDistance((String) input, (String) criteriaValue) > 0.5f;
                }
                return false;
            }
        },
        PREFIX {
            @Override
            public boolean inputMatchesCriteria(Object input, Object criteriaValue) {
                return ((String) input).startsWith((String) criteriaValue);
            }
        },
        SUFFIX {
            @Override
            public boolean inputMatchesCriteria(Object input, Object criteriaValue) {
                return ((String) input).endsWith((String) criteriaValue);
            }
        },
        CONTAINS {
            @Override
            public boolean inputMatchesCriteria(Object input, Object criteriaValue) {
                return ((String) input).contains((String) criteriaValue);
            }
        },
        LT {
            @Override
            public boolean inputMatchesCriteria(Object input, Object criteriaValue) {
                return parseDouble(input) < parseDouble(criteriaValue);
            }
        },
        LTE {
            @Override
            public boolean inputMatchesCriteria(Object input, Object criteriaValue) {
                return parseDouble(input) <= parseDouble(criteriaValue);
            }
        },
        GT {
            @Override
            public boolean inputMatchesCriteria(Object input, Object criteriaValue) {
                return parseDouble(input) > parseDouble(criteriaValue);
            }
        },
        GTE {
            @Override
            public boolean inputMatchesCriteria(Object input, Object criteriaValue) {
                validateInput(input);
                return parseDouble(input) >= parseDouble(criteriaValue);
            }
        };

        public void validateInput(Object input) {
            boolean isValid = isValidForInput(input);
            if (isValid == false) {
                throw new IllegalArgumentException("Input [" + input + "] is not valid for CriteriaType [" + this + "]");
            }
        }

        public abstract boolean inputMatchesCriteria(Object input, Object criteriaValue);

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

        private boolean isValidForInput(Object input) {
            if (this == EXACT) {
                return input instanceof String || input instanceof Number;
            } else if (List.of(EXACT_FUZZY, PREFIX, SUFFIX, CONTAINS).contains(this)) {
                return input instanceof String;
            } else if (List.of(LT, LTE, GT, GTE).contains(this)) {
                try {
                    if (input instanceof Number == false) {
                        parseDouble(input.toString());
                    }
                    return true;
                } catch (NumberFormatException e) {
                    return false;
                }
            }
            return false;
        }

        private static double parseDouble(Object input) {
            return (input instanceof Number) ? ((Number) input).doubleValue() : Double.parseDouble(input.toString());
        }
    }

    /**
     *
     * @param criteriaType The {@link CriteriaType}, indicating how the criteria is matched
     * @param criteriaMetadata The metadata for this identifier, indicating the criteria key of what is matched against.
     *                         Required unless the CriteriaType is GLOBAL.
     * @param criteriaValues The values to match against when evaluating {@link QueryRuleCriteria} against a {@link QueryRule}
     *                      Required unless the CriteriaType is GLOBAL.
     */
    public QueryRuleCriteria(CriteriaType criteriaType, @Nullable String criteriaMetadata, @Nullable List<Object> criteriaValues) {

        Objects.requireNonNull(criteriaType);

        if (criteriaType != GLOBAL) {
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
        this.criteriaType = in.readEnum(CriteriaType.class);
        this.criteriaMetadata = in.readOptionalString();
        this.criteriaValues = in.readOptionalList(StreamInput::readGenericValue);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(criteriaType);
        out.writeOptionalString(criteriaMetadata);
        out.writeOptionalCollection(criteriaValues, StreamOutput::writeGenericValue);
    }

    private static final ConstructingObjectParser<QueryRuleCriteria, String> PARSER = new ConstructingObjectParser<>(
        "query_rule_criteria",
        false,
        (params, resourceName) -> {
            final CriteriaType type = CriteriaType.criteriaType((String) params[0]);
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

    public CriteriaType criteriaType() {
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

    public boolean isMatch(Object matchValue, CriteriaType matchType) {
        if (matchType == GLOBAL) {
            return true;
        }
        final String matchString = matchValue.toString();
        for (Object criteriaValue : criteriaValues) {
            matchType.validateInput(matchValue);
            boolean matchFound = matchType.inputMatchesCriteria(matchString, criteriaValue);
            if (matchFound) {
                return true;
            }
        }
        return false;
    }
}
