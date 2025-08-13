/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.search.MatchQueryParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Match query is a query that analyzes the text and constructs a phrase prefix
 * query as the result of the analysis.
 */
public class MatchPhrasePrefixQueryBuilder extends AbstractQueryBuilder<MatchPhrasePrefixQueryBuilder> {
    public static final String NAME = "match_phrase_prefix";
    public static final ParseField MAX_EXPANSIONS_FIELD = new ParseField("max_expansions");
    public static final ParseField ZERO_TERMS_QUERY_FIELD = new ParseField("zero_terms_query");

    private final String fieldName;

    private final Object value;

    private String analyzer;

    private int slop = MatchQueryParser.DEFAULT_PHRASE_SLOP;

    private int maxExpansions = FuzzyQuery.defaultMaxExpansions;

    private ZeroTermsQueryOption zeroTermsQuery = MatchQueryParser.DEFAULT_ZERO_TERMS_QUERY;

    public MatchPhrasePrefixQueryBuilder(String fieldName, Object value) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires fieldName");
        }
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    /**
     * Read from a stream.
     */
    public MatchPhrasePrefixQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        value = in.readGenericValue();
        slop = in.readVInt();
        maxExpansions = in.readVInt();
        analyzer = in.readOptionalString();
        zeroTermsQuery = ZeroTermsQueryOption.readFromStream(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(value);
        out.writeVInt(slop);
        out.writeVInt(maxExpansions);
        out.writeOptionalString(analyzer);
        zeroTermsQuery.writeTo(out);
    }

    /** Returns the field name used in this query. */
    public String fieldName() {
        return this.fieldName;
    }

    /** Returns the value used in this query. */
    public Object value() {
        return this.value;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping
     * config for the field, or, if not set, the default search analyzer.
     */
    public MatchPhrasePrefixQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /** Sets a slop factor for phrase queries */
    public MatchPhrasePrefixQueryBuilder slop(int slop) {
        if (slop < 0) {
            throw new IllegalArgumentException("No negative slop allowed.");
        }
        this.slop = slop;
        return this;
    }

    /** Get the slop factor for phrase queries. */
    public int slop() {
        return this.slop;
    }

    /**
     * The number of term expansions to use.
     */
    public MatchPhrasePrefixQueryBuilder maxExpansions(int maxExpansions) {
        if (maxExpansions < 0) {
            throw new IllegalArgumentException("No negative maxExpansions allowed.");
        }
        this.maxExpansions = maxExpansions;
        return this;
    }

    /**
     * Get the (optional) number of term expansions when using fuzzy or prefix
     * type query.
     */
    public int maxExpansions() {
        return this.maxExpansions;
    }

    /**
     * Sets query to use in case no query terms are available, e.g. after analysis removed them.
     * Defaults to {@link ZeroTermsQueryOption#NONE}, but can be set to
     * {@link ZeroTermsQueryOption#ALL} instead.
     */
    public MatchPhrasePrefixQueryBuilder zeroTermsQuery(ZeroTermsQueryOption zeroTermsQuery) {
        if (zeroTermsQuery == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires zeroTermsQuery to be non-null");
        }
        this.zeroTermsQuery = zeroTermsQuery;
        return this;
    }

    public ZeroTermsQueryOption zeroTermsQuery() {
        return this.zeroTermsQuery;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);

        builder.field(MatchQueryBuilder.QUERY_FIELD.getPreferredName(), value);
        if (analyzer != null) {
            builder.field(MatchQueryBuilder.ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        if (slop != MatchQueryParser.DEFAULT_PHRASE_SLOP) {
            builder.field(MatchPhraseQueryBuilder.SLOP_FIELD.getPreferredName(), slop);
        }
        if (maxExpansions != FuzzyQuery.defaultMaxExpansions) {
            builder.field(MAX_EXPANSIONS_FIELD.getPreferredName(), maxExpansions);
        }
        if (zeroTermsQuery != MatchQueryParser.DEFAULT_ZERO_TERMS_QUERY) {
            builder.field(ZERO_TERMS_QUERY_FIELD.getPreferredName(), zeroTermsQuery.toString());
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        // validate context specific fields
        if (analyzer != null && context.getIndexAnalyzers().get(analyzer) == null) {
            throw new QueryShardException(context, "[" + NAME + "] analyzer [" + analyzer + "] not found");
        }

        MatchQueryParser queryParser = new MatchQueryParser(context);
        if (analyzer != null) {
            queryParser.setAnalyzer(analyzer);
        }
        queryParser.setPhraseSlop(slop);
        queryParser.setMaxExpansions(maxExpansions);
        queryParser.setZeroTermsQuery(zeroTermsQuery);

        return queryParser.parse(MatchQueryParser.Type.PHRASE_PREFIX, fieldName, value);
    }

    @Override
    protected boolean doEquals(MatchPhrasePrefixQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(value, other.value)
            && Objects.equals(analyzer, other.analyzer)
            && Objects.equals(slop, other.slop)
            && Objects.equals(maxExpansions, other.maxExpansions)
            && Objects.equals(zeroTermsQuery, other.zeroTermsQuery);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, analyzer, slop, maxExpansions, zeroTermsQuery);
    }

    public static MatchPhrasePrefixQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzer = null;
        int slop = MatchQueryParser.DEFAULT_PHRASE_SLOP;
        int maxExpansion = FuzzyQuery.defaultMaxExpansions;
        String queryName = null;
        XContentParser.Token token;
        String currentFieldName = null;
        ZeroTermsQueryOption zeroTermsQuery = MatchQueryParser.DEFAULT_ZERO_TERMS_QUERY;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (MatchQueryBuilder.QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = parser.objectText();
                        } else if (MatchQueryBuilder.ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            analyzer = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (MatchPhraseQueryBuilder.SLOP_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            slop = parser.intValue();
                        } else if (MAX_EXPANSIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            maxExpansion = parser.intValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else if (ZERO_TERMS_QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            String zeroTermsValue = parser.text();
                            if ("none".equalsIgnoreCase(zeroTermsValue)) {
                                zeroTermsQuery = ZeroTermsQueryOption.NONE;
                            } else if ("all".equalsIgnoreCase(zeroTermsValue)) {
                                zeroTermsQuery = ZeroTermsQueryOption.ALL;
                            } else {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    "Unsupported zero_terms_query value [" + zeroTermsValue + "]"
                                );
                            }
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]"
                            );
                        }
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                        );
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                value = parser.objectText();
            }
        }

        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder(fieldName, value);
        matchQuery.analyzer(analyzer);
        matchQuery.slop(slop);
        matchQuery.maxExpansions(maxExpansion);
        matchQuery.queryName(queryName);
        matchQuery.boost(boost);
        matchQuery.zeroTermsQuery(zeroTermsQuery);
        return matchQuery;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
