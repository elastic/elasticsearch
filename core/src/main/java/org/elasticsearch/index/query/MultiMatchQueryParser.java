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

package org.elasticsearch.index.query;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.search.MatchQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Same as {@link MatchQueryParser} but has support for multiple fields.
 */
public class MultiMatchQueryParser implements QueryParser<MultiMatchQueryBuilder> {

    public static final ParseField SLOP_FIELD = new ParseField("slop", "phrase_slop");
    public static final ParseField ZERO_TERMS_QUERY_FIELD = new ParseField("zero_terms_query");
    public static final ParseField LENIENT_FIELD = new ParseField("lenient");
    public static final ParseField CUTOFF_FREQUENCY_FIELD = new ParseField("cutoff_frequency");
    public static final ParseField TIE_BREAKER_FIELD = new ParseField("tie_breaker");
    public static final ParseField USE_DIS_MAX_FIELD = new ParseField("use_dis_max");
    public static final ParseField FUZZY_REWRITE_FIELD = new ParseField("fuzzy_rewrite");
    public static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    public static final ParseField OPERATOR_FIELD = new ParseField("operator");
    public static final ParseField MAX_EXPANSIONS_FIELD = new ParseField("max_expansions");
    public static final ParseField PREFIX_LENGTH_FIELD = new ParseField("prefix_length");
    public static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");

    @Override
    public String[] names() {
        return new String[]{
                MultiMatchQueryBuilder.NAME, "multiMatch"
        };
    }

    @Override
    public MultiMatchQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        Object value = null;
        Map<String, Float> fieldsBoosts = new HashMap<>();
        MultiMatchQueryBuilder.Type type = MultiMatchQueryBuilder.DEFAULT_TYPE;
        String analyzer = null;
        int slop = MultiMatchQueryBuilder.DEFAULT_PHRASE_SLOP;
        Fuzziness fuzziness = null;
        int prefixLength = MultiMatchQueryBuilder.DEFAULT_PREFIX_LENGTH;
        int maxExpansions = MultiMatchQueryBuilder.DEFAULT_MAX_EXPANSIONS;
        Operator operator = MultiMatchQueryBuilder.DEFAULT_OPERATOR;
        String minimumShouldMatch = null;
        String fuzzyRewrite = null;
        Boolean useDisMax = null;
        Float tieBreaker = null;
        Float cutoffFrequency = null;
        boolean lenient = MultiMatchQueryBuilder.DEFAULT_LENIENCY;
        MatchQuery.ZeroTermsQuery zeroTermsQuery = MultiMatchQueryBuilder.DEFAULT_ZERO_TERMS_QUERY;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.parseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
                if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseFieldAndBoost(parser, fieldsBoosts);
                    }
                } else if (token.isValue()) {
                    parseFieldAndBoost(parser, fieldsBoosts);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + MultiMatchQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    value = parser.objectText();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, TYPE_FIELD)) {
                    type = MultiMatchQueryBuilder.Type.parse(parser.text(), parseContext.parseFieldMatcher());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, ANALYZER_FIELD)) {
                    analyzer = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, SLOP_FIELD)) {
                    slop = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Fuzziness.FIELD)) {
                    fuzziness = Fuzziness.parse(parser);
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, PREFIX_LENGTH_FIELD)) {
                    prefixLength = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MAX_EXPANSIONS_FIELD)) {
                    maxExpansions = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, OPERATOR_FIELD)) {
                    operator = Operator.fromString(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MINIMUM_SHOULD_MATCH_FIELD)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, FUZZY_REWRITE_FIELD)) {
                    fuzzyRewrite = parser.textOrNull();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, USE_DIS_MAX_FIELD)) {
                    useDisMax = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, TIE_BREAKER_FIELD)) {
                    tieBreaker = parser.floatValue();
                }  else if (parseContext.parseFieldMatcher().match(currentFieldName, CUTOFF_FREQUENCY_FIELD)) {
                    cutoffFrequency = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, LENIENT_FIELD)) {
                    lenient = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, ZERO_TERMS_QUERY_FIELD)) {
                    String zeroTermsDocs = parser.text();
                    if ("none".equalsIgnoreCase(zeroTermsDocs)) {
                        zeroTermsQuery = MatchQuery.ZeroTermsQuery.NONE;
                    } else if ("all".equalsIgnoreCase(zeroTermsDocs)) {
                        zeroTermsQuery = MatchQuery.ZeroTermsQuery.ALL;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "Unsupported zero_terms_docs value [" + zeroTermsDocs + "]");
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + MultiMatchQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + MultiMatchQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }

        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "No text specified for multi_match query");
        }

        if (fieldsBoosts.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "No fields specified for multi_match query");
        }

        return new MultiMatchQueryBuilder(value)
                .fields(fieldsBoosts)
                .type(type)
                .analyzer(analyzer)
                .cutoffFrequency(cutoffFrequency)
                .fuzziness(fuzziness)
                .fuzzyRewrite(fuzzyRewrite)
                .useDisMax(useDisMax)
                .lenient(lenient)
                .maxExpansions(maxExpansions)
                .minimumShouldMatch(minimumShouldMatch)
                .operator(operator)
                .prefixLength(prefixLength)
                .slop(slop)
                .tieBreaker(tieBreaker)
                .zeroTermsQuery(zeroTermsQuery)
                .boost(boost)
                .queryName(queryName);
    }

    private void parseFieldAndBoost(XContentParser parser, Map<String, Float> fieldsBoosts) throws IOException {
        String fField = null;
        Float fBoost = AbstractQueryBuilder.DEFAULT_BOOST;
        char[] fieldText = parser.textCharacters();
        int end = parser.textOffset() + parser.textLength();
        for (int i = parser.textOffset(); i < end; i++) {
            if (fieldText[i] == '^') {
                int relativeLocation = i - parser.textOffset();
                fField = new String(fieldText, parser.textOffset(), relativeLocation);
                fBoost = Float.parseFloat(new String(fieldText, i + 1, parser.textLength() - relativeLocation - 1));
                break;
            }
        }
        if (fField == null) {
            fField = parser.text();
        }
        fieldsBoosts.put(fField, fBoost);
    }

    @Override
    public MultiMatchQueryBuilder getBuilderPrototype() {
        return MultiMatchQueryBuilder.PROTOTYPE;
    }
}
