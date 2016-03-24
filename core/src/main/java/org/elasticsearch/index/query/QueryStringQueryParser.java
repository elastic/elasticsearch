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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Parser for query_string query
 */
public class QueryStringQueryParser implements QueryParser {

    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final ParseField DEFAULT_FIELD_FIELD = new ParseField("default_field");
    public static final ParseField DEFAULT_OPERATOR_FIELD = new ParseField("default_operator");
    public static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    public static final ParseField QUOTE_ANALYZER_FIELD = new ParseField("quote_analyzer");
    public static final ParseField ALLOW_LEADING_WILDCARD_FIELD = new ParseField("allow_leading_wildcard");
    public static final ParseField AUTO_GENERATED_PHRASE_QUERIES_FIELD = new ParseField("auto_generated_phrase_queries");
    public static final ParseField MAX_DETERMINED_STATES_FIELD = new ParseField("max_determined_states");
    public static final ParseField LOWERCASE_EXPANDED_TERMS_FIELD = new ParseField("lowercase_expanded_terms");
    public static final ParseField ENABLE_POSITION_INCREMENTS_FIELD = new ParseField("enable_position_increment");
    public static final ParseField ESCAPE_FIELD = new ParseField("escape");
    public static final ParseField USE_DIS_MAX_FIELD = new ParseField("use_dis_max");
    public static final ParseField FUZZY_PREFIX_LENGTH_FIELD = new ParseField("fuzzy_prefix_length");
    public static final ParseField FUZZY_MAX_EXPANSIONS_FIELD = new ParseField("fuzzy_max_expansions");
    public static final ParseField FUZZY_REWRITE_FIELD = new ParseField("fuzzy_rewrite");
    public static final ParseField PHRASE_SLOP_FIELD = new ParseField("phrase_slop");
    public static final ParseField TIE_BREAKER_FIELD = new ParseField("tie_breaker");
    public static final ParseField ANALYZE_WILDCARD_FIELD = new ParseField("analyze_wildcard");
    public static final ParseField REWRITE_FIELD = new ParseField("rewrite");
    public static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    public static final ParseField QUOTE_FIELD_SUFFIX_FIELD = new ParseField("quote_field_suffix");
    public static final ParseField LENIENT_FIELD = new ParseField("lenient");
    public static final ParseField LOCALE_FIELD = new ParseField("locale");
    public static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");
  
    @Override
    public String[] names() {
        return new String[]{QueryStringQueryBuilder.NAME, Strings.toCamelCase(QueryStringQueryBuilder.NAME)};
    }

    @Override
    public QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        String currentFieldName = null;
        XContentParser.Token token;
        String queryString = null;
        String defaultField = null;
        String analyzer = null;
        String quoteAnalyzer = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        boolean autoGeneratePhraseQueries = QueryStringQueryBuilder.DEFAULT_AUTO_GENERATE_PHRASE_QUERIES;
        int maxDeterminizedStates = QueryStringQueryBuilder.DEFAULT_MAX_DETERMINED_STATES;
        boolean lowercaseExpandedTerms = QueryStringQueryBuilder.DEFAULT_LOWERCASE_EXPANDED_TERMS;
        boolean enablePositionIncrements = QueryStringQueryBuilder.DEFAULT_ENABLE_POSITION_INCREMENTS;
        boolean escape = QueryStringQueryBuilder.DEFAULT_ESCAPE;
        boolean useDisMax = QueryStringQueryBuilder.DEFAULT_USE_DIS_MAX;
        int fuzzyPrefixLength = QueryStringQueryBuilder.DEFAULT_FUZZY_PREFIX_LENGTH;
        int fuzzyMaxExpansions = QueryStringQueryBuilder.DEFAULT_FUZZY_MAX_EXPANSIONS;
        int phraseSlop = QueryStringQueryBuilder.DEFAULT_PHRASE_SLOP;
        float tieBreaker = QueryStringQueryBuilder.DEFAULT_TIE_BREAKER;
        Boolean analyzeWildcard = null;
        Boolean allowLeadingWildcard = null;
        String minimumShouldMatch = null;
        String quoteFieldSuffix = null;
        Boolean lenient = null;
        Operator defaultOperator = QueryStringQueryBuilder.DEFAULT_OPERATOR;
        String timeZone = null;
        Locale locale = QueryStringQueryBuilder.DEFAULT_LOCALE;
        Fuzziness fuzziness = QueryStringQueryBuilder.DEFAULT_FUZZINESS;
        String fuzzyRewrite = null;
        String rewrite = null;
        Map<String, Float> fieldsAndWeights = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String fField = null;
                        float fBoost = AbstractQueryBuilder.DEFAULT_BOOST;
                        char[] text = parser.textCharacters();
                        int end = parser.textOffset() + parser.textLength();
                        for (int i = parser.textOffset(); i < end; i++) {
                            if (text[i] == '^') {
                                int relativeLocation = i - parser.textOffset();
                                fField = new String(text, parser.textOffset(), relativeLocation);
                                fBoost = Float.parseFloat(new String(text, i + 1, parser.textLength() - relativeLocation - 1));
                                break;
                            }
                        }
                        if (fField == null) {
                            fField = parser.text();
                        }
                        fieldsAndWeights.put(fField, fBoost);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    queryString = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, DEFAULT_FIELD_FIELD)) {
                    defaultField = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, DEFAULT_OPERATOR_FIELD)) {
                    defaultOperator = Operator.fromString(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, ANALYZER_FIELD)) {
                    analyzer = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, QUOTE_ANALYZER_FIELD)) {
                    quoteAnalyzer = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, ALLOW_LEADING_WILDCARD_FIELD)) {
                    allowLeadingWildcard = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AUTO_GENERATED_PHRASE_QUERIES_FIELD)) {
                    autoGeneratePhraseQueries = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MAX_DETERMINED_STATES_FIELD)) {
                    maxDeterminizedStates = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, LOWERCASE_EXPANDED_TERMS_FIELD)) {
                    lowercaseExpandedTerms = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, ENABLE_POSITION_INCREMENTS_FIELD)) {
                    enablePositionIncrements = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, ESCAPE_FIELD)) {
                    escape = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, USE_DIS_MAX_FIELD)) {
                    useDisMax = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, FUZZY_PREFIX_LENGTH_FIELD)) {
                    fuzzyPrefixLength = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, FUZZY_MAX_EXPANSIONS_FIELD)) {
                    fuzzyMaxExpansions = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, FUZZY_REWRITE_FIELD)) {
                    fuzzyRewrite = parser.textOrNull();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, PHRASE_SLOP_FIELD)) {
                    phraseSlop = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Fuzziness.FIELD)) {
                    fuzziness = Fuzziness.parse(parser);
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, TIE_BREAKER_FIELD)) {
                    tieBreaker = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, ANALYZE_WILDCARD_FIELD)) {
                    analyzeWildcard = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, REWRITE_FIELD)) {
                    rewrite = parser.textOrNull();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MINIMUM_SHOULD_MATCH_FIELD)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, QUOTE_FIELD_SUFFIX_FIELD)) {
                    quoteFieldSuffix = parser.textOrNull();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, LENIENT_FIELD)) {
                    lenient = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, LOCALE_FIELD)) {
                    String localeStr = parser.text();
                    locale = Locale.forLanguageTag(localeStr);
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, TIME_ZONE_FIELD)) {
                    try {
                        timeZone = parser.text();
                    } catch (IllegalArgumentException e) {
                        throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME + "] time_zone [" + parser.text() + "] is unknown");
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }
        if (queryString == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME + "] must be provided with a [query]");
        }

        QueryStringQueryBuilder queryStringQuery = new QueryStringQueryBuilder(queryString);
        queryStringQuery.fields(fieldsAndWeights);
        queryStringQuery.defaultField(defaultField);
        queryStringQuery.defaultOperator(defaultOperator);
        queryStringQuery.analyzer(analyzer);
        queryStringQuery.quoteAnalyzer(quoteAnalyzer);
        queryStringQuery.allowLeadingWildcard(allowLeadingWildcard);
        queryStringQuery.autoGeneratePhraseQueries(autoGeneratePhraseQueries);
        queryStringQuery.maxDeterminizedStates(maxDeterminizedStates);
        queryStringQuery.lowercaseExpandedTerms(lowercaseExpandedTerms);
        queryStringQuery.enablePositionIncrements(enablePositionIncrements);
        queryStringQuery.escape(escape);
        queryStringQuery.useDisMax(useDisMax);
        queryStringQuery.fuzzyPrefixLength(fuzzyPrefixLength);
        queryStringQuery.fuzzyMaxExpansions(fuzzyMaxExpansions);
        queryStringQuery.fuzzyRewrite(fuzzyRewrite);
        queryStringQuery.phraseSlop(phraseSlop);
        queryStringQuery.fuzziness(fuzziness);
        queryStringQuery.tieBreaker(tieBreaker);
        queryStringQuery.analyzeWildcard(analyzeWildcard);
        queryStringQuery.rewrite(rewrite);
        queryStringQuery.minimumShouldMatch(minimumShouldMatch);
        queryStringQuery.quoteFieldSuffix(quoteFieldSuffix);
        queryStringQuery.lenient(lenient);
        queryStringQuery.timeZone(timeZone);
        queryStringQuery.locale(locale);
        queryStringQuery.boost(boost);
        queryStringQuery.queryName(queryName);
        return queryStringQuery;
    }

    @Override
    public QueryStringQueryBuilder getBuilderPrototype() {
        return QueryStringQueryBuilder.PROTOTYPE;
    }
}
