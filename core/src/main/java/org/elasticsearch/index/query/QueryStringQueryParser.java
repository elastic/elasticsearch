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
                if ("fields".equals(currentFieldName)) {
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
                if ("query".equals(currentFieldName)) {
                    queryString = parser.text();
                } else if ("default_field".equals(currentFieldName) || "defaultField".equals(currentFieldName)) {
                    defaultField = parser.text();
                } else if ("default_operator".equals(currentFieldName) || "defaultOperator".equals(currentFieldName)) {
                    defaultOperator = Operator.fromString(parser.text());
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = parser.text();
                } else if ("quote_analyzer".equals(currentFieldName) || "quoteAnalyzer".equals(currentFieldName)) {
                    quoteAnalyzer = parser.text();
                } else if ("allow_leading_wildcard".equals(currentFieldName) || "allowLeadingWildcard".equals(currentFieldName)) {
                    allowLeadingWildcard = parser.booleanValue();
                } else if ("auto_generate_phrase_queries".equals(currentFieldName) || "autoGeneratePhraseQueries".equals(currentFieldName)) {
                    autoGeneratePhraseQueries = parser.booleanValue();
                } else if ("max_determinized_states".equals(currentFieldName) || "maxDeterminizedStates".equals(currentFieldName)) {
                    maxDeterminizedStates = parser.intValue();
                } else if ("lowercase_expanded_terms".equals(currentFieldName) || "lowercaseExpandedTerms".equals(currentFieldName)) {
                    lowercaseExpandedTerms = parser.booleanValue();
                } else if ("enable_position_increments".equals(currentFieldName) || "enablePositionIncrements".equals(currentFieldName)) {
                    enablePositionIncrements = parser.booleanValue();
                } else if ("escape".equals(currentFieldName)) {
                    escape = parser.booleanValue();
                } else if ("use_dis_max".equals(currentFieldName) || "useDisMax".equals(currentFieldName)) {
                    useDisMax = parser.booleanValue();
                } else if ("fuzzy_prefix_length".equals(currentFieldName) || "fuzzyPrefixLength".equals(currentFieldName)) {
                    fuzzyPrefixLength = parser.intValue();
                } else if ("fuzzy_max_expansions".equals(currentFieldName) || "fuzzyMaxExpansions".equals(currentFieldName)) {
                    fuzzyMaxExpansions = parser.intValue();
                } else if ("fuzzy_rewrite".equals(currentFieldName) || "fuzzyRewrite".equals(currentFieldName)) {
                    fuzzyRewrite = parser.textOrNull();
                } else if ("phrase_slop".equals(currentFieldName) || "phraseSlop".equals(currentFieldName)) {
                    phraseSlop = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Fuzziness.FIELD)) {
                    fuzziness = Fuzziness.parse(parser);
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("tie_breaker".equals(currentFieldName) || "tieBreaker".equals(currentFieldName)) {
                    tieBreaker = parser.floatValue();
                } else if ("analyze_wildcard".equals(currentFieldName) || "analyzeWildcard".equals(currentFieldName)) {
                    analyzeWildcard = parser.booleanValue();
                } else if ("rewrite".equals(currentFieldName)) {
                    rewrite = parser.textOrNull();
                } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if ("quote_field_suffix".equals(currentFieldName) || "quoteFieldSuffix".equals(currentFieldName)) {
                    quoteFieldSuffix = parser.textOrNull();
                } else if ("lenient".equalsIgnoreCase(currentFieldName)) {
                    lenient = parser.booleanValue();
                } else if ("locale".equals(currentFieldName)) {
                    String localeStr = parser.text();
                    locale = Locale.forLanguageTag(localeStr);
                } else if ("time_zone".equals(currentFieldName) || "timeZone".equals(currentFieldName)) {
                    try {
                        timeZone = parser.text();
                    } catch (IllegalArgumentException e) {
                        throw new ParsingException(parser.getTokenLocation(), "[" + QueryStringQueryBuilder.NAME + "] time_zone [" + parser.text() + "] is unknown");
                    }
                } else if ("_name".equals(currentFieldName)) {
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
