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

import com.carrotsearch.hppc.ObjectFloatHashMap;
import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.support.QueryParsers;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;

import static org.elasticsearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;

/**
 *
 */
public class QueryStringQueryParser implements QueryParser {

    public static final String NAME = "query_string";
    private static final ParseField FUZZINESS = Fuzziness.FIELD.withDeprecation("fuzzy_min_sim");

    private final boolean defaultAnalyzeWildcard;
    private final boolean defaultAllowLeadingWildcard;

    @Inject
    public QueryStringQueryParser(Settings settings) {
        this.defaultAnalyzeWildcard = settings.getAsBoolean("indices.query.query_string.analyze_wildcard", QueryParserSettings.DEFAULT_ANALYZE_WILDCARD);
        this.defaultAllowLeadingWildcard = settings.getAsBoolean("indices.query.query_string.allowLeadingWildcard", QueryParserSettings.DEFAULT_ALLOW_LEADING_WILDCARD);
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String queryName = null;
        QueryParserSettings qpSettings = new QueryParserSettings();
        qpSettings.defaultField(parseContext.defaultField());
        qpSettings.lenient(parseContext.queryStringLenient());
        qpSettings.analyzeWildcard(defaultAnalyzeWildcard);
        qpSettings.allowLeadingWildcard(defaultAllowLeadingWildcard);
        qpSettings.locale(Locale.ROOT);

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("fields".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String fField = null;
                        float fBoost = -1;
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
                        if (qpSettings.fields() == null) {
                            qpSettings.fields(new ArrayList<String>());
                        }

                        if (Regex.isSimpleMatchPattern(fField)) {
                            for (String field : parseContext.mapperService().simpleMatchToIndexNames(fField)) {
                                qpSettings.fields().add(field);
                                if (fBoost != -1) {
                                    if (qpSettings.boosts() == null) {
                                        qpSettings.boosts(new ObjectFloatHashMap<String>());
                                    }
                                    qpSettings.boosts().put(field, fBoost);
                                }
                            }
                        } else {
                            qpSettings.fields().add(fField);
                            if (fBoost != -1) {
                                if (qpSettings.boosts() == null) {
                                    qpSettings.boosts(new ObjectFloatHashMap<String>());
                                }
                                qpSettings.boosts().put(fField, fBoost);
                            }
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext, "[query_string] query does not support [" + currentFieldName
                            + "]");
                }
            } else if (token.isValue()) {
                if ("query".equals(currentFieldName)) {
                    qpSettings.queryString(parser.text());
                } else if ("default_field".equals(currentFieldName) || "defaultField".equals(currentFieldName)) {
                    qpSettings.defaultField(parser.text());
                } else if ("default_operator".equals(currentFieldName) || "defaultOperator".equals(currentFieldName)) {
                    String op = parser.text();
                    if ("or".equalsIgnoreCase(op)) {
                        qpSettings.defaultOperator(org.apache.lucene.queryparser.classic.QueryParser.Operator.OR);
                    } else if ("and".equalsIgnoreCase(op)) {
                        qpSettings.defaultOperator(org.apache.lucene.queryparser.classic.QueryParser.Operator.AND);
                    } else {
                        throw new QueryParsingException(parseContext, "Query default operator [" + op + "] is not allowed");
                    }
                } else if ("analyzer".equals(currentFieldName)) {
                    NamedAnalyzer analyzer = parseContext.analysisService().analyzer(parser.text());
                    if (analyzer == null) {
                        throw new QueryParsingException(parseContext, "[query_string] analyzer [" + parser.text() + "] not found");
                    }
                    qpSettings.forcedAnalyzer(analyzer);
                } else if ("quote_analyzer".equals(currentFieldName) || "quoteAnalyzer".equals(currentFieldName)) {
                    NamedAnalyzer analyzer = parseContext.analysisService().analyzer(parser.text());
                    if (analyzer == null) {
                        throw new QueryParsingException(parseContext, "[query_string] quote_analyzer [" + parser.text()
                                + "] not found");
                    }
                    qpSettings.forcedQuoteAnalyzer(analyzer);
                } else if ("allow_leading_wildcard".equals(currentFieldName) || "allowLeadingWildcard".equals(currentFieldName)) {
                    qpSettings.allowLeadingWildcard(parser.booleanValue());
                } else if ("auto_generate_phrase_queries".equals(currentFieldName) || "autoGeneratePhraseQueries".equals(currentFieldName)) {
                    qpSettings.autoGeneratePhraseQueries(parser.booleanValue());
                } else if ("max_determinized_states".equals(currentFieldName) || "maxDeterminizedStates".equals(currentFieldName)) {
                    qpSettings.maxDeterminizedStates(parser.intValue());
                } else if ("lowercase_expanded_terms".equals(currentFieldName) || "lowercaseExpandedTerms".equals(currentFieldName)) {
                    qpSettings.lowercaseExpandedTerms(parser.booleanValue());
                } else if ("enable_position_increments".equals(currentFieldName) || "enablePositionIncrements".equals(currentFieldName)) {
                    qpSettings.enablePositionIncrements(parser.booleanValue());
                } else if ("escape".equals(currentFieldName)) {
                    qpSettings.escape(parser.booleanValue());
                } else if ("use_dis_max".equals(currentFieldName) || "useDisMax".equals(currentFieldName)) {
                    qpSettings.useDisMax(parser.booleanValue());
                } else if ("fuzzy_prefix_length".equals(currentFieldName) || "fuzzyPrefixLength".equals(currentFieldName)) {
                    qpSettings.fuzzyPrefixLength(parser.intValue());
                } else if ("fuzzy_max_expansions".equals(currentFieldName) || "fuzzyMaxExpansions".equals(currentFieldName)) {
                    qpSettings.fuzzyMaxExpansions(parser.intValue());
                } else if ("fuzzy_rewrite".equals(currentFieldName) || "fuzzyRewrite".equals(currentFieldName)) {
                    qpSettings.fuzzyRewriteMethod(QueryParsers.parseRewriteMethod(parseContext.parseFieldMatcher(), parser.textOrNull()));
                } else if ("phrase_slop".equals(currentFieldName) || "phraseSlop".equals(currentFieldName)) {
                    qpSettings.phraseSlop(parser.intValue());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, FUZZINESS)) {
                    qpSettings.setFuzziness(Fuzziness.parse(parser));
                } else if ("boost".equals(currentFieldName)) {
                    qpSettings.boost(parser.floatValue());
                } else if ("tie_breaker".equals(currentFieldName) || "tieBreaker".equals(currentFieldName)) {
                    qpSettings.tieBreaker(parser.floatValue());
                } else if ("analyze_wildcard".equals(currentFieldName) || "analyzeWildcard".equals(currentFieldName)) {
                    qpSettings.analyzeWildcard(parser.booleanValue());
                } else if ("rewrite".equals(currentFieldName)) {
                    qpSettings.rewriteMethod(QueryParsers.parseRewriteMethod(parseContext.parseFieldMatcher(), parser.textOrNull()));
                } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                    qpSettings.minimumShouldMatch(parser.textOrNull());
                } else if ("quote_field_suffix".equals(currentFieldName) || "quoteFieldSuffix".equals(currentFieldName)) {
                    qpSettings.quoteFieldSuffix(parser.textOrNull());
                } else if ("lenient".equalsIgnoreCase(currentFieldName)) {
                    qpSettings.lenient(parser.booleanValue());
                } else if ("locale".equals(currentFieldName)) {
                    String localeStr = parser.text();
                    qpSettings.locale(LocaleUtils.parse(localeStr));
                } else if ("time_zone".equals(currentFieldName)) {
                    try {
                        qpSettings.timeZone(DateTimeZone.forID(parser.text()));
                    } catch (IllegalArgumentException e) {
                        throw new QueryParsingException(parseContext,
                                "[query_string] time_zone [" + parser.text() + "] is unknown");
                    }
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "[query_string] query does not support [" + currentFieldName
 + "]");
                }
            }
        }
        if (qpSettings.queryString() == null) {
            throw new QueryParsingException(parseContext, "query_string must be provided with a [query]");
        }
        qpSettings.defaultAnalyzer(parseContext.mapperService().searchAnalyzer());
        qpSettings.defaultQuoteAnalyzer(parseContext.mapperService().searchQuoteAnalyzer());

        if (qpSettings.escape()) {
            qpSettings.queryString(org.apache.lucene.queryparser.classic.QueryParser.escape(qpSettings.queryString()));
        }

        MapperQueryParser queryParser = parseContext.queryParser(qpSettings);

        try {
            Query query = queryParser.parse(qpSettings.queryString());
            if (query == null) {
                return null;
            }
            if (qpSettings.boost() != QueryParserSettings.DEFAULT_BOOST) {
                query.setBoost(query.getBoost() * qpSettings.boost());
            }
            query = fixNegativeQueryIfNeeded(query);
            if (query instanceof BooleanQuery) {
                query = Queries.applyMinimumShouldMatch((BooleanQuery) query, qpSettings.minimumShouldMatch());
            }
            if (queryName != null) {
                parseContext.addNamedQuery(queryName, query);
            }
            return query;
        } catch (org.apache.lucene.queryparser.classic.ParseException e) {
            throw new QueryParsingException(parseContext, "Failed to parse query [" + qpSettings.queryString() + "]", e);
        }
    }
}
