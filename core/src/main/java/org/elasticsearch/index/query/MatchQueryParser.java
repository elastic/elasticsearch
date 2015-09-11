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

import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.search.MatchQuery;

import java.io.IOException;

/**
 *
 */
public class MatchQueryParser implements QueryParser {

    public static final String NAME = "match";

    @Inject
    public MatchQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{
                NAME, "match_phrase", "matchPhrase", "match_phrase_prefix", "matchPhrasePrefix", "matchFuzzy", "match_fuzzy", "fuzzy_match"
        };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        MatchQuery.Type type = MatchQuery.Type.BOOLEAN;
        if ("match_phrase".equals(parser.currentName()) || "matchPhrase".equals(parser.currentName()) ||
                "text_phrase".equals(parser.currentName()) || "textPhrase".equals(parser.currentName())) {
            type = MatchQuery.Type.PHRASE;
        } else if ("match_phrase_prefix".equals(parser.currentName()) || "matchPhrasePrefix".equals(parser.currentName()) ||
                "text_phrase_prefix".equals(parser.currentName()) || "textPhrasePrefix".equals(parser.currentName())) {
            type = MatchQuery.Type.PHRASE_PREFIX;
        }

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext, "[match] query malformed, no field");
        }
        String fieldName = parser.currentName();

        Object value = null;
        float boost = 1.0f;
        MatchQuery matchQuery = new MatchQuery(parseContext);
        String minimumShouldMatch = null;
        String queryName = null;

        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("query".equals(currentFieldName)) {
                        value = parser.objectText();
                    } else if ("type".equals(currentFieldName)) {
                        String tStr = parser.text();
                        if ("boolean".equals(tStr)) {
                            type = MatchQuery.Type.BOOLEAN;
                        } else if ("phrase".equals(tStr)) {
                            type = MatchQuery.Type.PHRASE;
                        } else if ("phrase_prefix".equals(tStr) || "phrasePrefix".equals(currentFieldName)) {
                            type = MatchQuery.Type.PHRASE_PREFIX;
                        } else {
                            throw new QueryParsingException(parseContext, "[match] query does not support type " + tStr);
                        }
                    } else if ("analyzer".equals(currentFieldName)) {
                        String analyzer = parser.text();
                        if (parseContext.analysisService().analyzer(analyzer) == null) {
                            throw new QueryParsingException(parseContext, "[match] analyzer [" + parser.text() + "] not found");
                        }
                        matchQuery.setAnalyzer(analyzer);
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("slop".equals(currentFieldName) || "phrase_slop".equals(currentFieldName) || "phraseSlop".equals(currentFieldName)) {
                        matchQuery.setPhraseSlop(parser.intValue());
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, Fuzziness.FIELD)) {
                        matchQuery.setFuzziness(Fuzziness.parse(parser));
                    } else if ("prefix_length".equals(currentFieldName) || "prefixLength".equals(currentFieldName)) {
                        matchQuery.setFuzzyPrefixLength(parser.intValue());
                    } else if ("max_expansions".equals(currentFieldName) || "maxExpansions".equals(currentFieldName)) {
                        matchQuery.setMaxExpansions(parser.intValue());
                    } else if ("operator".equals(currentFieldName)) {
                        String op = parser.text();
                        if ("or".equalsIgnoreCase(op)) {
                            matchQuery.setOccur(BooleanClause.Occur.SHOULD);
                        } else if ("and".equalsIgnoreCase(op)) {
                            matchQuery.setOccur(BooleanClause.Occur.MUST);
                        } else {
                            throw new QueryParsingException(parseContext, "text query requires operator to be either 'and' or 'or', not ["
                                    + op + "]");
                        }
                    } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                        minimumShouldMatch = parser.textOrNull();
                    } else if ("fuzzy_rewrite".equals(currentFieldName) || "fuzzyRewrite".equals(currentFieldName)) {
                        matchQuery.setFuzzyRewriteMethod(QueryParsers.parseRewriteMethod(parseContext.parseFieldMatcher(), parser.textOrNull(), null));
                    } else if ("fuzzy_transpositions".equals(currentFieldName)) {
                        matchQuery.setTranspositions(parser.booleanValue());
                    } else if ("lenient".equals(currentFieldName)) {
                        matchQuery.setLenient(parser.booleanValue());
                    } else if ("cutoff_frequency".equals(currentFieldName)) {
                        matchQuery.setCommonTermsCutoff(parser.floatValue());
                    } else if ("zero_terms_query".equals(currentFieldName)) {
                        String zeroTermsDocs = parser.text();
                        if ("none".equalsIgnoreCase(zeroTermsDocs)) {
                            matchQuery.setZeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE);
                        } else if ("all".equalsIgnoreCase(zeroTermsDocs)) {
                            matchQuery.setZeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);
                        } else {
                            throw new QueryParsingException(parseContext, "Unsupported zero_terms_docs value [" + zeroTermsDocs + "]");
                        }
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new QueryParsingException(parseContext, "[match] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.objectText();
            // move to the next token
            token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new QueryParsingException(parseContext,
                        "[match] query parsed in simplified form, with direct field name, but included more options than just the field name, possibly use its 'options' form, with 'query' element?");
            }
        }

        if (value == null) {
            throw new QueryParsingException(parseContext, "No text specified for text query");
        }

        Query query = matchQuery.parse(type, fieldName, value);
        if (query == null) {
            return null;
        }

        if (query instanceof BooleanQuery) {
            query = Queries.applyMinimumShouldMatch((BooleanQuery) query, minimumShouldMatch);
        } else if (query instanceof ExtendedCommonTermsQuery) {
            ((ExtendedCommonTermsQuery)query).setLowFreqMinimumNumberShouldMatch(minimumShouldMatch);
        }
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
