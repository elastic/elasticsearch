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

import org.apache.lucene.search.FuzzyQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.search.MatchQuery.ZeroTermsQuery;

import java.io.IOException;

/**
 *
 */
public class MatchQueryParser implements QueryParser<MatchQueryBuilder> {

    @Override
    public String[] names() {
        return new String[]{
                MatchQueryBuilder.NAME, "match_phrase", "matchPhrase", "match_phrase_prefix", "matchPhrasePrefix", "matchFuzzy", "match_fuzzy", "fuzzy_match"
        };
    }

    @Override
    public MatchQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
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
            throw new ParsingException(parser.getTokenLocation(), "[" + MatchQueryBuilder.NAME + "] query malformed, no field");
        }
        String fieldName = parser.currentName();

        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String minimumShouldMatch = null;
        String analyzer = null;
        Operator operator = MatchQueryBuilder.DEFAULT_OPERATOR;
        int slop = MatchQuery.DEFAULT_PHRASE_SLOP;
        Fuzziness fuzziness = null;
        int prefixLength = FuzzyQuery.defaultPrefixLength;
        int maxExpansion = FuzzyQuery.defaultMaxExpansions;
        boolean fuzzyTranspositions = FuzzyQuery.defaultTranspositions;
        String fuzzyRewrite = null;
        boolean lenient = MatchQuery.DEFAULT_LENIENCY;
        Float cutOffFrequency = null;
        ZeroTermsQuery zeroTermsQuery = MatchQuery.DEFAULT_ZERO_TERMS_QUERY;
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
                            throw new ParsingException(parser.getTokenLocation(), "[" + MatchQueryBuilder.NAME + "] query does not support type " + tStr);
                        }
                    } else if ("analyzer".equals(currentFieldName)) {
                        analyzer = parser.text();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("slop".equals(currentFieldName) || "phrase_slop".equals(currentFieldName) || "phraseSlop".equals(currentFieldName)) {
                        slop = parser.intValue();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, Fuzziness.FIELD)) {
                        fuzziness = Fuzziness.parse(parser);
                    } else if ("prefix_length".equals(currentFieldName) || "prefixLength".equals(currentFieldName)) {
                        prefixLength = parser.intValue();
                    } else if ("max_expansions".equals(currentFieldName) || "maxExpansions".equals(currentFieldName)) {
                        maxExpansion = parser.intValue();
                    } else if ("operator".equals(currentFieldName)) {
                        operator = Operator.fromString(parser.text());
                    } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                        minimumShouldMatch = parser.textOrNull();
                    } else if ("fuzzy_rewrite".equals(currentFieldName) || "fuzzyRewrite".equals(currentFieldName)) {
                        fuzzyRewrite = parser.textOrNull();
                    } else if ("fuzzy_transpositions".equals(currentFieldName)) {
                        fuzzyTranspositions = parser.booleanValue();
                    } else if ("lenient".equals(currentFieldName)) {
                        lenient = parser.booleanValue();
                    } else if ("cutoff_frequency".equals(currentFieldName)) {
                        cutOffFrequency = parser.floatValue();
                    } else if ("zero_terms_query".equals(currentFieldName)) {
                        String zeroTermsDocs = parser.text();
                        if ("none".equalsIgnoreCase(zeroTermsDocs)) {
                            zeroTermsQuery = MatchQuery.ZeroTermsQuery.NONE;
                        } else if ("all".equalsIgnoreCase(zeroTermsDocs)) {
                            zeroTermsQuery = MatchQuery.ZeroTermsQuery.ALL;
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Unsupported zero_terms_docs value [" + zeroTermsDocs + "]");
                        }
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[" + MatchQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + MatchQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
                }
            }
            parser.nextToken();
        } else {
            value = parser.objectText();
            // move to the next token
            token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(),
                        "[match] query parsed in simplified form, with direct field name, but included more options than just the field name, possibly use its 'options' form, with 'query' element?");
            }
        }

        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "No text specified for text query");
        }

        MatchQueryBuilder matchQuery = new MatchQueryBuilder(fieldName, value);
        matchQuery.operator(operator);
        matchQuery.type(type);
        matchQuery.analyzer(analyzer);
        matchQuery.slop(slop);
        matchQuery.minimumShouldMatch(minimumShouldMatch);
        if (fuzziness != null) {
            matchQuery.fuzziness(fuzziness);
        }
        matchQuery.fuzzyRewrite(fuzzyRewrite);
        matchQuery.prefixLength(prefixLength);
        matchQuery.fuzzyTranspositions(fuzzyTranspositions);
        matchQuery.maxExpansions(maxExpansion);
        matchQuery.lenient(lenient);
        if (cutOffFrequency != null) {
            matchQuery.cutoffFrequency(cutOffFrequency);
        }
        matchQuery.zeroTermsQuery(zeroTermsQuery);
        matchQuery.queryName(queryName);
        matchQuery.boost(boost);
        return matchQuery;
    }

    @Override
    public MatchQueryBuilder getBuilderPrototype() {
        return MatchQueryBuilder.PROTOTYPE;
    }
}
