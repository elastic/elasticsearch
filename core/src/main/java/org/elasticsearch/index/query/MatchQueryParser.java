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
import org.elasticsearch.common.ParseField;
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

    public static final ParseField MATCH_PHRASE_FIELD = new ParseField("match_phrase", "text_phrase");
    public static final ParseField MATCH_PHRASE_PREFIX_FIELD = new ParseField("match_phrase_prefix", "text_phrase_prefix");
    public static final ParseField SLOP_FIELD = new ParseField("slop", "phrase_slop");
    public static final ParseField ZERO_TERMS_QUERY_FIELD = new ParseField("zero_terms_query");
    public static final ParseField CUTOFF_FREQUENCY_FIELD = new ParseField("cutoff_frequency");
    public static final ParseField LENIENT_FIELD = new ParseField("lenient");
    public static final ParseField FUZZY_TRANSPOSITIONS_FIELD = new ParseField("fuzzy_transpositions");
    public static final ParseField FUZZY_REWRITE_FIELD = new ParseField("fuzzy_rewrite");
    public static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    public static final ParseField OPERATOR_FIELD = new ParseField("operator");
    public static final ParseField MAX_EXPANSIONS_FIELD = new ParseField("max_expansions");
    public static final ParseField PREFIX_LENGTH_FIELD = new ParseField("prefix_length");
    public static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField QUERY_FIELD = new ParseField("query");

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
        if (parseContext.parseFieldMatcher().match(parser.currentName(), MATCH_PHRASE_FIELD)) {
            type = MatchQuery.Type.PHRASE;
        } else if (parseContext.parseFieldMatcher().match(parser.currentName(), MATCH_PHRASE_PREFIX_FIELD)) {
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
                    if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                        value = parser.objectText();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, TYPE_FIELD)) {
                        String tStr = parser.text();
                        if ("boolean".equals(tStr)) {
                            type = MatchQuery.Type.BOOLEAN;
                        } else if ("phrase".equals(tStr)) {
                            type = MatchQuery.Type.PHRASE;
                        } else if ("phrase_prefix".equals(tStr) || ("phrasePrefix".equals(tStr))) {
                            type = MatchQuery.Type.PHRASE_PREFIX;
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[" + MatchQueryBuilder.NAME + "] query does not support type " + tStr);
                        }
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
                        maxExpansion = parser.intValue();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, OPERATOR_FIELD)) {
                        operator = Operator.fromString(parser.text());
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, MINIMUM_SHOULD_MATCH_FIELD)) {
                        minimumShouldMatch = parser.textOrNull();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, FUZZY_REWRITE_FIELD)) {
                        fuzzyRewrite = parser.textOrNull();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, FUZZY_TRANSPOSITIONS_FIELD)) {
                        fuzzyTranspositions = parser.booleanValue();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, LENIENT_FIELD)) {
                        lenient = parser.booleanValue();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, CUTOFF_FREQUENCY_FIELD)) {
                        cutOffFrequency = parser.floatValue();
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
