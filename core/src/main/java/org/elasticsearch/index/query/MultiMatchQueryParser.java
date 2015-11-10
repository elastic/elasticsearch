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
            } else if ("fields".equals(currentFieldName)) {
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
                if ("query".equals(currentFieldName)) {
                    value = parser.objectText();
                } else if ("type".equals(currentFieldName)) {
                    type = MultiMatchQueryBuilder.Type.parse(parser.text(), parseContext.parseFieldMatcher());
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
                    maxExpansions = parser.intValue();
                } else if ("operator".equals(currentFieldName)) {
                    operator = Operator.fromString(parser.text());
                } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if ("fuzzy_rewrite".equals(currentFieldName) || "fuzzyRewrite".equals(currentFieldName)) {
                    fuzzyRewrite = parser.textOrNull();
                } else if ("use_dis_max".equals(currentFieldName) || "useDisMax".equals(currentFieldName)) {
                    useDisMax = parser.booleanValue();
                } else if ("tie_breaker".equals(currentFieldName) || "tieBreaker".equals(currentFieldName)) {
                    tieBreaker = parser.floatValue();
                }  else if ("cutoff_frequency".equals(currentFieldName)) {
                    cutoffFrequency = parser.floatValue();
                } else if ("lenient".equals(currentFieldName)) {
                    lenient = parser.booleanValue();
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
