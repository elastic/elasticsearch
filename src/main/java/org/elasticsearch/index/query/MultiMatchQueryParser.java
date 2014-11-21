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

import com.google.common.collect.Maps;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.search.MultiMatchQuery;

import java.io.IOException;
import java.util.Map;

/**
 * Same as {@link MatchQueryParser} but has support for multiple fields.
 */
public class MultiMatchQueryParser implements QueryParser {

    public static final String NAME = "multi_match";

    @Inject
    public MultiMatchQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{
                NAME, "multiMatch"
        };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        Object value = null;
        float boost = 1.0f;
        Float tieBreaker = null;
        MultiMatchQueryBuilder.Type type = null;
        MultiMatchQuery multiMatchQuery = new MultiMatchQuery(parseContext);
        String minimumShouldMatch = null;
        Map<String, Float> fieldNameWithBoosts = Maps.newHashMap();
        String queryName = null;
        XContentParser.Token token;
        String currentFieldName = null;
        Boolean useDisMax = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("fields".equals(currentFieldName)) {
                if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        extractFieldAndBoost(parseContext, parser, fieldNameWithBoosts);
                    }
                } else if (token.isValue()) {
                    extractFieldAndBoost(parseContext, parser, fieldNameWithBoosts);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[" + NAME + "] query does not support [" + currentFieldName
                            + "]");
                }
            } else if (token.isValue()) {
                if ("query".equals(currentFieldName)) {
                    value = parser.objectText();
                } else if ("type".equals(currentFieldName)) {
                    type = MultiMatchQueryBuilder.Type.parse(parser.text(), parseContext.parseFlags());
                } else if ("analyzer".equals(currentFieldName)) {
                    String analyzer = parser.text();
                    if (parseContext.analysisService().analyzer(analyzer) == null) {
                        throw new QueryParsingException(parseContext.index(), "["+ NAME +"] analyzer [" + parser.text() + "] not found");
                    }
                    multiMatchQuery.setAnalyzer(analyzer);
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("slop".equals(currentFieldName) || "phrase_slop".equals(currentFieldName) || "phraseSlop".equals(currentFieldName)) {
                    multiMatchQuery.setPhraseSlop(parser.intValue());
                } else if (Fuzziness.FIELD.match(currentFieldName, parseContext.parseFlags())) {
                    multiMatchQuery.setFuzziness(Fuzziness.parse(parser));
                } else if ("prefix_length".equals(currentFieldName) || "prefixLength".equals(currentFieldName)) {
                    multiMatchQuery.setFuzzyPrefixLength(parser.intValue());
                } else if ("max_expansions".equals(currentFieldName) || "maxExpansions".equals(currentFieldName)) {
                    multiMatchQuery.setMaxExpansions(parser.intValue());
                } else if ("operator".equals(currentFieldName)) {
                    String op = parser.text();
                    if ("or".equalsIgnoreCase(op)) {
                        multiMatchQuery.setOccur(BooleanClause.Occur.SHOULD);
                    } else if ("and".equalsIgnoreCase(op)) {
                        multiMatchQuery.setOccur(BooleanClause.Occur.MUST);
                    } else {
                        throw new QueryParsingException(parseContext.index(), "text query requires operator to be either 'and' or 'or', not [" + op + "]");
                    }
                } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                    minimumShouldMatch = parser.textOrNull();
                } else if ("rewrite".equals(currentFieldName)) {
                    multiMatchQuery.setRewriteMethod(QueryParsers.parseRewriteMethod(parser.textOrNull(), null));
                } else if ("fuzzy_rewrite".equals(currentFieldName) || "fuzzyRewrite".equals(currentFieldName)) {
                    multiMatchQuery.setFuzzyRewriteMethod(QueryParsers.parseRewriteMethod(parser.textOrNull(), null));
                } else if ("use_dis_max".equals(currentFieldName) || "useDisMax".equals(currentFieldName)) {
                    useDisMax = parser.booleanValue();
                } else if ("tie_breaker".equals(currentFieldName) || "tieBreaker".equals(currentFieldName)) {
                    multiMatchQuery.setTieBreaker(tieBreaker = parser.floatValue());
                }  else if ("cutoff_frequency".equals(currentFieldName)) {
                    multiMatchQuery.setCommonTermsCutoff(parser.floatValue());
                } else if ("lenient".equals(currentFieldName)) {
                    multiMatchQuery.setLenient(parser.booleanValue());
                } else if ("zero_terms_query".equals(currentFieldName)) {
                    String zeroTermsDocs = parser.text();
                    if ("none".equalsIgnoreCase(zeroTermsDocs)) {
                        multiMatchQuery.setZeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE);
                    } else if ("all".equalsIgnoreCase(zeroTermsDocs)) {
                        multiMatchQuery.setZeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);
                    } else {
                        throw new QueryParsingException(parseContext.index(), "Unsupported zero_terms_docs value [" + zeroTermsDocs + "]");
                    }
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[match] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No text specified for multi_match query");
        }

        if (fieldNameWithBoosts.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "No fields specified for multi_match query");
        }
        if (type == null) {
            type = MultiMatchQueryBuilder.Type.BEST_FIELDS;
        }
        if (useDisMax != null) { // backwards foobar
            boolean typeUsesDismax = type.tieBreaker() != 1.0f;
            if (typeUsesDismax != useDisMax) {
                if (useDisMax && tieBreaker == null) {
                    multiMatchQuery.setTieBreaker(0.0f);
                } else {
                    multiMatchQuery.setTieBreaker(1.0f);
                }
            }
        }
        Query query = multiMatchQuery.parse(type, fieldNameWithBoosts, value, minimumShouldMatch);
        if (query == null) {
            return null;
        }

        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    private void extractFieldAndBoost(QueryParseContext parseContext, XContentParser parser, Map<String, Float> fieldNameWithBoosts) throws IOException {
        String fField = null;
        Float fBoost = null;
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

        if (Regex.isSimpleMatchPattern(fField)) {
            for (String field : parseContext.mapperService().simpleMatchToIndexNames(fField)) {
                fieldNameWithBoosts.put(field, fBoost);
            }
        } else {
            fieldNameWithBoosts.put(fField, fBoost);
        }
    }
}
