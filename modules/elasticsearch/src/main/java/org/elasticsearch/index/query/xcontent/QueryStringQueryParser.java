/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.queryParser.MapperQueryParser;
import org.apache.lucene.queryParser.MultiFieldQueryParserSettings;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.impl.Constants;
import org.elasticsearch.common.trove.map.hash.TObjectFloatHashMap;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.query.parser.QueryParserCache;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;

import static org.elasticsearch.common.lucene.search.Queries.*;

/**
 * @author kimchy (shay.banon)
 */
public class QueryStringQueryParser extends AbstractIndexComponent implements XContentQueryParser {

    public static final String NAME = "query_string";

    private final AnalysisService analysisService;

    private final QueryParserCache queryParserCache;

    @Inject public QueryStringQueryParser(Index index, @IndexSettings Settings settings, AnalysisService analysisService, QueryParserCache queryParserCache) {
        super(index, settings);
        this.analysisService = analysisService;
        this.queryParserCache = queryParserCache;
    }

    @Override public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        MultiFieldQueryParserSettings qpSettings = new MultiFieldQueryParserSettings();
        qpSettings.defaultField(AllFieldMapper.NAME);

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
                            qpSettings.fields(Lists.<String>newArrayList());
                        }

                        if (Regex.isSimpleMatchPattern(fField)) {
                            for (String field : parseContext.mapperService().simpleMatchToIndexNames(fField)) {
                                qpSettings.fields().add(field);
                                if (fBoost != -1) {
                                    if (qpSettings.boosts() == null) {
                                        qpSettings.boosts(new TObjectFloatHashMap<String>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, 1.0f));
                                    }
                                    qpSettings.boosts().put(field, fBoost);
                                }
                            }
                        } else {
                            qpSettings.fields().add(fField);
                            if (fBoost != -1) {
                                if (qpSettings.boosts() == null) {
                                    qpSettings.boosts(new TObjectFloatHashMap<String>(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, 1.0f));
                                }
                                qpSettings.boosts().put(fField, fBoost);
                            }
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("query".equals(currentFieldName)) {
                    qpSettings.queryString(parser.text());
                } else if ("default_field".equals(currentFieldName) || "defaultField".equals(currentFieldName)) {
                    qpSettings.defaultField(parseContext.indexName(parser.text()));
                } else if ("default_operator".equals(currentFieldName) || "defaultOperator".equals(currentFieldName)) {
                    String op = parser.text();
                    if ("or".equalsIgnoreCase(op)) {
                        qpSettings.defaultOperator(QueryParser.Operator.OR);
                    } else if ("and".equalsIgnoreCase(op)) {
                        qpSettings.defaultOperator(QueryParser.Operator.AND);
                    } else {
                        throw new QueryParsingException(index, "Query default operator [" + op + "] is not allowed");
                    }
                } else if ("analyzer".equals(currentFieldName)) {
                    qpSettings.analyzer(analysisService.analyzer(parser.text()));
                } else if ("allow_leading_wildcard".equals(currentFieldName) || "allowLeadingWildcard".equals(currentFieldName)) {
                    qpSettings.allowLeadingWildcard(parser.booleanValue());
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
                } else if ("phrase_slop".equals(currentFieldName) || "phraseSlop".equals(currentFieldName)) {
                    qpSettings.phraseSlop(parser.intValue());
                } else if ("fuzzy_min_sim".equals(currentFieldName) || "fuzzyMinSim".equals(currentFieldName)) {
                    qpSettings.fuzzyMinSim(parser.floatValue());
                } else if ("boost".equals(currentFieldName)) {
                    qpSettings.boost(parser.floatValue());
                } else if ("tie_breaker".equals(currentFieldName) || "tieBreaker".equals(currentFieldName)) {
                    qpSettings.tieBreaker(parser.floatValue());
                } else if ("analyze_wildcard".equals(currentFieldName) || "analyzeWildcard".equals(currentFieldName)) {
                    qpSettings.analyzeWildcard(parser.booleanValue());
                }
            }
        }
        if (qpSettings.queryString() == null) {
            throw new QueryParsingException(index, "query_string must be provided with a [query]");
        }
        if (qpSettings.analyzer() == null) {
            qpSettings.analyzer(parseContext.mapperService().searchAnalyzer());
        }

        if (qpSettings.escape()) {
            qpSettings.queryString(QueryParser.escape(qpSettings.queryString()));
        }

        Query query = queryParserCache.get(qpSettings);
        if (query != null) {
            return query;
        }

        MapperQueryParser queryParser;
        if (qpSettings.fields() != null) {
            if (qpSettings.fields().size() == 1) {
                qpSettings.defaultField(qpSettings.fields().get(0));
                queryParser = parseContext.singleQueryParser(qpSettings);
            } else {
                qpSettings.defaultField(null); // reset defaultField when using multi query parser
                queryParser = parseContext.multiQueryParser(qpSettings);
            }
        } else {
            queryParser = parseContext.singleQueryParser(qpSettings);
        }


        try {
            query = queryParser.parse(qpSettings.queryString());
            query.setBoost(qpSettings.boost());
            query = optimizeQuery(fixNegativeQueryIfNeeded(query));
            queryParserCache.put(qpSettings, query);
            return query;
        } catch (ParseException e) {
            throw new QueryParsingException(index, "Failed to parse query [" + qpSettings.queryString() + "]", e);
        }
    }
}
