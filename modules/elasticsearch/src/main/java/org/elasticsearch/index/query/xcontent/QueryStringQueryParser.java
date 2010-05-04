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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.support.MapperQueryParser;
import org.elasticsearch.index.query.support.MultiFieldMapperQueryParser;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.trove.ExtTObjectFloatHashMap;
import org.elasticsearch.util.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.util.lucene.search.Queries.*;

/**
 * @author kimchy (shay.banon)
 */
public class QueryStringQueryParser extends AbstractIndexComponent implements XContentQueryParser {

    public static final String NAME = "query_string";

    private final AnalysisService analysisService;

    @Inject public QueryStringQueryParser(Index index, @IndexSettings Settings settings, AnalysisService analysisService) {
        super(index, settings);
        this.analysisService = analysisService;
    }

    @Override public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        // move to the field value

        String queryString = null;
        String defaultField = AllFieldMapper.NAME; // default to all
        MapperQueryParser.Operator defaultOperator = QueryParser.Operator.OR;
        boolean allowLeadingWildcard = true;
        boolean lowercaseExpandedTerms = true;
        boolean enablePositionIncrements = true;
        float fuzzyMinSim = FuzzyQuery.defaultMinSimilarity;
        int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;
        int phraseSlop = 0;
        float boost = 1.0f;
        boolean escape = false;
        Analyzer analyzer = null;
        List<String> fields = null;
        ExtTObjectFloatHashMap<String> boosts = null;
        float tieBreaker = 0.0f;
        boolean useDisMax = true;

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
                        if (fields == null) {
                            fields = Lists.newArrayList();
                        }
                        fields.add(fField);
                        if (fBoost != -1) {
                            if (boosts == null) {
                                boosts = new ExtTObjectFloatHashMap<String>();
                            }
                            boosts.put(fField, fBoost);
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("query".equals(currentFieldName)) {
                    queryString = parser.text();
                } else if ("default_field".equals(currentFieldName) || "defaultField".equals(currentFieldName)) {
                    defaultField = parseContext.indexName(parser.text());
                } else if ("default_operator".equals(currentFieldName) || "defaultOperator".equals(currentFieldName)) {
                    String op = parser.text();
                    if ("or".equalsIgnoreCase(op)) {
                        defaultOperator = QueryParser.Operator.OR;
                    } else if ("and".equalsIgnoreCase(op)) {
                        defaultOperator = QueryParser.Operator.AND;
                    } else {
                        throw new QueryParsingException(index, "Query default operator [" + op + "] is not allowed");
                    }
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = analysisService.analyzer(parser.text());
                } else if ("allow_leading_wildcard".equals(currentFieldName) || "allowLeadingWildcard".equals(currentFieldName)) {
                    allowLeadingWildcard = parser.booleanValue();
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
                } else if ("phrase_slop".equals(currentFieldName) || "phraseSlop".equals(currentFieldName)) {
                    phraseSlop = parser.intValue();
                } else if ("fuzzy_min_sim".equals(currentFieldName) || "fuzzyMinSim".equals(currentFieldName)) {
                    fuzzyMinSim = parser.floatValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("tie_breaker".equals(currentFieldName) || "tieBreaker".equals(currentFieldName)) {
                    tieBreaker = parser.floatValue();
                }
            }
        }
        if (queryString == null) {
            throw new QueryParsingException(index, "query_string must be provided with a [query]");
        }
        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }

        MapperQueryParser queryParser;
        if (fields != null) {
            if (fields.size() == 1) {
                queryParser = new MapperQueryParser(fields.get(0), analyzer, parseContext.mapperService(), parseContext.indexCache());
            } else {
                MultiFieldMapperQueryParser mQueryParser = new MultiFieldMapperQueryParser(fields, boosts, analyzer, parseContext.mapperService(), parseContext.indexCache());
                mQueryParser.setTieBreaker(tieBreaker);
                mQueryParser.setUseDisMax(useDisMax);
                queryParser = mQueryParser;
            }
        } else {
            queryParser = new MapperQueryParser(defaultField, analyzer, parseContext.mapperService(), parseContext.indexCache());
        }
        queryParser.setEnablePositionIncrements(enablePositionIncrements);
        queryParser.setLowercaseExpandedTerms(lowercaseExpandedTerms);
        queryParser.setAllowLeadingWildcard(allowLeadingWildcard);
        queryParser.setDefaultOperator(defaultOperator);
        queryParser.setFuzzyMinSim(fuzzyMinSim);
        queryParser.setFuzzyPrefixLength(fuzzyPrefixLength);
        queryParser.setPhraseSlop(phraseSlop);

        if (escape) {
            queryString = QueryParser.escape(queryString);
        }

        try {
            Query query = queryParser.parse(queryString);
            query.setBoost(boost);
            return optimizeQuery(fixNegativeQueryIfNeeded(query));
        } catch (ParseException e) {
            throw new QueryParsingException(index, "Failed to parse query [" + queryString + "]", e);
        }
    }
}
