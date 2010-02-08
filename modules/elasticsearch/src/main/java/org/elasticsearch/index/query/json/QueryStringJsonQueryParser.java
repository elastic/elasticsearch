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

package org.elasticsearch.index.query.json;

import com.google.inject.Inject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.support.MapperQueryParser;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class QueryStringJsonQueryParser extends AbstractIndexComponent implements JsonQueryParser {

    public static final String NAME = "queryString";

    private final AnalysisService analysisService;

    @Inject public QueryStringJsonQueryParser(Index index, @IndexSettings Settings settings, AnalysisService analysisService) {
        super(index, settings);
        this.analysisService = analysisService;
    }

    @Override public String name() {
        return NAME;
    }

    @Override public Query parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        // move to the field value

        String queryString = null;
        String defaultField = null;
        MapperQueryParser.Operator defaultOperator = QueryParser.Operator.OR;
        boolean allowLeadingWildcard = true;
        boolean lowercaseExpandedTerms = true;
        boolean enablePositionIncrements = true;
        float fuzzyMinSim = FuzzyQuery.defaultMinSimilarity;
        int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;
        int phraseSlop = 0;
        float boost = 1.0f;
        Analyzer analyzer = null;

        String currentFieldName = null;
        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else if (token == JsonToken.VALUE_STRING) {
                if ("query".equals(currentFieldName)) {
                    queryString = jp.getText();
                } else if ("defaultField".equals(currentFieldName)) {
                    defaultField = parseContext.indexName(jp.getText());
                } else if ("defaultOperator".equals(currentFieldName)) {
                    String op = jp.getText();
                    if ("or".equalsIgnoreCase(op)) {
                        defaultOperator = QueryParser.Operator.OR;
                    } else if ("and".equalsIgnoreCase(op)) {
                        defaultOperator = QueryParser.Operator.AND;
                    } else {
                        throw new QueryParsingException(index, "Query default operator [" + op + "] is not allowed");
                    }
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = analysisService.analyzer(jp.getText());
                }
            } else if (token == JsonToken.VALUE_FALSE || token == JsonToken.VALUE_TRUE) {
                if ("allowLeadingWildcard".equals(currentFieldName)) {
                    allowLeadingWildcard = token == JsonToken.VALUE_TRUE;
                } else if ("lowercaseExpandedTerms".equals(currentFieldName)) {
                    lowercaseExpandedTerms = token == JsonToken.VALUE_TRUE;
                } else if ("enablePositionIncrements".equals(currentFieldName)) {
                    enablePositionIncrements = token == JsonToken.VALUE_TRUE;
                }
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                if ("fuzzyMinSim".equals(currentFieldName)) {
                    fuzzyMinSim = jp.getFloatValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = jp.getFloatValue();
                }
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                if ("fuzzyPrefixLength".equals(currentFieldName)) {
                    fuzzyPrefixLength = jp.getIntValue();
                } else if ("phraseSlop".equals(currentFieldName)) {
                    phraseSlop = jp.getIntValue();
                } else if ("fuzzyMinSim".equals(currentFieldName)) {
                    fuzzyMinSim = jp.getFloatValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = jp.getFloatValue();
                }
            }
        }
        if (queryString == null) {
            throw new QueryParsingException(index, "QueryString must be provided with a [query]");
        }
        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }

        MapperQueryParser queryParser = new MapperQueryParser(defaultField, analyzer, parseContext.mapperService(), parseContext.filterCache());
        queryParser.setEnablePositionIncrements(enablePositionIncrements);
        queryParser.setLowercaseExpandedTerms(lowercaseExpandedTerms);
        queryParser.setAllowLeadingWildcard(allowLeadingWildcard);
        queryParser.setDefaultOperator(defaultOperator);
        queryParser.setFuzzyMinSim(fuzzyMinSim);
        queryParser.setFuzzyPrefixLength(fuzzyPrefixLength);
        queryParser.setPhraseSlop(phraseSlop);

        try {
            Query query = queryParser.parse(queryString);
            query.setBoost(boost);
            return query;
        } catch (ParseException e) {
            throw new QueryParsingException(index, "Failed to parse query [" + queryString + "]", e);
        }
    }
}
