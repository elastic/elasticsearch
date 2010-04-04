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
import org.elasticsearch.util.Booleans;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

import static org.elasticsearch.util.lucene.search.Queries.*;

/**
 * @author kimchy (shay.banon)
 */
public class FieldJsonQueryParser extends AbstractIndexComponent implements JsonQueryParser {

    public static final String NAME = "field";

    private final AnalysisService analysisService;

    @Inject public FieldJsonQueryParser(Index index, @IndexSettings Settings settings, AnalysisService analysisService) {
        super(index, settings);
        this.analysisService = analysisService;
    }

    @Override public String[] names() {
        return new String[]{NAME};
    }

    @Override public Query parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        JsonToken token = jp.nextToken();
        assert token == JsonToken.FIELD_NAME;
        String fieldName = jp.getCurrentName();

        String queryString = null;
        float boost = 1.0f;
        MapperQueryParser.Operator defaultOperator = QueryParser.Operator.OR;
        boolean lowercaseExpandedTerms = true;
        boolean enablePositionIncrements = true;
        int phraseSlop = 0;
        float fuzzyMinSim = FuzzyQuery.defaultMinSimilarity;
        int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;
        boolean escape = false;
        Analyzer analyzer = null;
        token = jp.nextToken();
        if (token == JsonToken.START_OBJECT) {
            String currentFieldName = null;
            while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME) {
                    currentFieldName = jp.getCurrentName();
                } else if (token == JsonToken.VALUE_STRING) {
                    if ("query".equals(currentFieldName)) {
                        queryString = jp.getText();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = Float.parseFloat(jp.getText());
                    } else if ("enable_position_increments".equals(currentFieldName)) {
                        enablePositionIncrements = Booleans.parseBoolean(jp.getText(), true);
                    } else if ("lowercase_expanded_terms".equals(currentFieldName)) {
                        lowercaseExpandedTerms = Booleans.parseBoolean(jp.getText(), true);
                    } else if ("phrase_slop".equals(currentFieldName)) {
                        phraseSlop = Integer.parseInt(jp.getText());
                    } else if ("analyzer".equals(currentFieldName)) {
                        analyzer = analysisService.analyzer(jp.getText());
                    } else if ("default_operator".equals(currentFieldName)) {
                        String op = jp.getText();
                        if ("or".equalsIgnoreCase(op)) {
                            defaultOperator = QueryParser.Operator.OR;
                        } else if ("and".equalsIgnoreCase(op)) {
                            defaultOperator = QueryParser.Operator.AND;
                        } else {
                            throw new QueryParsingException(index, "Query default operator [" + op + "] is not allowed");
                        }
                    } else if ("fuzzy_min_sim".equals(currentFieldName)) {
                        fuzzyMinSim = Float.parseFloat(jp.getText());
                    } else if ("fuzzy_prefix_length".equals(currentFieldName)) {
                        fuzzyPrefixLength = Integer.parseInt(jp.getText());
                    } else if ("escape".equals(currentFieldName)) {
                        escape = Booleans.parseBoolean(jp.getText(), false);
                    }
                } else if (token == JsonToken.VALUE_NUMBER_INT) {
                    if ("query".equals(currentFieldName)) {
                        queryString = jp.getText();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = jp.getIntValue();
                    } else if ("enable_position_increments".equals(currentFieldName)) {
                        enablePositionIncrements = jp.getIntValue() != 0;
                    } else if ("lowercase_expanded_terms".equals(currentFieldName)) {
                        lowercaseExpandedTerms = jp.getIntValue() != 0;
                    } else if ("phrase_slop".equals(currentFieldName)) {
                        phraseSlop = jp.getIntValue();
                    } else if ("fuzzy_min_sim".equals(currentFieldName)) {
                        fuzzyMinSim = jp.getIntValue();
                    } else if ("fuzzy_prefix_length".equals(currentFieldName)) {
                        fuzzyPrefixLength = jp.getIntValue();
                    } else if ("escape".equals(currentFieldName)) {
                        escape = jp.getIntValue() != 0;
                    }
                } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                    if ("query".equals(currentFieldName)) {
                        queryString = jp.getText();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = jp.getFloatValue();
                    } else if ("fuzzy_prefix_length".equals(currentFieldName)) {
                        fuzzyPrefixLength = jp.getIntValue();
                    }
                } else if (token == JsonToken.VALUE_TRUE) {
                    if ("query".equals(currentFieldName)) {
                        queryString = jp.getText();
                    } else if ("enable_position_increments".equals(currentFieldName)) {
                        enablePositionIncrements = true;
                    } else if ("lowercase_expanded_terms".equals(currentFieldName)) {
                        lowercaseExpandedTerms = true;
                    } else if ("escape".equals(currentFieldName)) {
                        escape = true;
                    }
                } else if (token == JsonToken.VALUE_FALSE) {
                    if ("query".equals(currentFieldName)) {
                        queryString = jp.getText();
                    } else if ("enable_position_increments".equals(currentFieldName)) {
                        enablePositionIncrements = false;
                    } else if ("lowercase_expanded_terms".equals(currentFieldName)) {
                        lowercaseExpandedTerms = false;
                    } else if ("escape".equals(currentFieldName)) {
                        escape = false;
                    }
                }
            }
            jp.nextToken();
        } else {
            queryString = jp.getText();
            // move to the next token
            jp.nextToken();
        }

        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }

        if (queryString == null) {
            throw new QueryParsingException(index, "No value specified for term query");
        }

        MapperQueryParser queryParser = new MapperQueryParser(fieldName, analyzer, parseContext.mapperService(), parseContext.indexCache());
        queryParser.setEnablePositionIncrements(enablePositionIncrements);
        queryParser.setLowercaseExpandedTerms(lowercaseExpandedTerms);
        queryParser.setPhraseSlop(phraseSlop);
        queryParser.setDefaultOperator(defaultOperator);
        queryParser.setFuzzyMinSim(fuzzyMinSim);
        queryParser.setFuzzyPrefixLength(fuzzyPrefixLength);

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