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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

/**
 *
 */
public class CommonTermsQueryParser implements QueryParser {

    public static final String NAME = "common";

    static final float DEFAULT_MAX_TERM_DOC_FREQ = 0.01f;

    static final Occur DEFAULT_HIGH_FREQ_OCCUR = Occur.SHOULD;

    static final Occur DEFAULT_LOW_FREQ_OCCUR = Occur.SHOULD;

    static final boolean DEFAULT_DISABLE_COORDS = true;


    @Inject
    public CommonTermsQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[] { NAME };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[common] query malformed, no field");
        }
        String fieldName = parser.currentName();
        Object value = null;
        float boost = 1.0f;
        String queryAnalyzer = null;
        String lowFreqMinimumShouldMatch = null;
        String highFreqMinimumShouldMatch = null;
        boolean disableCoords = DEFAULT_DISABLE_COORDS;
        Occur highFreqOccur = DEFAULT_HIGH_FREQ_OCCUR;
        Occur lowFreqOccur = DEFAULT_LOW_FREQ_OCCUR;
        float maxTermFrequency = DEFAULT_MAX_TERM_DOC_FREQ;
        String queryName = null;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                        String innerFieldName = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                innerFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                if ("low_freq".equals(innerFieldName) || "lowFreq".equals(innerFieldName)) {
                                    lowFreqMinimumShouldMatch = parser.text();
                                } else if ("high_freq".equals(innerFieldName) || "highFreq".equals(innerFieldName)) {
                                    highFreqMinimumShouldMatch = parser.text();
                                } else {
                                    throw new QueryParsingException(parseContext.index(), "[common] query does not support [" + innerFieldName + "] for [" + currentFieldName + "]");
                                }
                            }
                        }
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[common] query does not support [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    if ("query".equals(currentFieldName)) {
                        value = parser.objectText();
                    } else if ("analyzer".equals(currentFieldName)) {
                        String analyzer = parser.text();
                        if (parseContext.analysisService().analyzer(analyzer) == null) {
                            throw new QueryParsingException(parseContext.index(), "[common] analyzer [" + parser.text() + "] not found");
                        }
                        queryAnalyzer = analyzer;
                    } else if ("disable_coord".equals(currentFieldName) || "disableCoord".equals(currentFieldName)) {
                        disableCoords = parser.booleanValue();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("high_freq_operator".equals(currentFieldName) || "highFreqOperator".equals(currentFieldName)) {
                        String op = parser.text();
                        if ("or".equalsIgnoreCase(op)) {
                            highFreqOccur = BooleanClause.Occur.SHOULD;
                        } else if ("and".equalsIgnoreCase(op)) {
                            highFreqOccur = BooleanClause.Occur.MUST;
                        } else {
                            throw new QueryParsingException(parseContext.index(),
                                    "[common] query requires operator to be either 'and' or 'or', not [" + op + "]");
                        }
                    } else if ("low_freq_operator".equals(currentFieldName) || "lowFreqOperator".equals(currentFieldName)) {
                        String op = parser.text();
                        if ("or".equalsIgnoreCase(op)) {
                            lowFreqOccur = BooleanClause.Occur.SHOULD;
                        } else if ("and".equalsIgnoreCase(op)) {
                            lowFreqOccur = BooleanClause.Occur.MUST;
                        } else {
                            throw new QueryParsingException(parseContext.index(),
                                    "[common] query requires operator to be either 'and' or 'or', not [" + op + "]");
                        }
                    } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                        lowFreqMinimumShouldMatch = parser.text();
                    } else if ("cutoff_frequency".equals(currentFieldName)) {
                        maxTermFrequency = parser.floatValue();
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[common] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.objectText();
            // move to the next token
            token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new QueryParsingException(
                        parseContext.index(),
                        "[common] query parsed in simplified form, with direct field name, but included more options than just the field name, possibly use its 'options' form, with 'query' element?");
            }
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No text specified for text query");
        }
        FieldMapper<?> mapper = null;
        String field;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            mapper = smartNameFieldMappers.mapper();
            field = mapper.names().indexName();
        } else {
            field = fieldName;
        }

        Analyzer analyzer = null;
        if (queryAnalyzer == null) {
            if (mapper != null) {
                analyzer = mapper.searchAnalyzer();
            }
            if (analyzer == null && smartNameFieldMappers != null) {
                analyzer = smartNameFieldMappers.searchAnalyzer();
            }
            if (analyzer == null) {
                analyzer = parseContext.mapperService().searchAnalyzer();
            }
        } else {
            analyzer = parseContext.mapperService().analysisService().analyzer(queryAnalyzer);
            if (analyzer == null) {
                throw new ElasticsearchIllegalArgumentException("No analyzer found for [" + queryAnalyzer + "]");
            }
        }

        ExtendedCommonTermsQuery commonsQuery = new ExtendedCommonTermsQuery(highFreqOccur, lowFreqOccur, maxTermFrequency, disableCoords, mapper);
        commonsQuery.setBoost(boost);
        Query query = parseQueryString(commonsQuery, value.toString(), field, parseContext, analyzer, lowFreqMinimumShouldMatch, highFreqMinimumShouldMatch, smartNameFieldMappers);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }


    private final Query parseQueryString(ExtendedCommonTermsQuery query, String queryString, String field, QueryParseContext parseContext,
            Analyzer analyzer, String lowFreqMinimumShouldMatch, String highFreqMinimumShouldMatch, MapperService.SmartNameFieldMappers smartNameFieldMappers) throws IOException {
        // Logic similar to QueryParser#getFieldQuery
        int count = 0;
        try (TokenStream source = analyzer.tokenStream(field, queryString.toString())) {
            source.reset();
            CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
            BytesRefBuilder builder = new BytesRefBuilder();
            while (source.incrementToken()) {
                // UTF-8
                builder.copyChars(termAtt);
                query.add(new Term(field, builder.toBytesRef()));
                count++;
            }
        }

        if (count == 0) {
            return null;
        }
        query.setLowFreqMinimumNumberShouldMatch(lowFreqMinimumShouldMatch);
        query.setHighFreqMinimumNumberShouldMatch(highFreqMinimumShouldMatch);
        return query;
    }
}
