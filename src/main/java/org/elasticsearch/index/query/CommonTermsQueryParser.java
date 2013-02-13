/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.query;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameQuery;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;

/**
 *
 */
public class CommonTermsQueryParser implements QueryParser {

    public static final String NAME = "common";
    
    static final float DEFAULT_MAX_TERM_DOC_FREQ = 0.01f;
    
    static final Occur DEFAULT_HIGH_FREQ_OCCUR = Occur.MUST;
    
    static final Occur DEFAULT_LOW_FREQ_OCCUR = Occur.MUST;

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
        String minimumShouldMatch = null;
        boolean disableCoords = DEFAULT_DISABLE_COORDS;
        Occur highFreqOccur = DEFAULT_HIGH_FREQ_OCCUR;
        Occur lowFreqOccur = DEFAULT_HIGH_FREQ_OCCUR;
        float maxTermFrequency = DEFAULT_MAX_TERM_DOC_FREQ;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("query".equals(currentFieldName)) {
                        value = parser.objectText();
                    } else if ("analyzer".equals(currentFieldName)) {
                        String analyzer = parser.text();
                        if (parseContext.analysisService().analyzer(analyzer) == null) {
                            throw new QueryParsingException(parseContext.index(), "[common] analyzer [" + parser.text() + "] not found");
                        }
                        queryAnalyzer = analyzer;
                    } else if ("disable_coords".equals(currentFieldName)) {
                        disableCoords = parser.booleanValue();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("high_freq_operator".equals(currentFieldName)) {
                        String op = parser.text();
                        if ("or".equalsIgnoreCase(op)) {
                            highFreqOccur = BooleanClause.Occur.SHOULD;
                        } else if ("and".equalsIgnoreCase(op)) {
                            highFreqOccur = BooleanClause.Occur.MUST;
                        } else {
                            throw new QueryParsingException(parseContext.index(),
                                    "[common] query requires operator to be either 'and' or 'or', not [" + op + "]");
                        }
                    } else if ("low_freq_operator".equals(currentFieldName)) {
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
                        minimumShouldMatch = parser.textOrNull();
                    } else if ("cutoff_frequency".equals(currentFieldName)) {
                        maxTermFrequency = parser.floatValue();
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
        ExtendedCommonTermsQuery query = new ExtendedCommonTermsQuery(highFreqOccur, lowFreqOccur, maxTermFrequency, disableCoords);
        query.setBoost(boost);
        return parseQueryString(query, value.toString(), fieldName, parseContext, queryAnalyzer, minimumShouldMatch);
    }
        

    private final Query parseQueryString(ExtendedCommonTermsQuery query, String queryString, String fieldName, QueryParseContext parseContext,
            String queryAnalyzer, String minimumShouldMatch) throws IOException {
    
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
                throw new ElasticSearchIllegalArgumentException("No analyzer found for [" + queryAnalyzer + "]");
            }
        }

        // Logic similar to QueryParser#getFieldQuery
        TokenStream source = analyzer.tokenStream(field, new FastStringReader(queryString.toString()));
        source.reset();
        CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
        int count = 0;
        while (source.incrementToken()) {
            BytesRef ref = new BytesRef(termAtt.length() * 4); // oversize for
                                                               // UTF-8
            UnicodeUtil.UTF16toUTF8(termAtt.buffer(), 0, termAtt.length(), ref);
            query.add(new Term(field, ref));
            count++;
        }
        
        if (count == 0) {
            return null;
        }
        query.setMinimumNumberShouldMatch(minimumShouldMatch);
        return wrapSmartNameQuery(query, smartNameFieldMappers, parseContext);
    }
}