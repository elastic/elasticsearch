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
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for common terms query
 */
public class CommonTermsQueryParser implements QueryParser<CommonTermsQueryBuilder> {

    @Override
    public String[] names() {
        return new String[] { CommonTermsQueryBuilder.NAME };
    }

    @Override
    public CommonTermsQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[" + CommonTermsQueryBuilder.NAME + "] query malformed, no field");
        }
        String fieldName = parser.currentName();
        Object text = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzer = null;
        String lowFreqMinimumShouldMatch = null;
        String highFreqMinimumShouldMatch = null;
        boolean disableCoord = CommonTermsQueryBuilder.DEFAULT_DISABLE_COORD;
        Operator highFreqOperator = CommonTermsQueryBuilder.DEFAULT_HIGH_FREQ_OCCUR;
        Operator lowFreqOperator = CommonTermsQueryBuilder.DEFAULT_LOW_FREQ_OCCUR;
        float cutoffFrequency = CommonTermsQueryBuilder.DEFAULT_CUTOFF_FREQ;
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
                                    throw new ParsingException(parser.getTokenLocation(), "[" + CommonTermsQueryBuilder.NAME + "] query does not support [" + innerFieldName
                                            + "] for [" + currentFieldName + "]");
                                }
                            } else {
                                throw new ParsingException(parser.getTokenLocation(), "[" + CommonTermsQueryBuilder.NAME + "] unexpected token type [" + token
                                        + "] after [" + innerFieldName + "]");
                            }
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[" + CommonTermsQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    if ("query".equals(currentFieldName)) {
                        text = parser.objectText();
                    } else if ("analyzer".equals(currentFieldName)) {
                        analyzer = parser.text();
                    } else if ("disable_coord".equals(currentFieldName) || "disableCoord".equals(currentFieldName)) {
                        disableCoord = parser.booleanValue();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("high_freq_operator".equals(currentFieldName) || "highFreqOperator".equals(currentFieldName)) {
                        highFreqOperator = Operator.fromString(parser.text());
                    } else if ("low_freq_operator".equals(currentFieldName) || "lowFreqOperator".equals(currentFieldName)) {
                        lowFreqOperator = Operator.fromString(parser.text());
                    } else if ("minimum_should_match".equals(currentFieldName) || "minimumShouldMatch".equals(currentFieldName)) {
                        lowFreqMinimumShouldMatch = parser.text();
                    } else if ("cutoff_frequency".equals(currentFieldName)) {
                        cutoffFrequency = parser.floatValue();
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[" + CommonTermsQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            text = parser.objectText();
            // move to the next token
            token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(),
                        "[common] query parsed in simplified form, with direct field name, but included more options than just the field name, possibly use its 'options' form, with 'query' element?");
            }
        }

        if (text == null) {
            throw new ParsingException(parser.getTokenLocation(), "No text specified for text query");
        }
        return new CommonTermsQueryBuilder(fieldName, text)
                .lowFreqMinimumShouldMatch(lowFreqMinimumShouldMatch)
                .highFreqMinimumShouldMatch(highFreqMinimumShouldMatch)
                .analyzer(analyzer)
                .highFreqOperator(highFreqOperator)
                .lowFreqOperator(lowFreqOperator)
                .disableCoord(disableCoord)
                .cutoffFrequency(cutoffFrequency)
                .boost(boost)
                .queryName(queryName);
    }

    @Override
    public CommonTermsQueryBuilder getBuilderPrototype() {
        return CommonTermsQueryBuilder.PROTOTYPE;
    }
}
