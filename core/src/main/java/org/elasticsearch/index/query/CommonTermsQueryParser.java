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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for common terms query
 */
public class CommonTermsQueryParser implements QueryParser<CommonTermsQueryBuilder> {

    public static final ParseField CUTOFF_FREQUENCY_FIELD = new ParseField("cutoff_frequency");
    public static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    public static final ParseField LOW_FREQ_OPERATOR_FIELD = new ParseField("low_freq_operator");
    public static final ParseField HIGH_FREQ_OPERATOR_FIELD = new ParseField("high_freq_operator");
    public static final ParseField DISABLE_COORD_FIELD = new ParseField("disable_coord");
    public static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField HIGH_FREQ_FIELD = new ParseField("high_freq");
    public static final ParseField LOW_FREQ_FIELD = new ParseField("low_freq");

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
                    if (parseContext.parseFieldMatcher().match(currentFieldName, MINIMUM_SHOULD_MATCH_FIELD)) {
                        String innerFieldName = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                innerFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                if (parseContext.parseFieldMatcher().match(innerFieldName, LOW_FREQ_FIELD)) {
                                    lowFreqMinimumShouldMatch = parser.text();
                                } else if (parseContext.parseFieldMatcher().match(innerFieldName, HIGH_FREQ_FIELD)) {
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
                    if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                        text = parser.objectText();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, ANALYZER_FIELD)) {
                        analyzer = parser.text();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, DISABLE_COORD_FIELD)) {
                        disableCoord = parser.booleanValue();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                        boost = parser.floatValue();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, HIGH_FREQ_OPERATOR_FIELD)) {
                        highFreqOperator = Operator.fromString(parser.text());
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, LOW_FREQ_OPERATOR_FIELD)) {
                        lowFreqOperator = Operator.fromString(parser.text());
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, MINIMUM_SHOULD_MATCH_FIELD)) {
                        lowFreqMinimumShouldMatch = parser.text();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, CUTOFF_FREQUENCY_FIELD)) {
                        cutoffFrequency = parser.floatValue();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
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
