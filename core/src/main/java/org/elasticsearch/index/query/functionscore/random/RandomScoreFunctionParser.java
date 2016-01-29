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


package org.elasticsearch.index.query.functionscore.random;


import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;

import java.io.IOException;

public class RandomScoreFunctionParser implements ScoreFunctionParser<RandomScoreFunctionBuilder> {

    public static String[] NAMES = { "random_score", "randomScore" };

    private static RandomScoreFunctionBuilder PROTOTYPE = new RandomScoreFunctionBuilder();

    @Inject
    public RandomScoreFunctionParser() {
    }

    @Override
    public String[] getNames() {
        return NAMES;
    }

    @Override
    public RandomScoreFunctionBuilder fromXContent(QueryParseContext parseContext, XContentParser parser) throws IOException, ParsingException {
        RandomScoreFunctionBuilder randomScoreFunctionBuilder = new RandomScoreFunctionBuilder();
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("seed".equals(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_NUMBER) {
                        if (parser.numberType() == XContentParser.NumberType.INT) {
                            randomScoreFunctionBuilder.seed(parser.intValue());
                        } else if (parser.numberType() == XContentParser.NumberType.LONG) {
                            randomScoreFunctionBuilder.seed(parser.longValue());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "random_score seed must be an int, long or string, not '"
                                    + token.toString() + "'");
                        }
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        randomScoreFunctionBuilder.seed(parser.text());
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "random_score seed must be an int/long or string, not '"
                                + token.toString() + "'");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), NAMES[0] + " query does not support [" + currentFieldName + "]");
                }
            }
        }
        return randomScoreFunctionBuilder;
    }

    @Override
    public RandomScoreFunctionBuilder getBuilderPrototype() {
        return PROTOTYPE;
    }
}
