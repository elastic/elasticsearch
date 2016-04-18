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

package org.elasticsearch.search.aggregations.metrics.cardinality;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.AnyValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;


public class CardinalityParser extends AnyValuesSourceParser {

    private static final ParseField REHASH = new ParseField("rehash").withAllDeprecated("no replacement - values will always be rehashed");

    public CardinalityParser() {
        super(true, false);
    }

    @Override
    public String type() {
        return InternalCardinality.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String name, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.any(name, InternalCardinality.TYPE, context).formattable(false).build();

        long precisionThreshold = -1;
        Boolean rehash = null;
        boolean isSumDirectly = false;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token.isValue()) {
                if ("rehash".equals(currentFieldName)) {
                    rehash = parser.booleanValue();
                } else if (context.parseFieldMatcher().match(currentFieldName, PRECISION_THRESHOLD)) {
                    precisionThreshold = parser.longValue();
                } else if("isSumDirectly".equals(currentFieldName)){
                    isSumDirectly = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + name + "]: [" + currentFieldName
                            + "].", parser.getTokenLocation());
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + name + "].", parser.getTokenLocation());
            }
        }
        return factory;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (token.isValue()) {
            if (parseFieldMatcher.match(currentFieldName, CardinalityAggregatorBuilder.PRECISION_THRESHOLD_FIELD)) {
                otherOptions.put(CardinalityAggregatorBuilder.PRECISION_THRESHOLD_FIELD, parser.longValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, REHASH)) {
                // ignore
                return true;
            }
        }
        final CardinalityAggregatorFactory cardinalityAggregatorFactory = new CardinalityAggregatorFactory(name, config, precisionThreshold, rehash);
        cardinalityAggregatorFactory.setSumDirectly(isSumDirectly);
        return cardinalityAggregatorFactory;

    }

    @Override
    public CardinalityAggregatorBuilder getFactoryPrototypes() {
        return CardinalityAggregatorBuilder.PROTOTYPE;
    }
}
