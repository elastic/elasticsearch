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
package org.elasticsearch.search.aggregations.metrics.stats.extended;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class ExtendedStatsParser  implements Aggregator.Parser {

    static final ParseField SIGMA = new ParseField("sigma");

    @Override
    public String type() {
        return InternalExtendedStats.TYPE.name();
    }

    protected AggregatorFactory createFactory(String aggregationName, ValuesSourceConfig<ValuesSource.Numeric> config, double sigma) {
        return new ExtendedStatsAggregator.Factory(aggregationName, config, sigma);
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser<ValuesSource.Numeric> vsParser = ValuesSourceParser.numeric(aggregationName, InternalExtendedStats.TYPE, context).formattable(true)
                .build();

        XContentParser.Token token;
        String currentFieldName = null;
        double sigma = 2.0;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (SIGMA.match(currentFieldName)) {
                    sigma = parser.doubleValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (sigma < 0) {
            throw new SearchParseException(context, "[sigma] must not be negative. Value provided was" + sigma );
        }

        return createFactory(aggregationName, vsParser.config(), sigma);
    }
}
