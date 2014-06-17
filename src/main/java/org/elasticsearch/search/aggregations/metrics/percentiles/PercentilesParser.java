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
package org.elasticsearch.search.aggregations.metrics.percentiles;

import com.carrotsearch.hppc.DoubleArrayList;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class PercentilesParser implements Aggregator.Parser {

    private final static double[] DEFAULT_PERCENTS = new double[] { 1, 5, 25, 50, 75, 95, 99 };

    @Override
    public String type() {
        return InternalPercentiles.TYPE.name();
    }

    /**
     * We must override the parse method because we need to allow custom parameters
     * (execution_hint, etc) which is not possible otherwise
     */
    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser<ValuesSource.Numeric> vsParser = ValuesSourceParser.numeric(aggregationName, InternalPercentiles.TYPE, context)
                .requiresSortedValues(true)
                .build();

        double[] percents = DEFAULT_PERCENTS;
        boolean keyed = true;
        double compression = 100;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("percents".equals(currentFieldName)) {
                    DoubleArrayList values = new DoubleArrayList(10);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double percent = parser.doubleValue();
                        if (percent < 0 || percent > 100) {
                            throw new SearchParseException(context, "the percents in the percentiles aggregation [" +
                                    aggregationName + "] must be in the [0, 100] range");
                        }
                        values.add(percent);
                    }
                    percents = values.toArray();
                    // Some impls rely on the fact that percents are sorted
                    Arrays.sort(percents);
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("compression".equals(currentFieldName)) {
                    compression = parser.doubleValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        return new PercentilesAggregator.Factory(aggregationName, vsParser.config(), percents, compression, keyed);
    }

}
