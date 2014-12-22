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
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;

public abstract class AbstractPercentilesParser implements Aggregator.Parser {

    private boolean formattable;

    public AbstractPercentilesParser(boolean formattable) {
        this.formattable = formattable;
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
    
        ValuesSourceParser<ValuesSource.Numeric> vsParser = ValuesSourceParser.numeric(aggregationName, InternalPercentiles.TYPE, context)
                .formattable(formattable).build();
    
        double[] keys = null;
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
                if (keysFieldName().equals(currentFieldName)) {
                    DoubleArrayList values = new DoubleArrayList(10);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double value = parser.doubleValue();
                        values.add(value);
                    }
                    keys = values.toArray();
                    Arrays.sort(keys);
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
        return buildFactory(context, aggregationName, vsParser.config(), keys, compression, keyed);
    }

    protected abstract AggregatorFactory buildFactory(SearchContext context, String aggregationName, ValuesSourceConfig<Numeric> config, double[] cdfValues,
            double compression, boolean keyed);

    protected abstract String keysFieldName();

}
