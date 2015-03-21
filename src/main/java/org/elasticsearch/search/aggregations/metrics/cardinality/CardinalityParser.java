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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.core.Murmur3FieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;


public class CardinalityParser implements Aggregator.Parser {

    private static final ParseField PRECISION_THRESHOLD = new ParseField("precision_threshold");

    @Override
    public String type() {
        return InternalCardinality.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String name, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.any(name, InternalCardinality.TYPE, context).formattable(false).build();

        long precisionThreshold = -1;
        Boolean rehash = null;

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
                } else if (PRECISION_THRESHOLD.match(currentFieldName)) {
                    precisionThreshold = parser.longValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + name + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + name + "].");
            }
        }

        ValuesSourceConfig<?> config = vsParser.config();

        if (rehash == null && config.fieldContext() != null && config.fieldContext().mapper() instanceof Murmur3FieldMapper) {
            rehash = false;
        } else if (rehash == null) {
            rehash = true;
        }

        return new CardinalityAggregatorFactory(name, config, precisionThreshold, rehash);

    }

}
