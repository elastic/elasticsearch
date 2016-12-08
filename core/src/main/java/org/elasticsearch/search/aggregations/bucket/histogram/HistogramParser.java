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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.NumericValuesSourceParser;

import java.io.IOException;

/**
 * A parser for date histograms. This translates json into an
 * {@link HistogramAggregationBuilder} instance.
 */
public class HistogramParser extends NumericValuesSourceParser {

    private static final ObjectParser<double[], ParseFieldMatcherSupplier> EXTENDED_BOUNDS_PARSER = new ObjectParser<>(
            Histogram.EXTENDED_BOUNDS_FIELD.getPreferredName(),
            () -> new double[]{ Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY });
    static {
        EXTENDED_BOUNDS_PARSER.declareDouble((bounds, d) -> bounds[0] = d, new ParseField("min"));
        EXTENDED_BOUNDS_PARSER.declareDouble((bounds, d) -> bounds[1] = d, new ParseField("max"));
    }

    private final ObjectParser<HistogramAggregationBuilder, QueryParseContext> parser;

    public HistogramParser() {
        parser = new ObjectParser<>(HistogramAggregationBuilder.NAME);
        addFields(parser, true, true, false);

        parser.declareDouble(HistogramAggregationBuilder::interval, Histogram.INTERVAL_FIELD);

        parser.declareDouble(HistogramAggregationBuilder::offset, Histogram.OFFSET_FIELD);

        parser.declareBoolean(HistogramAggregationBuilder::keyed, Histogram.KEYED_FIELD);

        parser.declareLong(HistogramAggregationBuilder::minDocCount, Histogram.MIN_DOC_COUNT_FIELD);

        parser.declareField((histogram, extendedBounds) -> {
            histogram.extendedBounds(extendedBounds[0], extendedBounds[1]);
        }, EXTENDED_BOUNDS_PARSER::apply, ExtendedBounds.EXTENDED_BOUNDS_FIELD, ObjectParser.ValueType.OBJECT);

        parser.declareField(HistogramAggregationBuilder::order, HistogramParser::parseOrder,
                Histogram.ORDER_FIELD, ObjectParser.ValueType.OBJECT);
    }

    @Override
    public AggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        return parser.parse(context.parser(), new HistogramAggregationBuilder(aggregationName), context);
    }

    private static InternalOrder parseOrder(XContentParser parser, QueryParseContext context) throws IOException {
        InternalOrder order = null;
        Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                String dir = parser.text();
                boolean asc = "asc".equals(dir);
                if (!asc && !"desc".equals(dir)) {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown order direction: [" + dir
                            + "]. Should be either [asc] or [desc]");
                }
                order = resolveOrder(currentFieldName, asc);
            }
        }
        return order;
    }

    static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        return new InternalOrder.Aggregation(key, asc);
    }
}
