/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.range.date;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.support.numeric.ValueParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DateRangeParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalDateRange.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceConfig<NumericValuesSource> config = new ValuesSourceConfig<NumericValuesSource>(NumericValuesSource.class);

        String field = null;
        List<RangeAggregator.Range> ranges = null;
        String script = null;
        String scriptLang = null;
        Map<String, Object> scriptParams = null;
        boolean keyed = false;
        String format = null;
        boolean assumeSorted = false;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("format".equals(currentFieldName)) {
                    format = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("ranges".equals(currentFieldName)) {
                    ranges = new ArrayList<RangeAggregator.Range>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double from = Double.NEGATIVE_INFINITY;
                        String fromAsStr = null;
                        double to = Double.POSITIVE_INFINITY;
                        String toAsStr = null;
                        String key = null;
                        String toOrFromOrKey = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                toOrFromOrKey = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                if ("from".equals(toOrFromOrKey)) {
                                    from = parser.doubleValue();
                                } else if ("to".equals(toOrFromOrKey)) {
                                    to = parser.doubleValue();
                                } else {
                                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                                }
                            } else if (token == XContentParser.Token.VALUE_STRING) {
                                if ("from".equals(toOrFromOrKey)) {
                                    fromAsStr = parser.text();
                                } else if ("to".equals(toOrFromOrKey)) {
                                    toAsStr = parser.text();
                                } else if ("key".equals(toOrFromOrKey)) {
                                    key = parser.text();
                                } else {
                                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                                }
                            }
                        }
                        ranges.add(new RangeAggregator.Range(key, from, fromAsStr, to, toAsStr));
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else if ("script_values_sorted".equals(currentFieldName)) {
                    assumeSorted = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (ranges == null) {
            throw new SearchParseException(context, "Missing [ranges] in ranges aggregator [" + aggregationName + "]");
        }

        if (script != null) {
            config.script(context.scriptService().search(context.lookup(), scriptLang, script, scriptParams));
        }

        if (!assumeSorted) {
            // we need values to be sorted and unique for efficiency
            config.ensureSorted(true);
        }

        if (format != null) {
            config.formatter(new ValueFormatter.DateTime(format));
        } else {
            config.formatter(ValueFormatter.DateTime.DEFAULT);
        }

        config.parser(ValueParser.DateMath.DEFAULT);

        if (field == null) {
            return new RangeAggregator.Factory(aggregationName, config, InternalDateRange.FACTORY, ranges, keyed);
        }

        FieldMapper<?> mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            config.unmapped(true);
            return new RangeAggregator.Factory(aggregationName, config, InternalDateRange.FACTORY, ranges, keyed);
        }

        if (!(mapper instanceof DateFieldMapper)) {
            throw new AggregationExecutionException("date_range aggregation can only be applied to date fields which is not the case with field [" + field + "]");
        }

        IndexFieldData<?> indexFieldData = context.fieldData().getForField(mapper);
        config.fieldContext(new FieldContext(field, indexFieldData));
        if (format == null) {
            config.formatter(new ValueFormatter.DateTime(((DateFieldMapper) mapper).dateTimeFormatter()));
        }
        config.parser(new ValueParser.DateMath(((DateFieldMapper) mapper).dateMathParser()));
        return new RangeAggregator.Factory(aggregationName, config, InternalDateRange.FACTORY, ranges, keyed);
    }
}
