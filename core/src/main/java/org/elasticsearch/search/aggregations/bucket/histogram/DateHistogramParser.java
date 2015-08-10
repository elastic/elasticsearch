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

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.rounding.TimeZoneRounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;
import java.io.IOException;

/**
 *
 */
public class DateHistogramParser implements Aggregator.Parser {

    static final ParseField EXTENDED_BOUNDS = new ParseField("extended_bounds");
    static final ParseField OFFSET = new ParseField("offset");
    static final ParseField INTERVAL = new ParseField("interval");

    public static final ImmutableMap<String, DateTimeUnit> DATE_FIELD_UNITS;

    static {
        DATE_FIELD_UNITS = MapBuilder.<String, DateTimeUnit>newMapBuilder()
                .put("year", DateTimeUnit.YEAR_OF_CENTURY)
                .put("1y", DateTimeUnit.YEAR_OF_CENTURY)
                .put("quarter", DateTimeUnit.QUARTER)
                .put("1q", DateTimeUnit.QUARTER)
                .put("month", DateTimeUnit.MONTH_OF_YEAR)
                .put("1M", DateTimeUnit.MONTH_OF_YEAR)
                .put("week", DateTimeUnit.WEEK_OF_WEEKYEAR)
                .put("1w", DateTimeUnit.WEEK_OF_WEEKYEAR)
                .put("day", DateTimeUnit.DAY_OF_MONTH)
                .put("1d", DateTimeUnit.DAY_OF_MONTH)
                .put("hour", DateTimeUnit.HOUR_OF_DAY)
                .put("1h", DateTimeUnit.HOUR_OF_DAY)
                .put("minute", DateTimeUnit.MINUTES_OF_HOUR)
                .put("1m", DateTimeUnit.MINUTES_OF_HOUR)
                .put("second", DateTimeUnit.SECOND_OF_MINUTE)
                .put("1s", DateTimeUnit.SECOND_OF_MINUTE)
                .immutableMap();
    }

    @Override
    public String type() {
        return InternalDateHistogram.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.numeric(aggregationName, InternalDateHistogram.TYPE, context)
                .targetValueType(ValueType.DATE)
                .formattable(true)
                .timezoneAware(true)
                .build();

        boolean keyed = false;
        long minDocCount = 0;
        ExtendedBounds extendedBounds = null;
        InternalOrder order = (InternalOrder) Histogram.Order.KEY_ASC;
        String interval = null;
        long offset = 0;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (context.parseFieldMatcher().match(currentFieldName, OFFSET)) {
                    offset = parseOffset(parser.text());
                } else if (context.parseFieldMatcher().match(currentFieldName, INTERVAL)) {
                    interval = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("min_doc_count".equals(currentFieldName) || "minDocCount".equals(currentFieldName)) {
                    minDocCount = parser.longValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("order".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            String dir = parser.text();
                            boolean asc = "asc".equals(dir);
                            order = resolveOrder(currentFieldName, asc);
                            //TODO should we throw an error if the value is not "asc" or "desc"???
                        }
                    }
                } else if (context.parseFieldMatcher().match(currentFieldName, EXTENDED_BOUNDS)) {
                    extendedBounds = new ExtendedBounds();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("min".equals(currentFieldName)) {
                                extendedBounds.minAsStr = parser.text();
                            } else if ("max".equals(currentFieldName)) {
                                extendedBounds.maxAsStr = parser.text();
                            } else {
                                throw new SearchParseException(context, "Unknown extended_bounds key for a " + token + " in aggregation ["
                                        + aggregationName + "]: [" + currentFieldName + "].", parser.getTokenLocation());
                            }
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("min".equals(currentFieldName)) {
                                extendedBounds.min = parser.longValue();
                            } else if ("max".equals(currentFieldName)) {
                                extendedBounds.max = parser.longValue();
                            } else {
                                throw new SearchParseException(context, "Unknown extended_bounds key for a " + token + " in aggregation ["
                                        + aggregationName + "]: [" + currentFieldName + "].", parser.getTokenLocation());
                            }
                        } else {
                            throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                                    + currentFieldName + "].", parser.getTokenLocation());
                        }
                    }

                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].",
                        parser.getTokenLocation());
            }
        }

        if (interval == null) {
            throw new SearchParseException(context,
                    "Missing required field [interval] for histogram aggregation [" + aggregationName + "]", parser.getTokenLocation());
        }

        TimeZoneRounding.Builder tzRoundingBuilder;
        DateTimeUnit dateTimeUnit = DATE_FIELD_UNITS.get(interval);
        if (dateTimeUnit != null) {
            tzRoundingBuilder = TimeZoneRounding.builder(dateTimeUnit);
        } else {
            // the interval is a time value?
            tzRoundingBuilder = TimeZoneRounding.builder(TimeValue.parseTimeValue(interval, null, getClass().getSimpleName() + ".interval"));
        }

        Rounding rounding = tzRoundingBuilder
                .timeZone(vsParser.input().timezone())
                .offset(offset).build();

        ValuesSourceConfig config = vsParser.config();
        return new HistogramAggregator.Factory(aggregationName, config, rounding, order, keyed, minDocCount, extendedBounds,
                new InternalDateHistogram.Factory());

    }

    private static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key) || "_time".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        return new InternalOrder.Aggregation(key, asc);
    }

    private long parseOffset(String offset) throws IOException {
        if (offset.charAt(0) == '-') {
            return -TimeValue.parseTimeValue(offset.substring(1), null, getClass().getSimpleName() + ".parseOffset").millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue.parseTimeValue(offset.substring(beginIndex), null, getClass().getSimpleName() + ".parseOffset").millis();
    }
}
