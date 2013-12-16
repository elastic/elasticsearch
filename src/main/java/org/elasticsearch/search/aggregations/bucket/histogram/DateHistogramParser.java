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

package org.elasticsearch.search.aggregations.bucket.histogram;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.TimeZoneRounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.ValueFormatter;
import org.elasticsearch.search.aggregations.support.numeric.ValueParser;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DateHistogramParser implements Aggregator.Parser {

    private final ImmutableMap<String, DateTimeUnit> dateFieldUnits;

    public DateHistogramParser() {
        dateFieldUnits = MapBuilder.<String, DateTimeUnit>newMapBuilder()
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

        ValuesSourceConfig<NumericValuesSource> config = new ValuesSourceConfig<NumericValuesSource>(NumericValuesSource.class);

        String field = null;
        String script = null;
        String scriptLang = null;
        Map<String, Object> scriptParams = null;
        boolean keyed = false;
        boolean computeEmptyBuckets = false;
        InternalOrder order = (InternalOrder) Histogram.Order.KEY_ASC;
        String interval = null;
        boolean preZoneAdjustLargeInterval = false;
        DateTimeZone preZone = DateTimeZone.UTC;
        DateTimeZone postZone = DateTimeZone.UTC;
        String format = null;
        long preOffset = 0;
        long postOffset = 0;
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
                } else if ("time_zone".equals(currentFieldName) || "timeZone".equals(currentFieldName)) {
                    preZone = parseZone(parser, token);
                } else if ("pre_zone".equals(currentFieldName) || "preZone".equals(currentFieldName)) {
                    preZone = parseZone(parser, token);
                } else if ("pre_zone_adjust_large_interval".equals(currentFieldName) || "preZoneAdjustLargeInterval".equals(currentFieldName)) {
                    preZoneAdjustLargeInterval = parser.booleanValue();
                } else if ("post_zone".equals(currentFieldName) || "postZone".equals(currentFieldName)) {
                    postZone = parseZone(parser, token);
                } else if ("pre_offset".equals(currentFieldName) || "preOffset".equals(currentFieldName)) {
                    preOffset = parseOffset(parser.text());
                } else if ("post_offset".equals(currentFieldName) || "postOffset".equals(currentFieldName)) {
                    postOffset = parseOffset(parser.text());
                } else if ("interval".equals(currentFieldName)) {
                    interval = parser.text();
                } else if ("format".equals(currentFieldName)) {
                    format = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else if ("compute_empty_buckets".equals(currentFieldName) || "computeEmptyBuckets".equals(currentFieldName)) {
                    computeEmptyBuckets = parser.booleanValue();
                } else if ("script_values_sorted".equals(currentFieldName)) {
                    assumeSorted = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                } else if ("order".equals(currentFieldName)) {
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
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (interval == null) {
            throw new SearchParseException(context, "Missing required field [interval] for histogram aggregation [" + aggregationName + "]");
        }

        SearchScript searchScript = null;
        if (script != null) {
            searchScript = context.scriptService().search(context.lookup(), scriptLang, script, scriptParams);
            config.script(searchScript);
        }

        if (!assumeSorted) {
            // we need values to be sorted and unique for efficiency
            config.ensureSorted(true);
        }

        TimeZoneRounding.Builder tzRoundingBuilder;
        DateTimeUnit dateTimeUnit = dateFieldUnits.get(interval);
        if (dateTimeUnit != null) {
            tzRoundingBuilder = TimeZoneRounding.builder(dateTimeUnit);
        } else {
            // the interval is a time value?
            tzRoundingBuilder = TimeZoneRounding.builder(TimeValue.parseTimeValue(interval, null));
        }

        TimeZoneRounding rounding = tzRoundingBuilder
                .preZone(preZone).postZone(postZone)
                .preZoneAdjustLargeInterval(preZoneAdjustLargeInterval)
                .preOffset(preOffset).postOffset(postOffset)
                .build();

        if (format != null) {
            config.formatter(new ValueFormatter.DateTime(format));
        }

        if (field == null) {

            if (searchScript != null) {
                ValueParser valueParser = new ValueParser.DateMath(new DateMathParser(DateFieldMapper.Defaults.DATE_TIME_FORMATTER, DateFieldMapper.Defaults.TIME_UNIT));
                config.parser(valueParser);
                return new HistogramAggregator.Factory(aggregationName, config, rounding, order, keyed, computeEmptyBuckets, InternalDateHistogram.FACTORY);
            }

            // falling back on the get field data context
            return new HistogramAggregator.Factory(aggregationName, config, rounding, order, keyed, computeEmptyBuckets, InternalDateHistogram.FACTORY);
        }

        FieldMapper<?> mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            config.unmapped(true);
            return new HistogramAggregator.Factory(aggregationName, config, rounding, order, keyed, computeEmptyBuckets, InternalDateHistogram.FACTORY);
        }

        if (!(mapper instanceof DateFieldMapper)) {
            throw new SearchParseException(context, "date histogram can only be aggregated on date fields but  [" + field + "] is not a date field");
        }

        IndexFieldData<?> indexFieldData = context.fieldData().getForField(mapper);
        config.fieldContext(new FieldContext(field, indexFieldData));
        return new HistogramAggregator.Factory(aggregationName, config, rounding, order, keyed, computeEmptyBuckets, InternalDateHistogram.FACTORY);
    }

    private static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key) || "_time".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        int i = key.indexOf('.');
        if (i < 0) {
            return HistogramBase.Order.aggregation(key, asc);
        }
        return HistogramBase.Order.aggregation(key.substring(0, i), key.substring(i + 1), asc);
    }

    private long parseOffset(String offset) throws IOException {
        if (offset.charAt(0) == '-') {
            return -TimeValue.parseTimeValue(offset.substring(1), null).millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue.parseTimeValue(offset.substring(beginIndex), null).millis();
    }

    private DateTimeZone parseZone(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_NUMBER) {
            return DateTimeZone.forOffsetHours(parser.intValue());
        } else {
            String text = parser.text();
            int index = text.indexOf(':');
            if (index != -1) {
                int beginIndex = text.charAt(0) == '+' ? 1 : 0;
                // format like -02:30
                return DateTimeZone.forOffsetHoursMinutes(
                        Integer.parseInt(text.substring(beginIndex, index)),
                        Integer.parseInt(text.substring(index + 1))
                );
            } else {
                // id, listed here: http://joda-time.sourceforge.net/timezones.html
                return DateTimeZone.forID(text);
            }
        }
    }

}
