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

package org.elasticsearch.search.aggregations.reducers.derivative;

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.reducers.ReducerFactory;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DerivativeParser implements Reducer.Parser {

    public static final ParseField FORMAT = new ParseField("format");
    public static final ParseField GAP_POLICY = new ParseField("gap_policy");
    public static final ParseField UNITS = new ParseField("units");

    private final ImmutableMap<String, DateTimeUnit> dateFieldUnits;

    public DerivativeParser() {
        dateFieldUnits = MapBuilder.<String, DateTimeUnit> newMapBuilder().put("year", DateTimeUnit.YEAR_OF_CENTURY)
                .put("1y", DateTimeUnit.YEAR_OF_CENTURY).put("quarter", DateTimeUnit.QUARTER).put("1q", DateTimeUnit.QUARTER)
                .put("month", DateTimeUnit.MONTH_OF_YEAR).put("1M", DateTimeUnit.MONTH_OF_YEAR).put("week", DateTimeUnit.WEEK_OF_WEEKYEAR)
                .put("1w", DateTimeUnit.WEEK_OF_WEEKYEAR).put("day", DateTimeUnit.DAY_OF_MONTH).put("1d", DateTimeUnit.DAY_OF_MONTH)
                .put("hour", DateTimeUnit.HOUR_OF_DAY).put("1h", DateTimeUnit.HOUR_OF_DAY).put("minute", DateTimeUnit.MINUTES_OF_HOUR)
                .put("1m", DateTimeUnit.MINUTES_OF_HOUR).put("second", DateTimeUnit.SECOND_OF_MINUTE)
                .put("1s", DateTimeUnit.SECOND_OF_MINUTE).immutableMap();
    }

    @Override
    public String type() {
        return DerivativeReducer.TYPE.name();
    }

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;
        String units = null;
        GapPolicy gapPolicy = GapPolicy.IGNORE;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (FORMAT.match(currentFieldName)) {
                    format = parser.text();
                } else if (BUCKETS_PATH.match(currentFieldName)) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (GAP_POLICY.match(currentFieldName)) {
                    gapPolicy = GapPolicy.parse(context, parser.text());
                } else if (UNITS.match(currentFieldName)) {
                    units = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_PATH.match(currentFieldName)) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPaths = paths.toArray(new String[paths.size()]);
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + reducerName + "].");
            }
        }

        if (bucketsPaths == null) {
            throw new SearchParseException(context, "Missing required field [" + BUCKETS_PATH.getPreferredName()
                    + "] for derivative aggregation [" + reducerName + "]");
        }

        ValueFormatter formatter = null;
        if (format != null) {
            formatter = ValueFormat.Patternable.Number.format(format).formatter();
        }

        long xAxisUnits = -1;
        if (units != null) {
            DateTimeUnit dateTimeUnit = dateFieldUnits.get(units);
            if (dateTimeUnit != null) {
                xAxisUnits = dateTimeUnit.field().getDurationField().getUnitMillis();
            } else {
                TimeValue timeValue = TimeValue.parseTimeValue(units, null);
                if (timeValue != null) {
                    xAxisUnits = timeValue.getMillis();
                }
            }
        }

        return new DerivativeReducer.Factory(reducerName, bucketsPaths, formatter, gapPolicy, xAxisUnits);
    }

}
