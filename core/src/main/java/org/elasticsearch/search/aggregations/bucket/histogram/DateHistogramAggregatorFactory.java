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

import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

public final class DateHistogramAggregatorFactory
        extends ValuesSourceAggregatorFactory<ValuesSource.Numeric, DateHistogramAggregatorFactory> {

    public static final Map<String, DateTimeUnit> DATE_FIELD_UNITS;

    static {
        Map<String, DateTimeUnit> dateFieldUnits = new HashMap<>();
        dateFieldUnits.put("year", DateTimeUnit.YEAR_OF_CENTURY);
        dateFieldUnits.put("1y", DateTimeUnit.YEAR_OF_CENTURY);
        dateFieldUnits.put("quarter", DateTimeUnit.QUARTER);
        dateFieldUnits.put("1q", DateTimeUnit.QUARTER);
        dateFieldUnits.put("month", DateTimeUnit.MONTH_OF_YEAR);
        dateFieldUnits.put("1M", DateTimeUnit.MONTH_OF_YEAR);
        dateFieldUnits.put("week", DateTimeUnit.WEEK_OF_WEEKYEAR);
        dateFieldUnits.put("1w", DateTimeUnit.WEEK_OF_WEEKYEAR);
        dateFieldUnits.put("day", DateTimeUnit.DAY_OF_MONTH);
        dateFieldUnits.put("1d", DateTimeUnit.DAY_OF_MONTH);
        dateFieldUnits.put("hour", DateTimeUnit.HOUR_OF_DAY);
        dateFieldUnits.put("1h", DateTimeUnit.HOUR_OF_DAY);
        dateFieldUnits.put("minute", DateTimeUnit.MINUTES_OF_HOUR);
        dateFieldUnits.put("1m", DateTimeUnit.MINUTES_OF_HOUR);
        dateFieldUnits.put("second", DateTimeUnit.SECOND_OF_MINUTE);
        dateFieldUnits.put("1s", DateTimeUnit.SECOND_OF_MINUTE);
        DATE_FIELD_UNITS = unmodifiableMap(dateFieldUnits);
    }

    private final DateHistogramInterval dateHistogramInterval;
    private final long interval;
    private final long offset;
    private final InternalOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final ExtendedBounds extendedBounds;

    public DateHistogramAggregatorFactory(String name, Type type, ValuesSourceConfig<Numeric> config, long interval,
            DateHistogramInterval dateHistogramInterval, long offset, InternalOrder order, boolean keyed, long minDocCount,
            ExtendedBounds extendedBounds, AggregationContext context, AggregatorFactory<?> parent,
            AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData) throws IOException {
        super(name, type, config, context, parent, subFactoriesBuilder, metaData);
        this.interval = interval;
        this.dateHistogramInterval = dateHistogramInterval;
        this.offset = offset;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
    }

    public long minDocCount() {
        return minDocCount;
    }

    private Rounding createRounding() {
        Rounding.Builder tzRoundingBuilder;
        if (dateHistogramInterval != null) {
            DateTimeUnit dateTimeUnit = DATE_FIELD_UNITS.get(dateHistogramInterval.toString());
            if (dateTimeUnit != null) {
                tzRoundingBuilder = Rounding.builder(dateTimeUnit);
            } else {
                // the interval is a time value?
                tzRoundingBuilder = Rounding.builder(
                        TimeValue.parseTimeValue(dateHistogramInterval.toString(), null, getClass().getSimpleName() + ".interval"));
            }
        } else {
            // the interval is an integer time value in millis?
            tzRoundingBuilder = Rounding.builder(TimeValue.timeValueMillis(interval));
        }
        if (timeZone() != null) {
            tzRoundingBuilder.timeZone(timeZone());
        }
        Rounding rounding = tzRoundingBuilder.build();
        return rounding;
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource.Numeric valuesSource, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, context, parent);
        }
        return createAggregator(valuesSource, parent, pipelineAggregators, metaData);
    }

    private Aggregator createAggregator(ValuesSource.Numeric valuesSource, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        Rounding rounding = createRounding();
        // we need to round the bounds given by the user and we have to do it
        // for every aggregator we create
        // as the rounding is not necessarily an idempotent operation.
        // todo we need to think of a better structure to the factory/agtor
        // code so we won't need to do that
        ExtendedBounds roundedBounds = null;
        if (extendedBounds != null) {
            // parse any string bounds to longs and round them
            roundedBounds = extendedBounds.parseAndValidate(name, context.searchContext(), config.format()).round(rounding);
        }
        return new DateHistogramAggregator(name, factories, rounding, offset, order, keyed, minDocCount, roundedBounds, valuesSource,
                config.format(), context, parent, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        return createAggregator(null, parent, pipelineAggregators, metaData);
    }
}
