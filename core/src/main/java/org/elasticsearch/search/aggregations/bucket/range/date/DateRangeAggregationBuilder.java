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

package org.elasticsearch.search.aggregations.bucket.range.date;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.joda.time.DateTime;

import java.io.IOException;

public class DateRangeAggregationBuilder extends AbstractRangeBuilder<DateRangeAggregationBuilder, RangeAggregator.Range> {
    public static final String NAME = InternalDateRange.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    public DateRangeAggregationBuilder(String name) {
        super(name, InternalDateRange.FACTORY);
    }

    /**
     * Read from a stream.
     */
    public DateRangeAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalDateRange.FACTORY, Range::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the dates, inclusive
     * @param to
     *            the upper bound on the dates, exclusive
     */
    public DateRangeAggregationBuilder addRange(String key, String from, String to) {
        addRange(new Range(key, from, to));
        return this;
    }

    /**
     * Same as {@link #addRange(String, String, String)} but the key will be
     * automatically generated based on <code>from</code> and <code>to</code>.
     */
    public DateRangeAggregationBuilder addRange(String from, String to) {
        return addRange(null, from, to);
    }

    /**
     * Add a new range with no lower bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param to
     *            the upper bound on the dates, exclusive
     */
    public DateRangeAggregationBuilder addUnboundedTo(String key, String to) {
        addRange(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, String)} but the key will be
     * computed automatically.
     */
    public DateRangeAggregationBuilder addUnboundedTo(String to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Add a new range with no upper bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     */
    public DateRangeAggregationBuilder addUnboundedFrom(String key, String from) {
        addRange(new Range(key, from, null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, String)} but the key will be
     * computed automatically.
     */
    public DateRangeAggregationBuilder addUnboundedFrom(String from) {
        return addUnboundedFrom(null, from);
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the dates, inclusive
     * @param to
     *            the upper bound on the dates, exclusive
     */
    public DateRangeAggregationBuilder addRange(String key, double from, double to) {
        addRange(new Range(key, from, to));
        return this;
    }

    /**
     * Same as {@link #addRange(String, double, double)} but the key will be
     * automatically generated based on <code>from</code> and <code>to</code>.
     */
    public DateRangeAggregationBuilder addRange(double from, double to) {
        return addRange(null, from, to);
    }

    /**
     * Add a new range with no lower bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param to
     *            the upper bound on the dates, exclusive
     */
    public DateRangeAggregationBuilder addUnboundedTo(String key, double to) {
        addRange(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, double)} but the key will be
     * computed automatically.
     */
    public DateRangeAggregationBuilder addUnboundedTo(double to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Add a new range with no upper bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     */
    public DateRangeAggregationBuilder addUnboundedFrom(String key, double from) {
        addRange(new Range(key, from, null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, double)} but the key will be
     * computed automatically.
     */
    public DateRangeAggregationBuilder addUnboundedFrom(double from) {
        return addUnboundedFrom(null, from);
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the dates, inclusive
     * @param to
     *            the upper bound on the dates, exclusive
     */
    public DateRangeAggregationBuilder addRange(String key, DateTime from, DateTime to) {
        addRange(new Range(key, convertDateTime(from), convertDateTime(to)));
        return this;
    }

    private Double convertDateTime(DateTime dateTime) {
        if (dateTime == null) {
            return null;
        } else {
            return (double) dateTime.getMillis();
        }
    }

    /**
     * Same as {@link #addRange(String, DateTime, DateTime)} but the key will be
     * automatically generated based on <code>from</code> and <code>to</code>.
     */
    public DateRangeAggregationBuilder addRange(DateTime from, DateTime to) {
        return addRange(null, from, to);
    }

    /**
     * Add a new range with no lower bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param to
     *            the upper bound on the dates, exclusive
     */
    public DateRangeAggregationBuilder addUnboundedTo(String key, DateTime to) {
        addRange(new Range(key, null, convertDateTime(to)));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, DateTime)} but the key will be
     * computed automatically.
     */
    public DateRangeAggregationBuilder addUnboundedTo(DateTime to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Add a new range with no upper bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     */
    public DateRangeAggregationBuilder addUnboundedFrom(String key, DateTime from) {
        addRange(new Range(key, convertDateTime(from), null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, DateTime)} but the key will be
     * computed automatically.
     */
    public DateRangeAggregationBuilder addUnboundedFrom(DateTime from) {
        return addUnboundedFrom(null, from);
    }

    @Override
    protected DateRangeAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new DateRangeAggregatorFactory(name, type, config, ranges, keyed, rangeFactory, context, parent, subFactoriesBuilder,
                metaData);
    }
}
