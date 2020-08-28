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

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Map;

public class DateRangeAggregationBuilder extends AbstractRangeBuilder<DateRangeAggregationBuilder, RangeAggregator.Range> {
    public static final String NAME = "date_range";
    public static final ValuesSourceRegistry.RegistryKey<RangeAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        RangeAggregatorSupplier.class
    );

    public static final ObjectParser<DateRangeAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME,  DateRangeAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, true);
        PARSER.declareBoolean(DateRangeAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);

        PARSER.declareObjectArray((agg, ranges) -> {
            for (RangeAggregator.Range range : ranges) {
                agg.addRange(range);
            }
        }, (p, c) -> RangeAggregator.Range.PARSER.parse(p, null), RangeAggregator.RANGES_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        AbstractRangeAggregatorFactory.registerAggregators(builder, REGISTRY_KEY);
    }

    public DateRangeAggregationBuilder(String name) {
        super(name, InternalDateRange.FACTORY);
    }

    protected DateRangeAggregationBuilder(DateRangeAggregationBuilder clone,
                                          AggregatorFactories.Builder factoriesBuilder,
                                          Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new DateRangeAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public DateRangeAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalDateRange.FACTORY, RangeAggregator.Range::new);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.DATE;
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
        addRange(new RangeAggregator.Range(key, from, to));
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
        addRange(new RangeAggregator.Range(key, null, to));
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
        addRange(new RangeAggregator.Range(key, from, null));
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
        addRange(new RangeAggregator.Range(key, from, to));
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
        addRange(new RangeAggregator.Range(key, null, to));
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
        addRange(new RangeAggregator.Range(key, from, null));
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
    public DateRangeAggregationBuilder addRange(String key, ZonedDateTime from, ZonedDateTime to) {
        addRange(new RangeAggregator.Range(key, convertDateTime(from), convertDateTime(to)));
        return this;
    }

    private static Double convertDateTime(ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        } else {
            return (double) dateTime.toInstant().toEpochMilli();
        }
    }

    /**
     * Same as {@link #addRange(String, ZonedDateTime, ZonedDateTime)} but the key will be
     * automatically generated based on <code>from</code> and <code>to</code>.
     */
    public DateRangeAggregationBuilder addRange(ZonedDateTime from, ZonedDateTime to) {
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
    public DateRangeAggregationBuilder addUnboundedTo(String key, ZonedDateTime to) {
        addRange(new RangeAggregator.Range(key, null, convertDateTime(to)));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, ZonedDateTime)} but the key will be
     * computed automatically.
     */
    public DateRangeAggregationBuilder addUnboundedTo(ZonedDateTime to) {
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
    public DateRangeAggregationBuilder addUnboundedFrom(String key, ZonedDateTime from) {
        addRange(new RangeAggregator.Range(key, convertDateTime(from), null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, ZonedDateTime)} but the key will be
     * computed automatically.
     */
    public DateRangeAggregationBuilder addUnboundedFrom(ZonedDateTime from) {
        return addUnboundedFrom(null, from);
    }

    @Override
    protected DateRangeAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config,
                                                    AggregatorFactory parent,
                                                    AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        // We need to call processRanges here so they are parsed and we know whether `now` has been used before we make
        // the decision of whether to cache the request
        RangeAggregator.Range[] ranges = processRanges(range -> {
            DocValueFormat parser = config.format();
            assert parser != null;
            double from = range.getFrom();
            double to = range.getTo();
            String fromAsString = range.getFromAsString();
            String toAsString = range.getToAsString();
            if (fromAsString != null) {
                from = parser.parseDouble(fromAsString, false, queryShardContext::nowInMillis);
            } else if (Double.isFinite(from)) {
                // from/to provided as double should be converted to string and parsed regardless to support
                // different formats like `epoch_millis` vs. `epoch_second` with numeric input
                from = parser.parseDouble(Long.toString((long) from), false, queryShardContext::nowInMillis);
            }
            if (toAsString != null) {
                to = parser.parseDouble(toAsString, false, queryShardContext::nowInMillis);
            } else if (Double.isFinite(to)) {
                to = parser.parseDouble(Long.toString((long) to), false, queryShardContext::nowInMillis);
            }
            return new RangeAggregator.Range(range.getKey(), from, fromAsString, to, toAsString);
        });
        if (ranges.length == 0) {
            throw new IllegalArgumentException("No [ranges] specified for the [" + this.getName() + "] aggregation");
        }
        return new DateRangeAggregatorFactory(name, config, ranges, keyed, rangeFactory, queryShardContext, parent, subFactoriesBuilder,
                metadata);
    }
}
