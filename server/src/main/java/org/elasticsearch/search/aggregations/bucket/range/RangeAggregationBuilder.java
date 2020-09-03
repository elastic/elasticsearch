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
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Map;

public class RangeAggregationBuilder extends AbstractRangeBuilder<RangeAggregationBuilder, Range> {
    public static final String NAME = "range";
    public static final ValuesSourceRegistry.RegistryKey<RangeAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        RangeAggregatorSupplier.class
    );

    public static final ObjectParser<RangeAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, RangeAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareBoolean(RangeAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);

        PARSER.declareObjectArray((agg, ranges) -> {
            for (Range range : ranges) {
                agg.addRange(range);
            }
        }, (p, c) -> RangeAggregator.Range.PARSER.parse(p, null), RangeAggregator.RANGES_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        AbstractRangeAggregatorFactory.registerAggregators(builder, REGISTRY_KEY);
    }

    public RangeAggregationBuilder(String name) {
        super(name, InternalRange.FACTORY);
    }

    /**
     * Read from a stream.
     */
    public RangeAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalRange.FACTORY, Range::new);
    }

    protected RangeAggregationBuilder(RangeAggregationBuilder clone,
                                      AggregatorFactories.Builder factoriesBuilder,
                                      Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new RangeAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     * @param to
     *            the upper bound on the distances, exclusive
     */
    public RangeAggregationBuilder addRange(String key, double from, double to) {
        addRange(new Range(key, from, to));
        return this;
    }

    /**
     * Same as {@link #addRange(String, double, double)} but the key will be
     * automatically generated based on <code>from</code> and
     * <code>to</code>.
     */
    public RangeAggregationBuilder addRange(double from, double to) {
        return addRange(null, from, to);
    }

    /**
     * Add a new range with no lower bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param to
     *            the upper bound on the distances, exclusive
     */
    public RangeAggregationBuilder addUnboundedTo(String key, double to) {
        addRange(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, double)} but the key will be
     * computed automatically.
     */
    public RangeAggregationBuilder addUnboundedTo(double to) {
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
    public RangeAggregationBuilder addUnboundedFrom(String key, double from) {
        addRange(new Range(key, from, null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, double)} but the key will be
     * computed automatically.
     */
    public RangeAggregationBuilder addUnboundedFrom(double from) {
        return addUnboundedFrom(null, from);
    }

    @Override
    protected RangeAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config,
                                                AggregatorFactory parent,
                                                AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        // We need to call processRanges here so they are parsed before we make the decision of whether to cache the request
        Range[] ranges = processRanges(range -> {
            DocValueFormat parser = config.format();
            assert parser != null;
            Double from = range.from;
            Double to = range.to;
            if (range.fromAsStr != null) {
                from = parser.parseDouble(range.fromAsStr, false, queryShardContext::nowInMillis);
            }
            if (range.toAsStr != null) {
                to = parser.parseDouble(range.toAsStr, false, queryShardContext::nowInMillis);
            }
            return new Range(range.key, from, range.fromAsStr, to, range.toAsStr);
        });
        if (ranges.length == 0) {
            throw new IllegalArgumentException("No [ranges] specified for the [" + this.getName() + "] aggregation");
        }
        return new RangeAggregatorFactory(name, config, ranges, keyed, rangeFactory, queryShardContext, parent, subFactoriesBuilder,
                metadata);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }
}
