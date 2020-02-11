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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class PercentilesAggregationBuilder extends AbstractPercentilesAggregationBuilder<PercentilesAggregationBuilder> {
    public static final String NAME = Percentiles.TYPE_NAME;

    private static final double[] DEFAULT_PERCENTS = new double[] { 1, 5, 25, 50, 75, 95, 99 };
    private static final ParseField PERCENTS_FIELD = new ParseField("percents");

    private static final ConstructingObjectParser<PercentilesAggregationBuilder, String> PARSER;
    static {
        PARSER = AbstractPercentilesAggregationBuilder.getParser(
            PercentilesAggregationBuilder.NAME,
            (name, values, percentileConfig) -> {
                if (values == null) {
                    values = DEFAULT_PERCENTS; // this is needed because Percentiles has a default, while Ranks does not
                } else {
                    values = validatePercentiles(values, name);
                }
                return new PercentilesAggregationBuilder(name, values, percentileConfig);
            },
            PERCENTS_FIELD);
    }

    public PercentilesAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, aggregationName);
    }

    public PercentilesAggregationBuilder(String name) {
        this(name, DEFAULT_PERCENTS, null);
    }

    public PercentilesAggregationBuilder(String name, double[] values, PercentilesConfig percentilesConfig) {
        super(name, values, percentilesConfig, PERCENTS_FIELD);
    }

    protected PercentilesAggregationBuilder(PercentilesAggregationBuilder clone,
                                            Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new PercentilesAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /**
     * Set the values to compute percentiles from.
     */
    public PercentilesAggregationBuilder percentiles(double... percents) {
        this.values = validatePercentiles(percents, name);
        return this;
    }

    private static double[] validatePercentiles(double[] percents, String aggName) {
        if (percents == null) {
            throw new IllegalArgumentException("[percents] must not be null: [" + aggName + "]");
        }
        if (percents.length == 0) {
            throw new IllegalArgumentException("[percents] must not be empty: [" + aggName + "]");
        }
        double[] sortedPercents = Arrays.copyOf(percents, percents.length);
        Arrays.sort(sortedPercents);
        for (double percent : sortedPercents) {
            if (percent < 0.0 || percent > 100.0) {
                throw new IllegalArgumentException("percent must be in [0,100], got [" + percent + "]: [" + aggName + "]");
            }
        }
        return sortedPercents;
    }

    /**
     * Get the values to compute percentiles from.
     */
    public double[] percentiles() {
        return values;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource> innerBuild(QueryShardContext queryShardContext,
                                                                    ValuesSourceConfig<ValuesSource> config,
                                                                    AggregatorFactory parent,
                                                                    Builder subFactoriesBuilder) throws IOException {
        return new PercentilesAggregatorFactory(name, config, values, configOrDefault(), keyed,
            queryShardContext, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
