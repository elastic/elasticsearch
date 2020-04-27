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
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

public class PercentileRanksAggregationBuilder extends AbstractPercentilesAggregationBuilder<PercentileRanksAggregationBuilder> {
    public static final String NAME = PercentileRanks.TYPE_NAME;
    private static final ParseField VALUES_FIELD = new ParseField("values");

    public static final ConstructingObjectParser<PercentileRanksAggregationBuilder, String> PARSER =
            AbstractPercentilesAggregationBuilder.createParser(
                PercentileRanksAggregationBuilder.NAME,
                PercentileRanksAggregationBuilder::new,
                PercentilesConfig.TDigest::new,
                VALUES_FIELD);

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        PercentileRanksAggregatorFactory.registerAggregators(builder);
    }

    public PercentileRanksAggregationBuilder(String name, double[] values) {
        this(name, values, null);
    }

    private PercentileRanksAggregationBuilder(String name, double[] values, PercentilesConfig percentilesConfig) {
        super(name, values, percentilesConfig, VALUES_FIELD);
    }

    public PercentileRanksAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    private PercentileRanksAggregationBuilder(PercentileRanksAggregationBuilder clone,
                                              AggregatorFactories.Builder factoriesBuilder,
                                              Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new PercentileRanksAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    /**
     * Get the values to compute percentiles from.
     */
    public double[] values() {
        return values;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                                     ValuesSourceConfig config,
                                                                     AggregatorFactory parent,
                                                                     AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new PercentileRanksAggregatorFactory(name, config, values, configOrDefault(), keyed, queryShardContext,
                    parent, subFactoriesBuilder, metadata);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
