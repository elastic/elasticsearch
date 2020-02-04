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
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PercentilesAggregationBuilder extends AbstractPercentilesAggregationBuilder<PercentilesAggregationBuilder> {
    public static final String NAME = Percentiles.TYPE_NAME;

    private static final double[] DEFAULT_PERCENTS = new double[] { 1, 5, 25, 50, 75, 95, 99 };
    private static final ParseField PERCENTS_FIELD = new ParseField("percents");

    private static final ConstructingObjectParser<PercentilesAggregationBuilder, String> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(PercentilesAggregationBuilder.NAME, false, (objects, name) -> {

            double[] values = ((List<Double>) objects[0]).stream().mapToDouble(Double::doubleValue).toArray();
            PercentilesMethod.Config percentilesConfig;

            if (objects[1] != null && objects[2] != null) {
                throw new IllegalStateException("Only one percentiles method should be declared.");
            } else if (objects[1] == null && objects[2] == null) {
                // Default is tdigest
                percentilesConfig = new PercentilesMethod.Config.TDigest();
            } else if (objects[1] != null) {
                percentilesConfig = (PercentilesMethod.Config) objects[1];
            } else {
                percentilesConfig = (PercentilesMethod.Config) objects[2];
            }

            return new PercentilesAggregationBuilder(name, values, percentilesConfig);
        });

        ValuesSourceParserHelper.declareAnyFields(PARSER, true, true);

        PARSER.declareDoubleArray(ConstructingObjectParser.constructorArg(), PERCENTS_FIELD);
        PARSER.declareBoolean(PercentilesAggregationBuilder::keyed, KEYED_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), PercentilesMethod.TDIGEST_PARSER,
            PercentilesMethod.TDIGEST.getParseField());
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), PercentilesMethod.HDR_PARSER,
            PercentilesMethod.HDR.getParseField());
    }

    public PercentilesAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, aggregationName);
    }

    public PercentilesAggregationBuilder(String name) {
        super(name, PERCENTS_FIELD);
        this.values = DEFAULT_PERCENTS;
    }

    public PercentilesAggregationBuilder(String name, double[] values, PercentilesMethod.Config percentilesConfig) {
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
        if (percents == null) {
            throw new IllegalArgumentException("[percents] must not be null: [" + name + "]");
        }
        if (percents.length == 0) {
            throw new IllegalArgumentException("[percents] must not be empty: [" + name + "]");
        }
        double[] sortedPercents = Arrays.copyOf(percents, percents.length);
        Arrays.sort(sortedPercents);
        this.values = sortedPercents;
        return this;
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
