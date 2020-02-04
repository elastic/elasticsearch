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
import java.util.List;
import java.util.Map;

public class PercentileRanksAggregationBuilder extends AbstractPercentilesAggregationBuilder<PercentileRanksAggregationBuilder> {
    public static final String NAME = PercentileRanks.TYPE_NAME;

    private static final ParseField VALUES_FIELD = new ParseField("values");
    private static final ConstructingObjectParser<PercentileRanksAggregationBuilder, String> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(PercentileRanksAggregationBuilder.NAME, false, (objects, name) -> {

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

            return new PercentileRanksAggregationBuilder(name, values, percentilesConfig);
        });

        ValuesSourceParserHelper.declareAnyFields(PARSER, true, true);

        PARSER.declareDoubleArray(ConstructingObjectParser.constructorArg(), VALUES_FIELD);
        PARSER.declareBoolean(PercentileRanksAggregationBuilder::keyed, PercentilesAggregationBuilder.KEYED_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), PercentilesMethod.TDIGEST_PARSER,
            PercentilesMethod.TDIGEST.getParseField());
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), PercentilesMethod.HDR_PARSER,
            PercentilesMethod.HDR.getParseField());
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, aggregationName);
    }

    public PercentileRanksAggregationBuilder(String name, double[] values) {
        this(name, values, null);
    }

    private PercentileRanksAggregationBuilder(String name, double[] values, PercentilesMethod.Config percentilesConfig) {
        super(name, values, percentilesConfig, VALUES_FIELD);
    }

    public PercentileRanksAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    private PercentileRanksAggregationBuilder(PercentileRanksAggregationBuilder clone,
                                              Builder factoriesBuilder,
                                              Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new PercentileRanksAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /**
     * Get the values to compute percentiles from.
     */
    public double[] values() {
        return values;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource> innerBuild(QueryShardContext queryShardContext,
                                                                     ValuesSourceConfig<ValuesSource> config,
                                                                     AggregatorFactory parent,
                                                                     Builder subFactoriesBuilder) throws IOException {
        return new PercentileRanksAggregatorFactory(name, config, values, configOrDefault(), keyed, queryShardContext,
                    parent, subFactoriesBuilder, metaData);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
