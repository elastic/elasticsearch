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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ExtendedStatsAggregationBuilder
        extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource.Numeric, ExtendedStatsAggregationBuilder> {
    public static final String NAME = "extended_stats";

    private static final ObjectParser<ExtendedStatsAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(ExtendedStatsAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareNumericFields(PARSER, true, true, false);
        PARSER.declareDouble(ExtendedStatsAggregationBuilder::sigma, ExtendedStatsAggregator.SIGMA_FIELD);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new ExtendedStatsAggregationBuilder(aggregationName), null);
    }

    private double sigma = 2.0;

    public ExtendedStatsAggregationBuilder(String name) {
        super(name, CoreValuesSourceType.NUMERIC, ValueType.NUMERIC);
    }

    protected ExtendedStatsAggregationBuilder(ExtendedStatsAggregationBuilder clone,
                                              Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.sigma = clone.sigma;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new ExtendedStatsAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /**
     * Read from a stream.
     */
    public ExtendedStatsAggregationBuilder(StreamInput in) throws IOException {
        super(in, CoreValuesSourceType.NUMERIC, ValueType.NUMERIC);
        sigma = in.readDouble();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(sigma);
    }

    public ExtendedStatsAggregationBuilder sigma(double sigma) {
        if (sigma < 0.0) {
            throw new IllegalArgumentException("[sigma] must be greater than or equal to 0. Found [" + sigma + "] in [" + name + "]");
        }
        this.sigma = sigma;
        return this;
    }

    public double sigma() {
        return sigma;
    }

    @Override
    protected ExtendedStatsAggregatorFactory innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig<Numeric> config,
                                                        AggregatorFactory parent, Builder subFactoriesBuilder) throws IOException {
        return new ExtendedStatsAggregatorFactory(name, config, sigma, queryShardContext, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ExtendedStatsAggregator.SIGMA_FIELD.getPreferredName(), sigma);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sigma);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ExtendedStatsAggregationBuilder other = (ExtendedStatsAggregationBuilder) obj;
        return Objects.equals(sigma, other.sigma);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
