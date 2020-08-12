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
package org.elasticsearch.search.aggregations.metrics.datasketches.hll;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * AggregationBuilder of HllSketch Aggregation
 */
public class HllSketchAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, HllSketchAggregationBuilder> {
    public static final String NAME = "hll_sketch";
    public static final ObjectParser<HllSketchAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        HllSketchAggregationBuilder::new
    );
    public static final ParseField DEFAULT_LGK_FIELD = new ParseField("default_lgk", new String[0]);
    private Integer defaultLgk = null;

    public HllSketchAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            this.defaultLgk = in.readInt();
        }
    }

    /**
     * constructor of HllSketchAggregationBuilder
     * @param clone source
     * @param factoriesBuilder AggregatorFactories.Builder of this AggregationBuilder
     * @param metaData metadata of this AggregationBuilder
     */
    public HllSketchAggregationBuilder(
        HllSketchAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metaData
    ) {
        super(clone, factoriesBuilder, metaData);
        this.defaultLgk = clone.defaultLgk;
    }

    public HllSketchAggregationBuilder(String name) {
        super(name);
    }

    /**
     * set new defaultLgk of this HllSketchAggregationBuilder
     * @param defaultLgk default lgK when any hll union construct
     * @return this HllSketchAggregationBuilder
     */
    public HllSketchAggregationBuilder defaultLgK(int defaultLgk) {
        if (defaultLgk < 4 || defaultLgk > 21) {
            throw new IllegalArgumentException(
                "[default_lgk] must be number from 4 to 21 (include). Found [" + defaultLgk + "] in [" + this.name + "]"
            );
        } else {
            this.defaultLgk = defaultLgk;
            return this;
        }
    }

    public Integer defaultLgK() {
        return this.defaultLgk;
    }

    @Override
    protected void innerWriteTo(StreamOutput streamOutput) throws IOException {
        boolean hasDefaultLgk = this.defaultLgk != null;
        streamOutput.writeBoolean(hasDefaultLgk);
        if (hasDefaultLgk) {
            streamOutput.writeInt(this.defaultLgk);
        }
    }

    /**
     * build an AggregatorFactory
     * @param queryShardContext context about query,index and shard
     * @param config config about value source
     * @param parent parent of this AggregatorFactory
     * @param subFactoriesBuilder sub factories of this AggregatorFactory
     * @return HllSketchAggregatorFactory
     * @throws IOException throws by HllSketchAggregatorFactory's super class
     */
    @Override
    protected HllSketchAggregatorFactory innerBuild(
        QueryShardContext queryShardContext,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new HllSketchAggregatorFactory(
            this.name,
            config,
            this.defaultLgk,
            queryShardContext,
            parent,
            subFactoriesBuilder,
            this.metadata
        );
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (this.defaultLgk != null) {
            builder.field(DEFAULT_LGK_FIELD.getPreferredName(), this.defaultLgk);
        }
        return builder;
    }

    /**
     * a hash code identified this HllSketchAggregationBuilder
     * @return hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(new Object[] { this.defaultLgk });
    }

    @Override
    public boolean equals(Object o) {
        HllSketchAggregationBuilder other = (HllSketchAggregationBuilder) o;
        return Objects.equals(this.defaultLgk, other.defaultLgk);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new HllSketchAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected boolean serializeTargetValueType(Version version) {
        return true;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, false, false);
        PARSER.declareInt(HllSketchAggregationBuilder::defaultLgK, DEFAULT_LGK_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        HllSketchAggregatorFactory.registerAggregators(builder);
    }
}
