/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceParseHelper;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GeoLineAggregationBuilder
    extends MultiValuesSourceAggregationBuilder.LeafOnly<GeoLineAggregationBuilder> {

    static final ParseField GEO_POINT_FIELD = new ParseField("geo_point");
    static final ParseField SORT_FIELD = new ParseField("sort");

    static final String NAME = "geo_line";

    private static final ObjectParser<GeoLineAggregationBuilder, String> PARSER =
        ObjectParser.fromBuilder(NAME, GeoLineAggregationBuilder::new);
    static {
        MultiValuesSourceParseHelper.declareCommon(PARSER, true, ValueType.NUMERIC);
        MultiValuesSourceParseHelper.declareField(GEO_POINT_FIELD.getPreferredName(), PARSER, true, false, false);
        MultiValuesSourceParseHelper.declareField(SORT_FIELD.getPreferredName(), PARSER, true, false, false);
    }

    public static void registerUsage(ValuesSourceRegistry.Builder builder) {
        builder.registerUsage(NAME, CoreValuesSourceType.GEOPOINT);
    }

    public GeoLineAggregationBuilder(String name) {
        super(name);
    }

    private GeoLineAggregationBuilder(GeoLineAggregationBuilder clone,
                                      AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
    }

    /**
     * Read from a stream.
     */
    public GeoLineAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new GeoLineAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected MultiValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                            Map<String, ValuesSourceConfig> configs,
                                                            Map<String, QueryBuilder> filters,
                                                            DocValueFormat format,
                                                            AggregatorFactory parent,
                                                            AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new GeoLineAggregatorFactory(name, configs, format, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    public GeoLineAggregationBuilder value(MultiValuesSourceFieldConfig valueConfig) {
        valueConfig = Objects.requireNonNull(valueConfig, "Configuration for field [" + GEO_POINT_FIELD + "] cannot be null");
        field(GEO_POINT_FIELD.getPreferredName(), valueConfig);
        return this;
    }

    public GeoLineAggregationBuilder sort(MultiValuesSourceFieldConfig sortConfig) {
        sortConfig = Objects.requireNonNull(sortConfig, "Configuration for field [" + SORT_FIELD + "] cannot be null");
        field(SORT_FIELD.getPreferredName(), sortConfig);
        return this;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
