/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceParseHelper;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GeoLineAggregationBuilder extends MultiValuesSourceAggregationBuilder.LeafOnly<GeoLineAggregationBuilder> {

    static final ParseField POINT_FIELD = new ParseField("point");
    static final ParseField SORT_FIELD = new ParseField("sort");
    static final ParseField ORDER_FIELD = new ParseField("sort_order");
    static final ParseField INCLUDE_SORT_FIELD = new ParseField("include_sort");
    static final ParseField SIZE_FIELD = new ParseField("size");

    public static final String NAME = "geo_line";

    public static final ObjectParser<GeoLineAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        GeoLineAggregationBuilder::new
    );
    static {
        MultiValuesSourceParseHelper.declareCommon(PARSER, true, ValueType.NUMERIC);
        MultiValuesSourceParseHelper.declareField(POINT_FIELD.getPreferredName(), PARSER, true, false, false, false, false);
        MultiValuesSourceParseHelper.declareField(SORT_FIELD.getPreferredName(), PARSER, true, false, false, false, false);
        PARSER.declareString((builder, order) -> builder.sortOrder(SortOrder.fromString(order)), ORDER_FIELD);
        PARSER.declareBoolean(GeoLineAggregationBuilder::includeSort, INCLUDE_SORT_FIELD);
        PARSER.declareInt(GeoLineAggregationBuilder::size, SIZE_FIELD);
    }

    private boolean includeSort;
    private SortOrder sortOrder = SortOrder.ASC;
    private int size = MAX_PATH_SIZE;
    static final int MAX_PATH_SIZE = 10000;

    public static void registerUsage(ValuesSourceRegistry.Builder builder) {
        builder.registerUsage(NAME, CoreValuesSourceType.GEOPOINT);
    }

    public GeoLineAggregationBuilder(String name) {
        super(name);
    }

    private GeoLineAggregationBuilder(
        GeoLineAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metaData
    ) {
        super(clone, factoriesBuilder, metaData);
    }

    /**
     * Read from a stream.
     */
    public GeoLineAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        sortOrder = SortOrder.readFromStream(in);
        includeSort = in.readBoolean();
        size = in.readVInt();
    }

    public GeoLineAggregationBuilder includeSort(boolean includeSort) {
        this.includeSort = includeSort;
        return this;
    }

    public GeoLineAggregationBuilder sortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
        return this;
    }

    public GeoLineAggregationBuilder size(int size) {
        if (size <= 0 || size > MAX_PATH_SIZE) {
            throw new IllegalArgumentException("invalid [size] value [" + size + "] must be a positive integer <= " + MAX_PATH_SIZE);
        }
        this.size = size;
        return this;
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
    protected void innerWriteTo(StreamOutput out) throws IOException {
        sortOrder.writeTo(out);
        out.writeBoolean(includeSort);
        out.writeVInt(size);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected MultiValuesSourceAggregatorFactory innerBuild(
        AggregationContext aggregationContext,
        Map<String, ValuesSourceConfig> configs,
        Map<String, QueryBuilder> filters,
        DocValueFormat format,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        validateTimeSeriesConfigs(aggregationContext, configs);
        return new GeoLineAggregatorFactory(
            name,
            configs,
            format,
            aggregationContext,
            parent,
            subFactoriesBuilder,
            metadata,
            includeSort,
            sortOrder,
            size
        );
    }

    private void validateTimeSeriesConfigs(AggregationContext context, Map<String, ValuesSourceConfig> configs) {
        ValuesSourceConfig sourceConfig = configs.get(SORT_FIELD.getPreferredName());
        if (context.isInSortOrderExecutionRequired()) {
            if (sourceConfig == null) {
                var fieldConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName(DataStream.TIMESTAMP_FIELD_NAME).build();
                sourceConfig = ValuesSourceConfig.resolveUnregistered(
                    context,
                    null,
                    fieldConfig.getFieldName(),
                    fieldConfig.getScript(),
                    fieldConfig.getMissing(),
                    fieldConfig.getTimeZone(),
                    null,
                    defaultValueSourceType()
                );
                configs.put(SORT_FIELD.getPreferredName(), sourceConfig);
            } else if (sourceConfig.fieldContext().field().equals(DataStream.TIMESTAMP_FIELD_NAME) == false) {
                throw new IllegalArgumentException(
                    "invalid field ["
                        + SORT_FIELD.getPreferredName()
                        + "]='"
                        + sourceConfig.fieldContext().field()
                        + "' configured for time-series aggregations"
                );
            }
        } else if (sourceConfig == null) {
            throw new IllegalArgumentException(
                "missing field [" + SORT_FIELD.getPreferredName() + "] configured for geo_line aggregations"
            );
        }
    }

    /** only for tests */
    public GeoLineAggregationBuilder point(MultiValuesSourceFieldConfig pointConfig) {
        pointConfig = Objects.requireNonNull(pointConfig, "Configuration for field [" + POINT_FIELD + "] cannot be null");
        field(POINT_FIELD.getPreferredName(), pointConfig);
        return this;
    }

    /** only for tests */
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

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
