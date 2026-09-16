/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Builds a bitmap containing the distinct non-negative values of an {@code integer} or {@code long}
 * field across all matching documents. The response is the same portable bitmap format accepted by
 * {@link BitmapTermsQueryBuilder}, base64 encoded by XContent.
 */
public final class RoaringBitmapAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<RoaringBitmapAggregationBuilder> {

    public static final String NAME = "roaring_bitmap";

    static final TransportVersion ROARING_BITMAP_AGGREGATION_ADDED = TransportVersion.fromName("roaring_bitmap_aggregation_added");

    static final ValuesSourceRegistry.RegistryKey<RoaringBitmapAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        RoaringBitmapAggregatorSupplier.class
    );

    public static final ObjectParser<RoaringBitmapAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        RoaringBitmapAggregationBuilder::new
    );

    static {
        // Scripts cannot declare whether their result needs a 32- or 64-bit bitmap, so this aggregation
        // deliberately requires a mapped field.
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);
    }

    public RoaringBitmapAggregationBuilder(String name) {
        super(name);
    }

    public RoaringBitmapAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(REGISTRY_KEY, CoreValuesSourceType.NUMERIC, RoaringBitmapAggregator::new, true);
    }

    private RoaringBitmapAggregationBuilder(
        RoaringBitmapAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new RoaringBitmapAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) {
        // No state beyond the standard values-source fields.
    }

    @Override
    protected RoaringBitmapAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        RoaringBitmapAggregatorSupplier supplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new RoaringBitmapAggregatorFactory(name, config, context, parent, subFactoriesBuilder, metadata, supplier);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public boolean supportsSampling() {
        // A bitmap produced from a sample cannot be scaled into the full set of matching values.
        return false;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ROARING_BITMAP_AGGREGATION_ADDED;
    }
}
