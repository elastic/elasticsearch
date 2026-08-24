/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

final class RoaringBitmapAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final RoaringBitmapAggregatorSupplier supplier;

    RoaringBitmapAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        RoaringBitmapAggregatorSupplier supplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.supplier = supplier;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new RoaringBitmapAggregator(name, null, InternalRoaringBitmap.BitmapFormat.UNMAPPED, context, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        if (config.getValuesSource() instanceof ValuesSource.Numeric == false) {
            throw new IllegalArgumentException("[roaring_bitmap] aggregation requires a numeric field");
        }
        ValuesSource.Numeric numeric = (ValuesSource.Numeric) config.getValuesSource();
        if (config.fieldType() instanceof NumberFieldMapper.NumberFieldType == false) {
            throw new IllegalArgumentException("[roaring_bitmap] aggregation requires a mapped [integer] or [long] field");
        }
        NumberFieldMapper.NumberFieldType numberFieldType = (NumberFieldMapper.NumberFieldType) config.fieldType();
        InternalRoaringBitmap.BitmapFormat width = switch (numberFieldType.numberType()) {
            case INTEGER -> InternalRoaringBitmap.BitmapFormat.INT;
            case LONG -> InternalRoaringBitmap.BitmapFormat.LONG;
            default -> throw new IllegalArgumentException(
                "[roaring_bitmap] aggregation is not supported on field ["
                    + numberFieldType.name()
                    + "] of type ["
                    + numberFieldType.typeName()
                    + "]; only [integer] and [long] fields are supported"
            );
        };
        return supplier.build(name, numeric, width, context, parent, metadata);
    }
}
