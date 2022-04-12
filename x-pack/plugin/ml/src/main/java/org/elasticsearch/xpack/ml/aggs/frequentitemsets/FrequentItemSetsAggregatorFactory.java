/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FrequentItemSetsAggregatorFactory extends AggregatorFactory {

    private final List<MultiValuesSourceFieldConfig> fields;
    private final double minimumSupport;
    private final int minimumSetSize;
    private final int size;

    // experimental
    private final String algorithm;

    public FrequentItemSetsAggregatorFactory(
        String name,
        AggregationContext context,
        AggregatorFactory parent,
        Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        List<MultiValuesSourceFieldConfig> fields,
        double minimumSupport,
        int minimumSetSize,
        int size,
        String algorithm
    ) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.fields = fields;
        this.minimumSupport = minimumSupport;
        this.minimumSetSize = minimumSetSize;
        this.size = size;
        this.algorithm = algorithm;
    }

    @Override
    protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {

        List<ValuesSourceConfig> configs = new ArrayList<>(fields.size());
        for (MultiValuesSourceFieldConfig field : fields) {
            configs.add(
                ValuesSourceConfig.resolve(
                    context,
                    field.getUserValueTypeHint(),
                    field.getFieldName(),
                    field.getScript(),
                    field.getMissing(),
                    field.getTimeZone(),
                    field.getFormat(),
                    CoreValuesSourceType.KEYWORD
                )
            );

        }

        return new FrequentItemSetsAggregator(name, context, parent, metadata, configs, minimumSupport, minimumSetSize, size, algorithm);
    }

}
