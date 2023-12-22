/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.exponentialhistogram.agg.ExponentialHistogramAggregationBuilder;
import org.elasticsearch.xpack.exponentialhistogram.agg.ExponentialHistogramAggregatorFactory;
import org.elasticsearch.xpack.exponentialhistogram.agg.ExponentialHistogramPercentilesAggregationBuilder;
import org.elasticsearch.xpack.exponentialhistogram.agg.ExponentialHistogramPercentilesAggregatorFactory;
import org.elasticsearch.xpack.exponentialhistogram.agg.InternalExponentialHistogram;
import org.elasticsearch.xpack.exponentialhistogram.agg.InternalExponentialHistogramPercentiles;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * <p>This plugin adds the mapping type <code>exponential_histogram</code>,
 * which holds a self-describing histogram of double-precision 64-bit IEEE 754
 * floating point numbers.</p>
 *
 * <p>Whereas the <code>histogram</code> mapping type requires producers and consumers of
 * the histogram values to agree upon an algorithm (HDR Histogram or T-Digest) and its
 * parameters, the <code>exponential_histogram</code> mapping type is intentionally more
 * restrictive, specifying the algorithm.
 * </p>
 *
 * <p>The <code>exponential_histogram</code> mapping type is in tech preview and is
 * intentionally undocumented.</p>
 */
public class ExponentialHistogramMapperPlugin extends Plugin implements MapperPlugin, SearchPlugin {
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(ExponentialHistogramFieldMapper.CONTENT_TYPE, ExponentialHistogramFieldMapper.PARSER);
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        List<SearchPlugin.AggregationSpec> specs = new ArrayList<>();
        specs.add(
            new SearchPlugin.AggregationSpec(
                ExponentialHistogramAggregationBuilder.NAME,
                ExponentialHistogramAggregationBuilder::new,
                ExponentialHistogramAggregationBuilder.PARSER
            ).addResultReader(InternalExponentialHistogram::new)
             .setAggregatorRegistrar(ExponentialHistogramAggregatorFactory::registerAggregators)
        );
        specs.add(
            new SearchPlugin.AggregationSpec(
                ExponentialHistogramPercentilesAggregationBuilder.NAME,
                ExponentialHistogramPercentilesAggregationBuilder::new,
                ExponentialHistogramPercentilesAggregationBuilder.PARSER
            ).addResultReader(InternalExponentialHistogramPercentiles::new)
                .setAggregatorRegistrar(ExponentialHistogramPercentilesAggregatorFactory::registerAggregators)
        );
        return List.copyOf(specs);
    }
}
