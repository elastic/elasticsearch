/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.util.List;

public final class MapReduceValueSourceRegistry {
    public static final String NAME = "mapreduce";

    @FunctionalInterface
    interface MapReduceValuesSupplier {
        ValuesExtractor build(ValuesSourceConfig config, int id);
    }

    static final ValuesSourceRegistry.RegistryKey<MapReduceValuesSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        MapReduceValuesSupplier.class
    );

    public static void registerMapReduceValueSource(ValuesSourceRegistry.Builder registry) {
        registry.register(
            REGISTRY_KEY,
            List.of(CoreValuesSourceType.KEYWORD, CoreValuesSourceType.IP),
            ValuesExtractor::buildBytesExtractor,
            false
        );

        registry.register(
            REGISTRY_KEY,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC, CoreValuesSourceType.BOOLEAN),
            ValuesExtractor::buildLongExtractor,
            false
        );
    }

    private MapReduceValueSourceRegistry() {}
}
