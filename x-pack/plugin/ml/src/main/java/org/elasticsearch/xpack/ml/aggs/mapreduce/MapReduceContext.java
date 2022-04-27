/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

final class MapReduceContext {
    private final List<ValuesExtractor> extractors;
    private final Supplier<MapReducer> mapReduceSupplier;
    private final Map<Long, MapReducer> mapReducerByBucketOrdinal = new HashMap<>();
    private final boolean profiling;

    MapReduceContext(List<ValuesExtractor> extractors, Supplier<MapReducer> mapReduceSupplier, boolean profiling) {
        this.extractors = extractors;
        this.mapReduceSupplier = mapReduceSupplier;
        this.profiling = profiling;
    }

    public MapReducer getMapReducer(long bucketOrd) {
        // TODO: are bucketOrdinals arbitrary long values or a counter (so we can use a list instead)???
        MapReducer mapReducer = mapReducerByBucketOrdinal.get(bucketOrd);
        if (mapReducer == null) {
            mapReducer = mapReduceSupplier.get();
            mapReducerByBucketOrdinal.put(bucketOrd, mapReducer);
            mapReducer.mapInit();
        }

        return mapReducer;
    }

    public Supplier<MapReducer> getMapReduceSupplier() {
        return mapReduceSupplier;
    }

    public List<ValuesExtractor> getExtractors() {
        return extractors;
    }

    public boolean profiling() {
        return profiling;
    }
}
