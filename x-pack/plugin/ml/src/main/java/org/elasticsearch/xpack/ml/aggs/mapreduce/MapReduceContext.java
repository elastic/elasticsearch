/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.List;
import java.util.function.Function;

final class MapReduceContext implements Releasable {
    private final List<ValuesExtractor> extractors;
    private final Function<BigArrays, MapReducer> mapReduceSupplier;
    private final BigArrays bigArrays;
    private final LongObjectPagedHashMap<MapReducer> mapReducerByBucketOrdinal;
    private final boolean profiling;

    MapReduceContext(
        List<ValuesExtractor> extractors,
        Function<BigArrays, MapReducer> mapReduceSupplier,
        BigArrays bigArrays,
        boolean profiling
    ) {
        this.extractors = extractors;
        this.mapReduceSupplier = mapReduceSupplier;
        this.bigArrays = bigArrays;
        this.mapReducerByBucketOrdinal = new LongObjectPagedHashMap<>(1, bigArrays);
        this.profiling = profiling;
    }

    public MapReducer getMapReducer(long bucketOrd) {
        // TODO: are bucketOrdinals arbitrary long values or a counter (so we can use a list instead)???
        MapReducer mapReducer = mapReducerByBucketOrdinal.get(bucketOrd);
        if (mapReducer == null) {
            mapReducer = mapReduceSupplier.apply(bigArrays);
            mapReducerByBucketOrdinal.put(bucketOrd, mapReducer);
            mapReducer.mapInit();
        }

        return mapReducer;
    }

    public MapReducer getEmptyMapReducer() {
        return mapReduceSupplier.apply(bigArrays);
    }

    public List<ValuesExtractor> getExtractors() {
        return extractors;
    }

    public boolean profiling() {
        return profiling;
    }

    @Override
    public void close() {
        Releasables.close(mapReducerByBucketOrdinal);
    }
}
