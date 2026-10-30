/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.apache.lucene.index.IndexFileNames;

import java.util.Set;

/**
 * How a cold search shard fills the cache for vector files ("cheap shards" prototype, see
 * {@code FastSearchActivationSettings}).
 * <p>
 * Vector search walks a graph (HNSW) or scans centroids: a long chain of dependent random reads inside one file. Small fills, which
 * suit postings, terms and doc values, turn that chain into as many object store round trips. Vector files are therefore filled with
 * ranges of at least {@code rangeSize} bytes, and the first cache miss on a vector file (or on a slice of it, e.g. the graph or the
 * quantized vectors of one field) prefetches the whole file or slice at once if it is at most {@code wholeFileOnMissMaxBytes} bytes, so
 * that the walk waits for one parallel fill instead of one fill per hop. Only files the query actually touches are fetched: raw vectors
 * that a quantized search never reads are never prefetched.
 *
 * @param rangeSize               minimum size of the ranges used to fill the cache for vector files, {@code 0} to not change it
 * @param wholeFileOnMissMaxBytes largest vector file or slice prefetched whole on its first cache miss, {@code 0} to disable
 */
public record VectorFillPolicy(long rangeSize, long wholeFileOnMissMaxBytes) {

    private static final Set<String> VECTOR_EXTENSIONS = Set.of("vec", "vex", "veq", "veb", "cenivf", "clivf");

    public static boolean isVectorFile(String fileName) {
        final String extension = IndexFileNames.getExtension(fileName);
        return extension != null && VECTOR_EXTENSIONS.contains(extension);
    }
}
