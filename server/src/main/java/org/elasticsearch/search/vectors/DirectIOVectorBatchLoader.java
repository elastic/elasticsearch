/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.set.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Bulk vector loader that performs optimized I/O operations to load multiple vectors
 * simultaneously using parallel random access when possible.
 */
public class DirectIOVectorBatchLoader {

    private static final int BATCH_PER_THREAD = 8;
    // TODO: hook into a dedicated thread pool or at least name the virtual threads
    private Executor vtExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public Map<Integer, float[]> loadSegmentVectors(int[] docIds, LeafReaderContext context, String field) throws IOException {
        return loadSegmentVectorsParallel(docIds, context, field);
    }

    private Map<Integer, float[]> loadSegmentVectorsParallel(int[] docIds, LeafReaderContext context, String field) throws IOException {
        FloatVectorValues vectorValues = context.reader().getFloatVectorValues(field);
        if (vectorValues == null) {
            throw new IllegalArgumentException("No float vector values found for field: " + field);
        }

        Map<Integer, Integer> docToOrdinal = buildDocToOrdinalMapping(vectorValues, docIds);
        List<List<Integer>> batches = createBatches(new ArrayList<>(docToOrdinal.keySet()), BATCH_PER_THREAD);

        List<CompletableFuture<Map<Integer, float[]>>> futures = new ArrayList<>();
        for (List<Integer> batch : batches) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return loadVectorBatch(vectorValues, batch, docToOrdinal);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to load vector batch", e);
                }
            }, vtExecutor));
        }

        Map<Integer, float[]> combinedResult = new HashMap<>();
        try {
            for (CompletableFuture<Map<Integer, float[]>> future : futures) {
                var results = future.get();
                combinedResult.putAll(results);
            }
        } catch (Exception e) {
            ExceptionsHelper.convertToElastic(e);
        }

        return combinedResult;
    }

    private Map<Integer, float[]> loadVectorBatch(
        FloatVectorValues vectorValues,
        List<Integer> docIdBatch,
        Map<Integer, Integer> docToOrdinal
    ) throws IOException {

        Map<Integer, float[]> batchResult = new HashMap<>();

        for (Integer docId : docIdBatch) {
            Integer ordinal = docToOrdinal.get(docId);
            if (ordinal != null) {
                // clone the vector since the reader reuses the array
                float[] vector = vectorValues.vectorValue(ordinal).clone();
                batchResult.put(docId, vector);
            }
        }

        return batchResult;
    }

    private Map<Integer, Integer> buildDocToOrdinalMapping(FloatVectorValues vectorValues, int[] targetDocIds) throws IOException {

        Map<Integer, Integer> docToOrdinal = new HashMap<>();

        Set<Integer> targetDocSet = Sets.newHashSetWithExpectedSize(targetDocIds.length);
        for (int docId : targetDocIds) {
            targetDocSet.add(docId);
        }

        KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
        for (int docId = iterator.nextDoc(); docId != KnnVectorValues.DocIndexIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
            if (targetDocSet.contains(docId)) {  // Only map docs we actually need
                docToOrdinal.put(docId, iterator.index());

                // Early termination when all target docs found
                if (docToOrdinal.size() == targetDocSet.size()) {
                    break;
                }
            }
        }

        return docToOrdinal;
    }

    private <T> List<List<T>> createBatches(List<T> items, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < items.size(); i += batchSize) {
            batches.add(items.subList(i, Math.min(i + batchSize, items.size())));
        }
        return batches;
    }

    /**
     * TODO: look into removing this method
     */
    public float[] loadSingleVector(int docId, LeafReaderContext context, String field) throws IOException {
        FloatVectorValues vectorValues = context.reader().getFloatVectorValues(field);
        if (vectorValues == null) {
            throw new IllegalArgumentException("No float vector values found for field: " + field);
        }

        KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
        for (int currentDoc = iterator.nextDoc(); currentDoc != KnnVectorValues.DocIndexIterator.NO_MORE_DOCS; currentDoc = iterator
            .nextDoc()) {
            if (currentDoc == docId) {
                float[] vector = vectorValues.vectorValue(iterator.index()).clone();
                return vector != null ? vector : null;
            }
        }

        throw new IllegalArgumentException("Document " + docId + " not found in vector values");
    }
}
