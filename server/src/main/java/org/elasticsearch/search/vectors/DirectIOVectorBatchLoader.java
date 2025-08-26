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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Bulk vector loader that performs optimized I/O operations to load multiple vectors
 * simultaneously using direct I/O and sector alignment when possible.
 */
public class DirectIOVectorBatchLoader {

    private static final int SECTOR_SIZE = 4096; // Default sector size for alignment

    /**
     * Loads vectors for multiple document IDs in a single bulk operation.
     */
    public Map<Integer, float[]> loadSegmentVectors(int[] docIds, LeafReaderContext context, String field) throws IOException {
        Map<Integer, float[]> vectorCache = new HashMap<>();

        // Get vector values for the field
        FloatVectorValues vectorValues = context.reader().getFloatVectorValues(field);
        if (vectorValues == null) {
            throw new IllegalArgumentException("No float vector values found for field: " + field);
        }

        // TODO: For now, use sequential access - future optimization can implement true bulk I/O
        KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();

        // Build a lookup of available documents
        Map<Integer, Integer> docToIndex = new HashMap<>();
        for (int docId = iterator.nextDoc(); docId != KnnVectorValues.DocIndexIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
            docToIndex.put(docId, iterator.index());
        }

        // Load vectors for requested documents
        for (int docId : docIds) {
            Integer vectorIndex = docToIndex.get(docId);
            if (vectorIndex != null) {
                float[] vector = vectorValues.vectorValue(vectorIndex);
                if (vector != null) {
                    vectorCache.put(docId, vector);
                }
            }
        }

        return vectorCache;
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
                float[] vector = vectorValues.vectorValue(iterator.index());
                return vector != null ? vector: null;
            }
        }

        throw new IllegalArgumentException("Document " + docId + " not found in vector values");
    }
}
