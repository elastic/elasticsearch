/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import static org.elasticsearch.search.aggregations.support.ValuesSource.Bytes;

/**
 * A specialized queue implementation for composite buckets
 */
final class CompositeValuesCollectorQueue {
    // the slot for the current candidate
    private static final int CANDIDATE_SLOT = Integer.MAX_VALUE;

    private final int maxSize;
    private final TreeMap<Integer, Integer> keys;
    private final CompositeValuesSource<?>[] arrays;
    private final int[] docCounts;
    private final SortedDocsProducer docsProducer;
    private boolean afterValueSet = false;

    /**
     * Ctr
     * @param reader The index reader.
     * @param query The query.
     * @param sources The list of {@link CompositeValuesSourceConfig} to build the composite buckets.
     * @param size The number of composite buckets to keep.
     */
    CompositeValuesCollectorQueue(IndexReader reader, Query query, CompositeValuesSourceConfig[] sources, int size) {
        this.maxSize = size;
        this.arrays = new CompositeValuesSource<?>[sources.length];
        this.docCounts = new int[size];
        this.docsProducer = SortedDocsProducer.createProducerOrNull(reader, sources[0]);
        for (int i = 0; i < sources.length; i++) {
            final int reverseMul = sources[i].reverseMul();
            if (sources[i].valuesSource() instanceof Bytes.WithOrdinals &&
                    reader instanceof DirectoryReader) {
                Bytes.WithOrdinals vs = (Bytes.WithOrdinals) sources[i].valuesSource();
                if (docsProducer != null && docsProducer.isApplicable(query)) {
                    // switch to a simple binary source because the number of visited documents
                    // should be low and global ordinals need one lookup per visited term.
                    arrays[i] = CompositeValuesSource.createBinary(vs::bytesValues, size, reverseMul);
                } else {
                    arrays[i] = CompositeValuesSource.createGlobalOrdinals(vs::globalOrdinalsValues, size, reverseMul);
                }
            } else if (sources[i].valuesSource() instanceof Bytes) {
                Bytes vs = (Bytes) sources[i].valuesSource();
                arrays[i] = CompositeValuesSource.createBinary(vs::bytesValues, size, reverseMul);
            } else if (sources[i].valuesSource() instanceof Numeric) {
                final Numeric vs = (Numeric) sources[i].valuesSource();
                if (vs.isFloatingPoint()) {
                    arrays[i] = CompositeValuesSource.createDouble(vs::doubleValues, size, reverseMul);
                } else {
                    arrays[i] = CompositeValuesSource.createLong(vs::longValues, sources[i].format(), size, reverseMul);
                }
            }
        }
        this.keys = new TreeMap<>(this::compare);
    }

    // for tests only
    CompositeValuesCollectorQueue(CompositeValuesSource<?>[] sources, SortedDocsProducer docsProducer, int size) {
        this.maxSize = size;
        this.arrays = sources;
        this.docCounts = new int[size];
        this.docsProducer = docsProducer;
        this.keys = new TreeMap<>(this::compare);
    }

    /**
     * The current size of the queue.
     */
    int size() {
        return keys.size();
    }

    /**
     * Whether the queue is full or not.
     */
    boolean isFull() {
        return keys.size() == maxSize;
    }

    /**
     * Return the {@link SortedDocsProducer} associated with this queue or null
     * if there is no producer for this source.
     */
    SortedDocsProducer getDocsProducer() {
        return docsProducer;
    }

    /**
     * Returns a sorted {@link Set} view of the slots contained in this queue.
     */
    Set<Integer> getSortedSlot() {
        return keys.keySet();
    }

    /**
     * Compares the current candidate with the values in the queue and returns
     * the slot if the candidate is already in the queue or -1 if the candidate is not present.
     */
    Integer compareCurrent() {
        return keys.get(CANDIDATE_SLOT);
    }

    /**
     * Returns the lowest value (exclusive) of the leading source.
     */
    Comparable<?> getLowerValueLeadSource() {
        return afterValueSet ? arrays[0].getAfter() : null;
    }

    /**
     * Returns the upper value (inclusive) of the leading source.
     */
    Comparable<?> getUpperValueLeadSource() throws IOException {
        return size() >= maxSize ? arrays[0].toComparable(keys.lastKey()) : null;
    }
    /**
     * Returns the document count in <code>slot</code>.
     */
    int getDocCount(int slot) {
        return docCounts[slot];
    }

    /**
     * Copies the current value in <code>slot</code>.
     */
    private void copyCurrent(int slot) {
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].copyCurrent(slot);
        }
        docCounts[slot] = 1;
    }

    /**
     * Compares the values in <code>slot1</code> with <code>slot2</code>.
     */
    int compare(int slot1, int slot2) {
        for (int i = 0; i < arrays.length; i++) {
            int cmp = (slot1 == CANDIDATE_SLOT) ? arrays[i].compareCurrent(slot2) :
                arrays[i].compare(slot1, slot2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Sets the after values for this comparator.
     */
    void setAfter(Comparable<?>[] values) {
        assert values.length == arrays.length;
        afterValueSet = true;
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].setAfter(values[i]);
        }
    }

    /**
     * Compares the after values with the values in <code>slot</code>.
     */
    private int compareCurrentWithAfter() {
        for (int i = 0; i < arrays.length; i++) {
            int cmp = arrays[i].compareCurrentWithAfter();
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Builds the {@link CompositeKey} for <code>slot</code>.
     */
    CompositeKey toCompositeKey(int slot) throws IOException {
        assert slot < maxSize;
        Comparable<?>[] values = new Comparable<?>[arrays.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = arrays[i].toComparable(slot);
        }
        return new CompositeKey(values);
    }

    /**
     * Creates the collector that will visit the composite buckets of the matching documents.
     * The provided collector <code>in</code> is called on each composite bucket.
     */
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector in) throws IOException {
        return getLeafCollector(null, context, in);
    }
    /**
     * Creates the collector that will visit the composite buckets of the matching documents.
     * If <code>forceLeadSourceValue</code> is not null, the leading source will use this value
     * for each document.
     * The provided collector <code>in</code> is called on each composite bucket.
     */
    LeafBucketCollector getLeafCollector(Comparable<?> forceLeadSourceValue,
                                         LeafReaderContext context, LeafBucketCollector in) throws IOException {
        int last = arrays.length - 1;
        LeafBucketCollector collector = in;
        while (last > 0) {
            collector = arrays[last--].getLeafCollector(context, collector);
        }
        if (forceLeadSourceValue != null) {
            collector = arrays[last].getLeafCollector(forceLeadSourceValue, context, collector);
        } else {
            collector = arrays[last].getLeafCollector(context, collector);
        }
        return collector;
    }

    /**
     * Check if the current candidate should be added in the queue.
     * @return The target slot of the candidate or -1 is the candidate is not competitive.
     */
    int addIfCompetitive() {
        // checks if the candidate key is competitive
        Integer topSlot = compareCurrent();
        if (topSlot != null) {
            // this key is already in the top N, skip it
            docCounts[topSlot] += 1;
            return topSlot;
        }
        if (afterValueSet && compareCurrentWithAfter() <= 0) {
            // this key is greater than the top value collected in the previous round, skip it
            return -1;
        }
        if (keys.size() >= maxSize) {
            // the tree map is full, check if the candidate key should be kept
            if (compare(CANDIDATE_SLOT, keys.lastKey()) > 0) {
                // the candidate key is not competitive, skip it
                return -1;
            }
        }

        // the candidate key is competitive
        final int newSlot;
        if (keys.size() >= maxSize) {
            // the tree map is full, we replace the last key with this candidate
            int slot = keys.pollLastEntry().getKey();
            // and we recycle the deleted slot
            newSlot = slot;
        } else {
            newSlot = keys.size();
            assert newSlot < maxSize;
        }
        // move the candidate key to its new slot
        copyCurrent(newSlot);
        keys.put(newSlot, newSlot);
        return newSlot;
    }
}
