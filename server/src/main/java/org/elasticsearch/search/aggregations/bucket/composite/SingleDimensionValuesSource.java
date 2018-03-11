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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * A source that can record and compare values produced by documents.
 */
abstract class SingleDimensionValuesSource<T extends Comparable<T>> implements Releasable {
    protected final int size;
    protected final int reverseMul;
    protected T afterValue;
    @Nullable
    protected MappedFieldType fieldType;

    /**
     * Ctr
     *
     * @param fieldType The fieldType associated with the source.
     * @param size The number of values to record.
     * @param reverseMul -1 if the natural order ({@link SortOrder#ASC} should be reversed.
     */
    SingleDimensionValuesSource(@Nullable MappedFieldType fieldType, int size, int reverseMul) {
        this.fieldType = fieldType;
        this.size = size;
        this.reverseMul = reverseMul;
        this.afterValue = null;
    }

    /**
     * The type of this source.
     */
    abstract String type();

    /**
     * Copies the current value in <code>slot</code>.
     */
    abstract void copyCurrent(int slot);

    /**
     * Compares the value in <code>from</code> with the value in <code>to</code>.
     */
    abstract int compare(int from, int to);

    /**
     * Compares the current value with the value in <code>slot</code>.
     */
    abstract int compareCurrent(int slot);

    /**
     * Compares the current value with the after value set in this source.
     */
    abstract int compareCurrentWithAfter();

    /**
     * Sets the after value for this source. Values that compares smaller are filtered.
     */
    abstract void setAfter(Comparable<?> value);

    /**
     * Returns the after value set for this source.
     */
    T getAfter() {
        return afterValue;
    }

    /**
     * Transforms the value in <code>slot</code> to a {@link Comparable} object.
     */
    abstract T toComparable(int slot) throws IOException;

    /**
     * Gets the {@link LeafCollector} that will record the values of the visited documents.
     */
    abstract LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException;

    /**
     * Gets a {@link LeafCollector} that will produce the provided value for all visited documents.
     */
    abstract LeafBucketCollector getLeafCollector(Comparable<?> value,
                                                  LeafReaderContext context, LeafBucketCollector next) throws IOException;

    /**
     * Returns a {@link SortedDocsProducer} or null if this source cannot produce sorted docs.
     */
    abstract SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query);

    /**
     * Returns true if a {@link SortedDocsProducer} should be used to optimize the execution.
     */
    protected boolean checkIfSortedDocsIsApplicable(IndexReader reader, MappedFieldType fieldType) {
        if (fieldType == null ||
                fieldType.indexOptions() == IndexOptions.NONE ||
                // inverse of the natural order
                reverseMul == -1) {
            return false;
        }

        if (reader.hasDeletions() &&
                (reader.numDocs() == 0 || (double) reader.numDocs() / (double) reader.maxDoc() < 0.5)) {
            // do not use the index if it has more than 50% of deleted docs
            return false;
        }
        return true;
    }
}
