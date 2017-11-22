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
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

import static org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import static org.elasticsearch.search.aggregations.support.ValuesSource.Bytes;
import static org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;

final class CompositeValuesComparator {
    private final int size;
    private final CompositeValuesSource<?, ?>[] arrays;
    private boolean topValueSet = false;

    /**
     *
     * @param sources The list of {@link CompositeValuesSourceConfig} to build the composite buckets.
     * @param size The number of composite buckets to keep.
     */
    CompositeValuesComparator(IndexReader reader, CompositeValuesSourceConfig[] sources, int size) {
        this.size = size;
        this.arrays = new CompositeValuesSource<?, ?>[sources.length];
        for (int i = 0; i < sources.length; i++) {
            final int reverseMul = sources[i].reverseMul();
            if (sources[i].valuesSource() instanceof WithOrdinals && reader instanceof DirectoryReader) {
                WithOrdinals vs = (WithOrdinals) sources[i].valuesSource();
                arrays[i] = CompositeValuesSource.wrapGlobalOrdinals(vs, size, reverseMul);
            } else if (sources[i].valuesSource() instanceof Bytes) {
                Bytes vs = (Bytes) sources[i].valuesSource();
                arrays[i] = CompositeValuesSource.wrapBinary(vs, size, reverseMul);
            } else if (sources[i].valuesSource() instanceof Numeric) {
                final Numeric vs = (Numeric) sources[i].valuesSource();
                if (vs.isFloatingPoint()) {
                    arrays[i] = CompositeValuesSource.wrapDouble(vs, size, reverseMul);
                } else {
                    arrays[i] = CompositeValuesSource.wrapLong(vs, size, reverseMul);
                }
            }
        }
    }

    /**
     * Moves the values in <code>slot1</code> to <code>slot2</code>.
     */
    void move(int slot1, int slot2) {
        assert slot1 < size && slot2 < size;
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].move(slot1, slot2);
        }
    }

    /**
     * Compares the values in <code>slot1</code> with <code>slot2</code>.
     */
    int compare(int slot1, int slot2) {
        assert slot1 < size && slot2 < size;
        for (int i = 0; i < arrays.length; i++) {
            int cmp = arrays[i].compare(slot1, slot2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Returns true if a top value has been set for this comparator.
     */
    boolean hasTop() {
        return topValueSet;
    }

    /**
     * Sets the top values for this comparator.
     */
    void setTop(Comparable<?>[] values) {
        assert values.length == arrays.length;
        topValueSet = true;
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].setTop(values[i]);
        }
    }

    /**
     * Compares the top values with the values in <code>slot</code>.
     */
    int compareTop(int slot) {
        assert slot < size;
        for (int i = 0; i < arrays.length; i++) {
            int cmp = arrays[i].compareTop(slot);
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
        assert slot < size;
        Comparable<?>[] values = new Comparable<?>[arrays.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = arrays[i].toComparable(slot);
        }
        return new CompositeKey(values);
    }

    /**
     * Gets the {@link LeafBucketCollector} that will record the composite buckets of the visited documents.
     */
    CompositeValuesSource.Collector getLeafCollector(LeafReaderContext context, CompositeValuesSource.Collector in) throws IOException {
        int last = arrays.length - 1;
        CompositeValuesSource.Collector next = arrays[last].getLeafCollector(context, in);
        for (int i = last - 1; i >= 0; i--) {
            next = arrays[i].getLeafCollector(context, next);
        }
        return next;
    }
}
