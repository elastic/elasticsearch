/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.index;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * A per-document set of presorted byte[] values.
 * <p>
 * Per-Document values in a SortedDocValues are deduplicated, dereferenced,
 * and sorted into a dictionary of unique values. A pointer to the
 * dictionary value (ordinal) can be retrieved for each document. Ordinals
 * are dense and in increasing sorted order.
 *
 * @deprecated Use {@link SortedSetDocValues} instead.
 */
@Deprecated
public abstract class LegacySortedSetDocValues {

    /** Sole constructor. (For invocation by subclass
     * constructors, typically implicit.) */
    protected LegacySortedSetDocValues() {}

    /** When returned by {@link #nextOrd()} it means there are no more
     *  ordinals for the document.
     */
    public static final long NO_MORE_ORDS = -1;

    /**
     * Returns the next ordinal for the current document (previously
     * set by {@link #setDocument(int)}.
     * @return next ordinal for the document, or {@link #NO_MORE_ORDS}.
     *         ordinals are dense, start at 0, then increment by 1 for
     *         the next value in sorted order.
     */
    public abstract long nextOrd();

    /**
     * Retrieves the number of unique ords for the current document. This must always be greater than
     * zero. It is illegal to call this method after advanceExact(int) returned {@code false}.
     */
    public abstract int docValueCount();

    /**
     * Sets iteration to the specified docID
     * @param docID document ID
     */
    public abstract void setDocument(int docID);

    /** Retrieves the value for the specified ordinal. The returned
     * {@link BytesRef} may be re-used across calls to lookupOrd so make sure to
     * {@link BytesRef#deepCopyOf(BytesRef) copy it} if you want to keep it
     * around.
     * @param ord ordinal to lookup
     * @see #nextOrd
     */
    public abstract BytesRef lookupOrd(long ord);

    /**
     * Returns the number of unique values.
     * @return number of unique values in this SortedDocValues. This is
     *         also equivalent to one plus the maximum ordinal.
     */
    public abstract long getValueCount();

    /** If {@code key} exists, returns its ordinal, else
     *  returns {@code -insertionPoint-1}, like {@code
     *  Arrays.binarySearch}.
     *
     *  @param key Key to look up
     **/
    public long lookupTerm(BytesRef key) {
        long low = 0;
        long high = getValueCount() - 1;

        while (low <= high) {
            long mid = (low + high) >>> 1;
            final BytesRef term = lookupOrd(mid);
            int cmp = term.compareTo(key);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }

        return -(low + 1);  // key not found.
    }

    /**
     * Returns a {@link TermsEnum} over the values.
     * The enum supports {@link TermsEnum#ord()} and {@link TermsEnum#seekExact(long)}.
     */
    public TermsEnum termsEnum() throws IOException {
        throw new UnsupportedOperationException();
    }
}
