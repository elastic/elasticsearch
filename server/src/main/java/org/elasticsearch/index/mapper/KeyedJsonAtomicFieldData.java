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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalMap;
import org.elasticsearch.index.fielddata.ordinals.RebasedGlobalOrdinalMap;
import org.elasticsearch.index.fielddata.plain.AbstractAtomicOrdinalsFieldData;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;

/**
 * The atomic field data implementation for {@link JsonFieldMapper.KeyedJsonFieldType}.
 *
 * This class wraps the field data that is built directly on the keyed JSON field,
 * and filters out values whose prefix doesn't match the requested key.
 */
public class KeyedJsonAtomicFieldData implements AtomicOrdinalsFieldData {

    private final AtomicOrdinalsFieldData delegate;
    private final SortedSetDocValues ordinalsValues;
    private final GlobalOrdinalMap ordinalMap;

    public static KeyedJsonAtomicFieldData create(String key,
                                                  AtomicOrdinalsFieldData delegate,
                                                  boolean rebaseOrdinals) {
        BytesRef keyBytes = new BytesRef(key);
        SortedSetDocValues values = delegate.getOrdinalsValues();

        long minOrd, maxOrd;
        try {
            minOrd = findMinOrd(keyBytes, values);
            if (minOrd < 0) {
                return new KeyedJsonAtomicFieldData(delegate,
                    DocValues.emptySortedSet(), delegate.getOrdinalMap());
            } else {
                maxOrd = findMaxOrd(keyBytes, values);

                SortedSetDocValues ordinalsValues = new KeyedJsonDocValues(
                    keyBytes, values, minOrd, maxOrd, rebaseOrdinals);
                GlobalOrdinalMap ordinalMap = new RebasedGlobalOrdinalMap(
                    delegate.getOrdinalMap(), minOrd, maxOrd);
                return new KeyedJsonAtomicFieldData(delegate, ordinalsValues, ordinalMap);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private KeyedJsonAtomicFieldData(AtomicOrdinalsFieldData delegate,
                                     SortedSetDocValues ordinalsValues,
                                     GlobalOrdinalMap ordinalMap) {
        this.delegate = delegate;
        this.ordinalsValues = ordinalsValues;
        this.ordinalMap = ordinalMap;
    }

    @Override
    public long ramBytesUsed() {
        return delegate.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return delegate.getChildResources();
    }

    @Override
    public SortedSetDocValues getOrdinalsValues() {
      return ordinalsValues;
    }

    @Override
    public GlobalOrdinalMap getOrdinalMap() {
        return ordinalMap;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public ScriptDocValues<?> getScriptValues() {
        return AbstractAtomicOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION
            .apply(getOrdinalsValues());
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getOrdinalsValues());
    }

    /**
     * Performs a binary search to find the first term with 'key' as a prefix.
     */
    static long findMinOrd(BytesRef key, SortedSetDocValues delegate) throws IOException {
        long low = 0;
        long high = delegate.getValueCount() - 1;

        long result = -1;
        while (low <= high) {
            long mid = (low + high) >>> 1;
            final BytesRef term = delegate.lookupOrd(mid);
            int cmp = compare(key, term);

            if (cmp == 0) {
                result = mid;
                high = mid - 1;
            } else if (cmp < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return result;
    }

    /**
     * Performs a binary search to find the last term with 'key' as a prefix.
     */
    static long findMaxOrd(BytesRef key, SortedSetDocValues delegate) throws IOException {
        long low = 0;
        long high = delegate.getValueCount() - 1;

        long result = -1;
        while (low <= high) {
            long mid = (low + high) >>> 1;
            final BytesRef term = delegate.lookupOrd(mid);
            int cmp = compare(key, term);

            if (cmp == 0) {
                result = mid;
                low = mid + 1;
            } else if (cmp < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return result;
    }

    private static int compare(BytesRef key, BytesRef term) {
        BytesRef extractedKey = JsonFieldParser.extractKey(term);
        return key.compareTo(extractedKey);
    }

    private static class KeyedJsonDocValues extends AbstractSortedSetDocValues {

        private final BytesRef key;
        private final SortedSetDocValues delegate;

        /**
         * The first and last ordinals whose term has 'key' as a prefix. These
         * values must be non-negative (there is at least one matching term).
         */
        private final long minOrd;
        private final long maxOrd;
        private final boolean rebaseOrdinals;

        /**
         * We cache the first ordinal in a document to avoid unnecessary iterations
         * through the delegate doc values. If no ordinal is cached for the current
         * document, this value will be -1.
         */
        private long cachedNextOrd;

        private KeyedJsonDocValues(BytesRef key,
                                   SortedSetDocValues delegate,
                                   long minOrd,
                                   long maxOrd,
                                   boolean rebaseOrdinals) {
            assert minOrd >= 0 && maxOrd >= 0;
            this.key = key;
            this.delegate = delegate;
            this.minOrd = minOrd;
            this.maxOrd = maxOrd;
            this.rebaseOrdinals = rebaseOrdinals;
            this.cachedNextOrd = -1;
        }

        @Override
        public long getValueCount() {
            return maxOrd - minOrd + 1;
        }

        /**
         * Returns the (un-prefixed) term value for the requested ordinal.
         *
         * Note that this method can only be called on ordinals returned from {@link #nextOrd()}.
         */
        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            long delegateOrd = unmapOrd(ord);
            BytesRef keyedValue = delegate.lookupOrd(delegateOrd);

            int prefixLength = key.length + 1;
            int valueLength = keyedValue.length - prefixLength;
            return new BytesRef(keyedValue.bytes, prefixLength, valueLength);
        }

        @Override
        public long nextOrd() throws IOException {
            if (cachedNextOrd >= 0) {
                long nextOrd = cachedNextOrd;
                cachedNextOrd = -1;
                return mapOrd(nextOrd);
            }

            long ord = delegate.nextOrd();
            if (ord != NO_MORE_ORDS && ord <= maxOrd) {
                assert ord >= minOrd;
                return mapOrd(ord);
            } else {
                return NO_MORE_ORDS;
            }
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (delegate.advanceExact(target)) {
                while (true) {
                    long ord = delegate.nextOrd();
                    if (ord == NO_MORE_ORDS || ord > maxOrd) {
                        break;
                    }

                    if (ord >= minOrd) {
                        cachedNextOrd = ord;
                        return true;
                    }
                }
            }

            cachedNextOrd = -1;
            return false;
        }

        /**
         * Maps an ordinal from the delegate doc values into the filtered ordinal space.
         * If ordinals are rebased, the new ord will lie in the range [0, (maxOrd - minOrd)].
         */
        private long mapOrd(long ord) {
            assert minOrd <= ord && ord <= maxOrd;
            return rebaseOrdinals ? ord - minOrd : ord;
        }

        /**
         * Given a filtered ordinal, maps it into the delegate ordinal space.
         */
        private long unmapOrd(long ord) {
            long delegateOrd = rebaseOrdinals ? ord + minOrd : ord;
            if (delegateOrd < minOrd || delegateOrd > maxOrd) {
                throw new IllegalArgumentException("The provided ordinal [" + ord + "] is outside the valid range.");
            }
            return delegateOrd;
        }
    }
}
