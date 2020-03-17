/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;

/**
 * The atomic field data implementation for {@link FlatObjectFieldMapper.KeyedFlatObjectFieldType}.
 *
 * This class wraps the field data that is built directly on the keyed flat object field,
 * and filters out values whose prefix doesn't match the requested key.
 *
 * In order to support all usage patterns, the delegate's ordinal values are shifted
 * to range from 0 to the number of total values.
 */
public class KeyedFlatObjectLeafFieldData implements LeafOrdinalsFieldData {

    private final String key;
    private final LeafOrdinalsFieldData delegate;

    KeyedFlatObjectLeafFieldData(String key,
                                 LeafOrdinalsFieldData delegate) {
        this.key = key;
        this.delegate = delegate;
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
        BytesRef keyBytes = new BytesRef(key);
        SortedSetDocValues values = delegate.getOrdinalsValues();

        long minOrd, maxOrd;
        try {
            minOrd = findMinOrd(keyBytes, values);
            if (minOrd < 0) {
                return DocValues.emptySortedSet();
            }
            maxOrd = findMaxOrd(keyBytes, values);
            assert maxOrd >= 0;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new KeyedJsonDocValues(keyBytes, values, minOrd, maxOrd);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public ScriptDocValues<?> getScriptValues() {
        return AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION
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
        BytesRef extractedKey = FlatObjectFieldParser.extractKey(term);
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

        /**
         * We cache the first ordinal in a document to avoid unnecessary iterations
         * through the delegate doc values. If no ordinal is cached for the current
         * document, this value will be -1.
         */
        private long cachedNextOrd;

        private KeyedJsonDocValues(BytesRef key,
                                   SortedSetDocValues delegate,
                                   long minOrd,
                                   long maxOrd) {
            assert minOrd >= 0 && maxOrd >= 0;
            this.key = key;
            this.delegate = delegate;
            this.minOrd = minOrd;
            this.maxOrd = maxOrd;
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
         * Maps an ordinal from the delegate doc values into the filtered ordinal space. The
         * ordinal is shifted to lie in the range [0, (maxOrd - minOrd)].
         */
        private long mapOrd(long ord) {
            assert minOrd <= ord && ord <= maxOrd;
            return ord - minOrd;
        }

        /**
         * Given a filtered ordinal in the range [0, (maxOrd - minOrd)], maps it into the
         * delegate ordinal space.
         */
        private long unmapOrd(long ord) {
            assert 0 <= ord && ord <= maxOrd - minOrd;
            return ord + minOrd;
        }
    }
}
