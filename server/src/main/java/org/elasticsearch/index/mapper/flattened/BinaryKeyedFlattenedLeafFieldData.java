/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * The atomic field data implementation for {@link FlattenedFieldMapper.KeyedFlattenedFieldType}.
 *
 * This class wraps the field data that is built directly on the keyed flattened field,
 * and filters out values whose prefix doesn't match the requested key.
 */
public final class BinaryKeyedFlattenedLeafFieldData implements LeafFieldData {

    private final String key;
    private final LeafFieldData delegate;
    private final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;

    private static final SortedBinaryDocValues EMPTY = new SortedBinaryDocValues(null) {
        @Override
        public boolean advanceExact(int doc) throws IOException {
            return false;
        }

        @Override
        public int docValueCount() {
            return 0;
        }

        @Override
        public BytesRef nextValue() throws IOException {
            return null;
        }
    };

    BinaryKeyedFlattenedLeafFieldData(
        String key,
        LeafFieldData delegate,
        ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory
    ) {
        this.key = key;
        this.delegate = delegate;
        this.toScriptFieldFactory = toScriptFieldFactory;
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
    public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return toScriptFieldFactory.getScriptFieldFactory(getBytesValues(), name);
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        return new KeyedFlattenedBinaryDocValues(new BytesRef(key), delegate.getBytesValues());
    }

    /**
     * Returns key-filtered view on the provided SortedBinaryDocValues, for use by block loaders.
     */
    static SortedBinaryDocValues getKeyFilteredSortedBinaryDocValues(MultiValuedSortedBinaryDocValues dv, String key) throws IOException {
        return new KeyedFlattenedBinaryDocValues(new BytesRef(key), dv);
    }

    private static int compare(BytesRef key, BytesRef term) {
        BytesRef extractedKey = FlattenedFieldParser.extractKey(term);
        return key.compareTo(extractedKey);
    }

    private static class KeyedFlattenedBinaryDocValues extends SortedBinaryDocValues {

        private final BytesRef key;
        private final SortedBinaryDocValues delegate;
        private BytesRefBuilder[] values = new BytesRefBuilder[] { new BytesRefBuilder() };
        private int count;
        private int index;

        private KeyedFlattenedBinaryDocValues(BytesRef key, SortedBinaryDocValues delegate) {
            super(delegate.docIdIterator());
            this.key = key;
            this.delegate = delegate;
        }

        @Override
        public int docValueCount() {
            return count;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            count = 0;
            index = 0;
            if (delegate.advanceExact(target) == false) {
                return false;
            }
            final int prefixLength = key.length + 1;
            for (int i = 0; i < delegate.docValueCount(); i++) {
                BytesRef keyedValue = delegate.nextValue();
                int comparison = compare(key, keyedValue);
                if (comparison == 0) {
                    grow(count + 1);
                    values[count].copyBytes(keyedValue.bytes, keyedValue.offset + prefixLength, keyedValue.length - prefixLength);
                    count++;
                } else if (comparison < 0) {
                    // Values are sorted by key; no later value can match.
                    break;
                }
            }
            return count > 0;
        }

        private void grow(int minSize) {
            if (values.length < minSize) {
                int oldLen = values.length;
                int newLen = ArrayUtil.oversize(minSize, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                values = Arrays.copyOf(values, newLen);
                for (int i = oldLen; i < newLen; i++) {
                    values[i] = new BytesRefBuilder();
                }
            }
        }

        @Override
        public BytesRef nextValue() {
            if (index >= count) {
                return null;
            }
            return values[index++].get();
        }
    }
}
