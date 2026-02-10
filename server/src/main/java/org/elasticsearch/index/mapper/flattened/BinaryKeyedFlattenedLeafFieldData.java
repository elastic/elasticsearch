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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;

import java.io.IOException;
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

    private static final SortedBinaryDocValues EMPTY = new SortedBinaryDocValues() {
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

    private static int compare(BytesRef key, BytesRef term) {
        BytesRef extractedKey = FlattenedFieldParser.extractKey(term);
        return key.compareTo(extractedKey);
    }

    private static class KeyedFlattenedBinaryDocValues extends SortedBinaryDocValues {

        private final BytesRef key;
        private final SortedBinaryDocValues delegate;
        private int count;
        private int seen;

        private KeyedFlattenedBinaryDocValues(BytesRef key, SortedBinaryDocValues delegate) {
            this.key = key;
            this.delegate = delegate;
        }

        @Override
        public int docValueCount() {
            return count;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            this.seen = 0;

            if (delegate.advanceExact(target)) {
                int offset = -1;
                int count = 0;
                for (int i = 0; i < delegate.docValueCount(); i++) {
                    BytesRef value = delegate.nextValue();
                    int comparison = compare(key, value);
                    if (comparison == 0) {
                        if (offset < 0) {
                            offset = i;
                        }
                        count++;
                    } else if (comparison < 0) {
                        break;
                    }
                }
                this.count = count;
                if (count == 0) {
                    return false;
                }

                // It is a match, but still need to reset the iterator on the current doc and
                // iterate the delegate until at least offset has been seen.
                boolean advanced = delegate.advanceExact(target);
                assert advanced;

                for (int i = 0; i < offset; ++i) {
                    delegate.nextValue();
                }
                return true;
            }

            this.count = 0;
            return false;
        }

        @Override
        public BytesRef nextValue() throws IOException {
            if (seen >= count) {
                return null;
            }
            seen++;
            BytesRef keyedValue = delegate.nextValue();
            int prefixLength = key.length + 1;
            int valueLength = keyedValue.length - prefixLength;
            return new BytesRef(keyedValue.bytes, keyedValue.offset + prefixLength, valueLength);
        }
    }
}
