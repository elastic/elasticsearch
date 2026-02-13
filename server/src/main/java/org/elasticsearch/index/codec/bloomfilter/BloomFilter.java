/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.Closeable;
import java.io.IOException;

public interface BloomFilter extends Closeable {
    BloomFilter NO_FILTER = new BloomFilter() {
        @Override
        public void close() {

        }

        @Override
        public boolean mayContainValue(String field, BytesRef term) {
            return true;
        }

        @Override
        public long sizeInBytes() {
            return 0;
        }
    };

    /**
     * Tests whether the given term may exist in the specified field.
     *
     * @param field the field name to check
     * @param term the term to test for membership
     * @return true if term may be present, false if definitely absent
     */
    boolean mayContainValue(String field, BytesRef term) throws IOException;

    /**
     * Returns the size in bytes of the bloom filter data on disk.
     */
    long sizeInBytes();

    static BloomFilter getBloomFilterForId(SegmentReadState state) throws IOException {
        var codec = state.segmentInfo.getCodec();
        final var docValuesProducer = codec.docValuesFormat().fieldsProducer(state);
        boolean success = false;
        try {
            var idFieldInfo = state.fieldInfos.fieldInfo(IdFieldMapper.NAME);
            assert idFieldInfo != null;

            var binaryDocValuesProducer = docValuesProducer.getBinary(idFieldInfo);
            if (binaryDocValuesProducer instanceof BloomFilter bloomFilter) {
                success = true;
                return new BloomFilter() {
                    @Override
                    public boolean mayContainValue(String field, BytesRef term) throws IOException {
                        return bloomFilter.mayContainValue(field, term);
                    }

                    @Override
                    public long sizeInBytes() {
                        return bloomFilter.sizeInBytes();
                    }

                    @Override
                    public void close() throws IOException {
                        docValuesProducer.close();
                    }
                };
            } else {
                docValuesProducer.close();
                return BloomFilter.NO_FILTER;
            }
        } finally {
            if (success == false) {
                docValuesProducer.close();
            }
        }
    }
}
