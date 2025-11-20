/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.storedfields.PerFieldStoredFieldsFormat;

import java.io.Closeable;
import java.io.IOException;

public interface BloomFilter extends Closeable {
    /**
     * Tests whether the given term may exist in the specified field.
     *
     * @param field the field name to check
     * @param term the term to test for membership
     * @return true if term may be present, false if definitely absent
     */
    boolean mayContainTerm(String field, BytesRef term) throws IOException;

    boolean isFilterAvailable();

    @Nullable
    static BloomFilter maybeGetBloomFilterForField(String field, SegmentReadState state) throws IOException {
        var codec = state.segmentInfo.getCodec();
        StoredFieldsReader storedFieldsReader = codec.storedFieldsFormat()
            .fieldsReader(state.directory, state.segmentInfo, state.fieldInfos, state.context);

        boolean success = false;
        try {
            if (storedFieldsReader instanceof PerFieldStoredFieldsFormat.PerFieldStoredFieldsReader perFieldStoredFieldsReader) {
                StoredFieldsReader idStoredFieldsReader = perFieldStoredFieldsReader.getReaderForField(field);
                if (idStoredFieldsReader instanceof BloomFilter bloomFilter && bloomFilter.isFilterAvailable()) {
                    success = true;
                    // We need to close the PerFieldStoredFieldsFormatReader otherwise we'll leak the reader for other fields
                    return new BloomFilter() {
                        @Override
                        public boolean mayContainTerm(String field, BytesRef term) throws IOException {
                            return bloomFilter.mayContainTerm(field, term);
                        }

                        @Override
                        public boolean isFilterAvailable() {
                            return bloomFilter.isFilterAvailable();
                        }

                        @Override
                        public void close() throws IOException {
                            storedFieldsReader.close();
                        }
                    };
                }
            }
        } finally {
            if (success == false) {
                storedFieldsReader.close();
            }
        }
        return null;
    }
}
