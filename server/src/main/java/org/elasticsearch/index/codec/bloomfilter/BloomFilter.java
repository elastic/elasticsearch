/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public interface BloomFilter {
    BloomFilter NO_FILTER = new BloomFilter() {

        @Override
        public boolean mayContainValue(String field, BytesRef term) {
            return true;
        }

        @Override
        public long sizeInBytes() {
            return 0;
        }

        @Override
        public double saturation() {
            // This always returns true in #mayContainValue, so we can safely return 1.
            return 1;
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

    /**
     * Returns the saturation of the bloom filter as a value in [0, 1], i.e. the fraction of bits
     * that are set. Higher saturation means a higher false positive rate.
     */
    double saturation() throws IOException;

}
