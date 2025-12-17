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

import java.io.Closeable;
import java.io.IOException;

public interface BloomFilter extends Closeable {
    BloomFilter NO_FILTER = new BloomFilter() {
        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean mayContainTerm(String field, BytesRef term) throws IOException {
            return true;
        }
    };

    /**
     * Tests whether the given term may exist in the specified field.
     *
     * @param field the field name to check
     * @param term the term to test for membership
     * @return true if term may be present, false if definitely absent
     */
    boolean mayContainTerm(String field, BytesRef term) throws IOException;
}
