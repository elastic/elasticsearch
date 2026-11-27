/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Set;

/**
 * A view of a segment in which the given fields read as they were before their first in-place update: a document carrying a captured
 * pre-image serves the companion value column in place of the live one (an empty capture meaning the field was absent), and every other
 * document serves the live column unchanged.
 * <p>
 * It wraps the reader handed to the source loader so synthetic recovery source reconstructs the original index operation with its own value
 * rather than the current one. The synthetic field loaders read by {@link NumericDocValues#advanceExact(int)} with non-decreasing doc ids,
 * so the overlay supports random access and rejects iteration.
 */
final class DocValuesUpdatePreImageLeafReader extends FilterLeafReader {

    private final Set<String> preImageFields;

    DocValuesUpdatePreImageLeafReader(LeafReader in, Set<String> preImageFields) {
        super(in);
        this.preImageFields = preImageFields;
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        if (preImageFields.contains(field) == false) {
            return in.getSortedNumericDocValues(field);
        }
        final SortedNumericDocValues live = in.getSortedNumericDocValues(field);
        final SortedNumericDocValues preImage = in.getSortedNumericDocValues(DocValuesUpdatePreImage.valueField(field));
        final NumericDocValues present = in.getNumericDocValues(DocValuesUpdatePreImage.presenceField(field));
        return new SortedNumericDocValues() {
            private SortedNumericDocValues current;

            @Override
            public boolean advanceExact(int target) throws IOException {
                if (present != null && present.advanceExact(target)) {
                    // A pre-image was captured: serve it, treating an empty capture as "the field had no value".
                    current = preImage;
                    return preImage != null && preImage.advanceExact(target);
                }
                current = live;
                return live != null && live.advanceExact(target);
            }

            @Override
            public int docValueCount() {
                return current.docValueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return current.nextValue();
            }

            @Override
            public long cost() {
                return live != null ? live.cost() : 0;
            }

            @Override
            public int docID() {
                return current == null ? -1 : current.docID();
            }

            @Override
            public int nextDoc() {
                throw new UnsupportedOperationException("pre-image overlay is read by advanceExact, not iteration");
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException("pre-image overlay is read by advanceExact, not iteration");
            }
        };
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        if (preImageFields.contains(field) == false) {
            return in.getBinaryDocValues(field);
        }
        final BinaryDocValues live = in.getBinaryDocValues(field);
        final BinaryDocValues preImage = in.getBinaryDocValues(DocValuesUpdatePreImage.valueField(field));
        final NumericDocValues present = in.getNumericDocValues(DocValuesUpdatePreImage.presenceField(field));
        return new BinaryDocValues() {
            private BinaryDocValues current;

            @Override
            public boolean advanceExact(int target) throws IOException {
                if (present != null && present.advanceExact(target)) {
                    current = preImage;
                    return preImage != null && preImage.advanceExact(target);
                }
                current = live;
                return live != null && live.advanceExact(target);
            }

            @Override
            public BytesRef binaryValue() throws IOException {
                return current.binaryValue();
            }

            @Override
            public long cost() {
                return live != null ? live.cost() : 0;
            }

            @Override
            public int docID() {
                return current == null ? -1 : current.docID();
            }

            @Override
            public int nextDoc() {
                throw new UnsupportedOperationException("pre-image overlay is read by advanceExact, not iteration");
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException("pre-image overlay is read by advanceExact, not iteration");
            }
        };
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        // The overlaid doc values differ from the wrapped reader, so this ephemeral view must not share its cache identity.
        return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }
}
