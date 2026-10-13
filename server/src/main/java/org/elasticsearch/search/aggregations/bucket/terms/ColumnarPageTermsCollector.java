/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.PageBudget;
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.index.fielddata.ColumnarPayloadSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.ValueDeduplicator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.Arrays;

/**
 * Counts the values of a columnar string field a page at a time rather than a document at a time.
 *
 * <p>The per-document route decodes each document's value, copies its bytes and hashes them, which costs the same
 * however few distinct values the field has. A page of the column instead arrives as a short dictionary of the
 * page's distinct values and an int per document naming one. This collector counts the ints and then adds each
 * distinct value to the buckets once, so the bytes are hashed once per value per page and not once per document.
 *
 * <p>{@link #collect} only remembers the document. The pages are read when the buffer fills and by {@link #flush},
 * which the owner has to call before it reads any result: Lucene does not tell a collector that its leaf is done,
 * so the owner flushes when the next leaf starts and when collection ends.
 *
 * <p>Only for a collection that has one owning bucket, no sub-aggregations and a document count of one for every
 * document, because those are what counting a page as a whole leaves out.
 */
final class ColumnarPageTermsCollector extends LeafBucketCollector {

    /** Where the counted terms go, and what reading a page is charged to. */
    interface Target {
        /** Adds {@code docCount} documents to the bucket of {@code term}, creating it when there is none. */
        void count(BytesRef term, long docCount);

        /** Charged before the reader's page storage grows. */
        PageBudget budget();

        /** Reports a flushed run of documents, and whether it was read as pages or one document at a time. */
        void flushed(int docs, boolean asPages);
    }

    /** Documents buffered before a page is read. A larger page repeats more, and holds more memory while it waits. */
    static final int PAGE_DOCS = 4096;

    private final ColumnarPayloadSortableBinaryDocValues values;
    private final StringColumnReader reader;
    private final Target target;
    private final ValueDeduplicator duplicates;
    private final PageSink sink = new PageSink();
    private final int[] docs = new int[PAGE_DOCS];
    private int buffered;
    /** How the pages read so far arrived, for a test or a profile to see which shape the column chose. */
    int ordinalPages;
    int flatPages;

    /**
     * @param values the leaf's values, which must be backed by a column ({@link
     *               ColumnarPayloadSortableBinaryDocValues#columnSource()} is not null)
     */
    ColumnarPageTermsCollector(ColumnarPayloadSortableBinaryDocValues values, Target target) {
        this.values = values;
        this.reader = values.columnSource().reader();
        this.target = target;
        this.duplicates = new ValueDeduplicator(values);
    }

    @Override
    public void collect(int doc, long owningBucketOrd) throws IOException {
        assert owningBucketOrd == 0 : "pages are only counted for a collection with one owning bucket";
        docs[buffered++] = doc;
        if (buffered == docs.length) {
            flush();
        }
    }

    /** Counts the documents remembered so far. Documents arrive in order, as the column reads them. */
    void flush() throws IOException {
        if (buffered == 0) {
            return;
        }
        final int count = buffered;
        buffered = 0;
        if (reader.readBlock(docs, 0, count, sink, target.budget())) {
            target.flushed(count, true);
        } else {
            // The column declined the page, so the documents are read one at a time as they always were.
            for (int i = 0; i < count; i++) {
                collectOneDoc(docs[i]);
            }
            target.flushed(count, false);
        }
    }

    private void collectOneDoc(int doc) throws IOException {
        if (values.advanceExact(doc) == false) {
            return;
        }
        final int valueCount = values.docValueCount();
        duplicates.reset(valueCount);
        for (int i = 0; i < valueCount; i++) {
            final BytesRef value = values.nextValue();
            if (duplicates.seen(value) == false) {
                target.count(value, 1);
            }
        }
    }

    /** Takes a page in whichever shape the column chose for it. */
    private final class PageSink implements StringBlockSink {
        private int[] slotCounts = new int[0];
        private int[] slotLastDoc = new int[0];
        private final FlatValues flat = new FlatValues();

        @Override
        public void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dictionary, int size) {
            ordinalPages++;
            if (slotCounts.length < size) {
                final int capacity = ArrayUtil.oversize(size, Integer.BYTES);
                slotCounts = new int[capacity];
                slotLastDoc = new int[capacity];
            }
            Arrays.fill(slotCounts, 0, size, 0);
            if (valueCounts == null) {
                for (int i = 0; i < valueCount; i++) {
                    slotCounts[ordinals[i]]++;
                }
            } else {
                // A document counts once for each distinct value it holds, however often it holds it.
                Arrays.fill(slotLastDoc, 0, size, -1);
                int at = 0;
                for (int doc = 0; doc < docCount; doc++) {
                    final int held = valueCounts[doc];
                    for (int i = 0; i < held; i++) {
                        final int slot = ordinals[at++];
                        if (slotLastDoc[slot] != doc) {
                            slotLastDoc[slot] = doc;
                            slotCounts[slot]++;
                        }
                    }
                }
            }
            for (int slot = 0; slot < size; slot++) {
                if (slotCounts[slot] > 0) {
                    target.count(dictionary[slot], slotCounts[slot]);
                }
            }
        }

        @Override
        public Values values(int valueCount, int[] valueCounts, int docCount) {
            flatPages++;
            return flat.reset(valueCounts);
        }
    }

    /** A page that repeats too little for a dictionary, counted a value at a time. */
    private final class FlatValues implements StringBlockSink.Values {
        private int[] valueCounts;
        private int doc;
        private int left;
        private boolean deduplicating;

        FlatValues reset(int[] valueCounts) {
            this.valueCounts = valueCounts;
            this.doc = 0;
            this.left = 0;
            this.deduplicating = false;
            return this;
        }

        @Override
        public void append(BytesRef value) {
            if (valueCounts == null) {
                target.count(value, 1);
                return;
            }
            if (left == 0) {
                // The next document that holds a value; the ones before it hold none.
                while (valueCounts[doc] == 0) {
                    doc++;
                }
                left = valueCounts[doc++];
                deduplicating = left > 1;
                if (deduplicating) {
                    duplicates.reset(left);
                }
            }
            left--;
            if (deduplicating == false || duplicates.seen(value) == false) {
                target.count(value, 1);
            }
        }

        @Override
        public void finish() {}

        @Override
        public void close() {}
    }
}
