/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.PageBudget;
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.ValueDeduplicator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.LongConsumer;

/**
 * Counts a run of documents of a columnar string field a page at a time rather than a document at a time.
 *
 * <p>The per-document route decodes each document's value, copies its bytes and hashes them, which costs the same
 * however few distinct values the field has. A page of the column instead arrives as a short dictionary of the
 * page's distinct values and an int per document naming one. This collector counts the ints and then adds each
 * distinct value to the buckets once, so the bytes are hashed once per value per page and not once per document.
 *
 * <p>Only {@link #collect(DocIdStream)} does this. Lucene gives a collector its matches as a stream of doc IDs when it
 * can: a run of consecutive documents that all match arrives as one, through {@code collectRange}, and so does a window
 * of matches found in a bit set, which is what a segment with deleted documents or a partial filter produces. The stream
 * is consumed before the call returns, so nothing has to be remembered between calls: its doc IDs are copied into one
 * array the {@link Target} owns, {@link #PAGE_DOCS} at a time, and each batch is read as a page when it is large
 * enough to earn back what a page costs. Documents that arrive one at a time, and batches that are too small, go to the
 * ordinary collector this one wraps.
 *
 * <p>Only for a collection that has one owning bucket, no sub-aggregations and a document count of one for every
 * document, because those are what counting a page as a whole leaves out.
 */
final class ColumnarPageTermsCollector extends LeafBucketCollector {

    /** Where the counted terms go, and what reading a page is charged to. */
    interface Target {
        /** Adds {@code docCount} documents to the bucket of {@code term}, creating it when there is none. */
        void count(BytesRef term, long docCount);

        /** Charged before the reader's page storage grows. The reader belongs to one leaf, so see {@link LeafBudget}. */
        PageBudget budget();

        /**
         * The array a batch of doc IDs is written to before it is read, whose length is the size of a page. The aggregator
         * gives it {@link #PAGE_DOCS}. One is held for the whole aggregation, since it is only used between a call
         * starting and returning.
         */
        int[] scratchDocs();

        /**
         * The arrays a page of ordinals is counted in. One is held for the whole aggregation, like
         * {@link #scratchDocs()}, so that what it grows by is charged once and not again for each leaf.
         */
        SlotScratch slotScratch();
    }

    /**
     * The arrays a page of ordinals is counted in, with a slot for each distinct term of the page. They grow to the
     * largest dictionary a page has had, and what they grow by is charged to the budget. The old arrays are garbage once
     * replaced, so only the growth is charged.
     */
    static final class SlotScratch {
        private final PageBudget budget;
        private int[] counts = new int[0];
        private int[] lastDoc = new int[0];

        /** @param budget what the growth is charged to, which has to stay charged for as long as the arrays are held */
        SlotScratch(PageBudget budget) {
            this.budget = budget;
        }

        /** Makes room for {@code size} slots, charging the budget for the growth before the arrays are allocated. */
        void ensure(int size) {
            if (counts.length < size) {
                final int capacity = ArrayUtil.oversize(size, Integer.BYTES);
                budget.charge((long) (capacity - counts.length) * 2 * Integer.BYTES);
                counts = new int[capacity];
                lastDoc = new int[capacity];
            }
        }
    }

    /**
     * What the reader of the current leaf has charged. A reader's page storage is garbage once its leaf is done, but the
     * aggregation keeps what it was charged until it closes, so it would add up over the leaves of a shard. The owner
     * calls {@link #release} when the next leaf starts to return it.
     */
    static final class LeafBudget implements PageBudget {
        private final LongConsumer charge;
        private long charged;

        /** @param charge adds the bytes to what the aggregation is charged, or with a negative number returns them */
        LeafBudget(LongConsumer charge) {
            this.charge = charge;
        }

        @Override
        public void charge(long bytes) {
            charge.accept(bytes);
            charged += bytes;
        }

        /** Returns what this leaf was charged. */
        void release() {
            // Not a call for zero, which would go through the branch that can trip the breaker.
            if (charged != 0) {
                charge.accept(-charged);
                charged = 0;
            }
        }
    }

    /** Documents read as one page. A larger page repeats more, and holds more memory while it is read. */
    static final int PAGE_DOCS = 4096;

    /**
     * Fewest documents a batch needs to be read as a page when the column holds one value for each document. A page has
     * a cost of its own, for the dictionary and the sink, which a batch of few documents does not earn back over hashing
     * their values. In the benchmark, with a hundred distinct values, a window of about eighty matches was level and
     * one of about two hundred was 1.6 times faster.
     */
    static final int MIN_PAGE_DOCS_SINGLE_VALUED = 128;

    /**
     * As {@link #MIN_PAGE_DOCS_SINGLE_VALUED}, for a column where documents can hold several values or none, which a
     * page reads more slowly. In the benchmark a window of about two hundred matches was 16% to 23% slower and one of
     * about four hundred was level, so a window has to be larger than that.
     */
    static final int MIN_PAGE_DOCS_MULTI_VALUED = 512;

    private final StringColumnReader reader;
    private final Target target;
    /** Counts what is not a range, and a page the column declines. Built for the same leaf and the same buckets. */
    private final LeafBucketCollector perDocument;
    /** A batch with fewer documents than this is counted the ordinary way. */
    private final int minPageDocs;
    private final ValueDeduplicator duplicates = new ValueDeduplicator(SortableBinaryDocValues.ValueOrder.ARRAY);
    private final PageSink sink = new PageSink();
    /** How the pages read so far arrived, for a test to see which shape the column chose. */
    int ordinalPages;
    int flatPages;

    ColumnarPageTermsCollector(StringColumnReader reader, Target target, LeafBucketCollector perDocument) {
        this(reader, target, perDocument, reader.hasValueAddresses() ? MIN_PAGE_DOCS_MULTI_VALUED : MIN_PAGE_DOCS_SINGLE_VALUED);
    }

    /** As above, with the fewest documents a page is read for chosen by the caller, which a test uses to read any. */
    ColumnarPageTermsCollector(StringColumnReader reader, Target target, LeafBucketCollector perDocument, int minPageDocs) {
        this.reader = reader;
        this.target = target;
        this.perDocument = perDocument;
        this.minPageDocs = minPageDocs;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        perDocument.setScorer(scorer);
    }

    @Override
    public void collect(int doc, long owningBucketOrd) throws IOException {
        perDocument.collect(doc, owningBucketOrd);
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
        final int[] docs = target.scratchDocs();
        assert docs.length > 0 : "the scratch array sets the size of a page";
        // A count of zero says the stream has nothing left. Its documents arrive in order, which the column needs.
        for (int count = stream.intoArray(docs); count > 0; count = stream.intoArray(docs)) {
            // Too few documents to earn back the cost of a page, or a page the column declines: read them one at a
            // time as they always were.
            if (count < minPageDocs || reader.readBlock(docs, 0, count, sink, target.budget()) == false) {
                for (int i = 0; i < count; i++) {
                    perDocument.collect(docs[i], 0);
                }
            }
        }
    }

    /** Takes a page in whichever shape the column chose for it. */
    private final class PageSink implements StringBlockSink {
        private final FlatValues flat = new FlatValues();

        @Override
        public void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dictionary, int size) {
            ordinalPages++;
            final SlotScratch scratch = target.slotScratch();
            scratch.ensure(size);
            final int[] slotCounts = scratch.counts;
            final int[] slotLastDoc = scratch.lastDoc;
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
