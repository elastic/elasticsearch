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
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.LongConsumer;

/**
 * Counts a columnar string field with one value a document a page at a time. A page comes back as a short dictionary and
 * an ordinal for each document, so each distinct value is hashed once per page, not once per document.
 *
 * <p>Lucene delivers matches as a stream of doc IDs when it can: a run of matching documents, or a window found in a bit
 * set. {@link #collect(DocIdStream)} reads each stream in batches of {@link #PAGE_DOCS}. Documents delivered one at a
 * time, and pages the column declines, go to the wrapped per-document collector.
 *
 * <p>Only for one owning bucket, no sub-aggregations and no {@code _doc_count}, which a page does not account for.
 */
final class ColumnarPageTermsCollector extends LeafBucketCollector {

    /** Where the counted terms go, and what reading a page is charged to. */
    interface Target {
        /** Adds {@code docCount} documents to the bucket of {@code term}. */
        void count(BytesRef term, long docCount);

        /** Charged before the reader's page storage grows. The reader belongs to one leaf; see {@link LeafBudget}. */
        PageBudget budget();

        /** The arrays a page is read through. */
        Scratch scratch();
    }

    /** The arrays a page is read through, held for the whole aggregation. Their growth is charged once, to the budget. */
    static final class Scratch {
        private final PageBudget budget;
        private int[] docs;
        private int[] ordinalCounts = new int[0];

        Scratch(PageBudget budget) {
            this.budget = budget;
        }

        /** A batch of doc IDs, {@link #PAGE_DOCS} long. */
        int[] docs() {
            if (docs == null) {
                budget.charge((long) PAGE_DOCS * Integer.BYTES);
                docs = new int[PAGE_DOCS];
            }
            return docs;
        }

        /** The count of each ordinal of a page's dictionary, with room for {@code dictionarySize} of them. */
        int[] ordinalCounts(int dictionarySize) {
            if (ordinalCounts.length < dictionarySize) {
                final int capacity = ArrayUtil.oversize(dictionarySize, Integer.BYTES);
                budget.charge((long) (capacity - ordinalCounts.length) * Integer.BYTES);
                ordinalCounts = new int[capacity];
            }
            return ordinalCounts;
        }
    }

    /**
     * What the current leaf's reader has charged. The reader is garbage when its leaf ends, but the aggregation keeps the
     * charge until it closes, so the owner calls {@link #release} when the next leaf starts.
     */
    static final class LeafBudget implements PageBudget {
        private final LongConsumer charge;
        private long charged;

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

    /** Doc IDs read as one page. */
    static final int PAGE_DOCS = 4096;

    private final StringColumnReader reader;
    private final Target target;
    /** Counts documents not delivered as a stream, and pages the column declines. */
    private final LeafBucketCollector perDocument;
    private final PageSink sink = new PageSink();
    /** Pages read so far, by the shape the column chose, for tests. */
    int ordinalPages;
    int flatPages;

    ColumnarPageTermsCollector(StringColumnReader reader, Target target, LeafBucketCollector perDocument) {
        assert reader.hasValueAddresses() == false : "only a column with one value a document is read as pages";
        this.reader = reader;
        this.target = target;
        this.perDocument = perDocument;
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
        final int[] docs = target.scratch().docs();
        // Zero means the stream is empty. Its doc IDs arrive in order, which the column needs.
        for (int count = stream.intoArray(docs); count > 0; count = stream.intoArray(docs)) {
            // A page the column declines is read a document at a time.
            if (reader.readBlock(docs, 0, count, sink, target.budget()) == false) {
                for (int i = 0; i < count; i++) {
                    perDocument.collect(docs[i], 0);
                }
            }
        }
    }

    /** Counts a page in whichever shape the column chose. A document holds at most one value. */
    private final class PageSink implements StringBlockSink {
        /** A page that repeats too little for a dictionary, counted a value at a time. */
        private final Values flat = new Values() {
            @Override
            public void append(BytesRef value) {
                target.count(value, 1);
            }

            @Override
            public void finish() {}

            @Override
            public void close() {}
        };

        @Override
        public void appendOrdinals(
            int[] ordinals,
            int valueCount,
            int[] valueCounts,
            int docCount,
            BytesRef[] dictionary,
            int dictionarySize
        ) {
            assert valueCount <= docCount : "a column with one value a document";
            ordinalPages++;
            final int[] ordinalCounts = target.scratch().ordinalCounts(dictionarySize);
            Arrays.fill(ordinalCounts, 0, dictionarySize, 0);
            for (int i = 0; i < valueCount; i++) {
                ordinalCounts[ordinals[i]]++;
            }
            for (int ordinal = 0; ordinal < dictionarySize; ordinal++) {
                if (ordinalCounts[ordinal] > 0) {
                    target.count(dictionary[ordinal], ordinalCounts[ordinal]);
                }
            }
        }

        @Override
        public Values values(int valueCount, int[] valueCounts, int docCount) {
            assert valueCount <= docCount : "a column with one value a document";
            flatPages++;
            return flat;
        }
    }
}
