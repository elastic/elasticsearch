/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * Binary doc values over a {@code BYTE_ARRAY} column of external Parquet files, with the bytes of a value as they are in the file. For
 * a string column that is the UTF-8 of the string, which is what a single-valued {@code keyword} field of a columnar index reads as
 * its binary doc values. A row without a value ({@code OPTIONAL} column) is not a document of the iterator.
 *
 * <p>It also serves ES|QL a block of values at a time ({@link #tryRead}), which finds the page of a document once for all the
 * documents of that page, and copies their bytes into one array. A filter on an exact value or on a value that contains some bytes
 * ({@link #tryTermEqualIterator}, {@link #tryContainsIterator}) compares a page at a time and, for a column with a dictionary, a
 * dictionary entry at a time, see {@link BytesPageMatcher}.
 */
final class ParquetBinaryDocValues extends BinaryDocValues implements BlockLoader.OptionalColumnAtATimeReader {

    private final List<ColumnChunkReader.Block> blocks;
    private final ColumnChunkReader reader;
    private final BytesPageDecoder decoder;
    private final boolean required;
    private final int maxDoc;

    private int doc = -1;

    ParquetBinaryDocValues(List<ColumnChunkReader.Block> blocks, boolean required) {
        this.blocks = blocks;
        this.decoder = new BytesPageDecoder();
        this.required = required;
        this.reader = new ColumnChunkReader(blocks, required, decoder);
        this.maxDoc = reader.maxDoc();
    }

    @Override
    public BytesRef binaryValue() {
        return decoder.value(doc - reader.pageStartDoc());
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = DocIdSetIterator.NO_MORE_DOCS;
            return false;
        }
        reader.load(target);
        doc = target;
        return reader.present(target);
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        // an iterator that is exhausted stays exhausted, and doc + 1 would overflow
        return doc == DocIdSetIterator.NO_MORE_DOCS ? doc : advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        while (target < maxDoc) {
            reader.load(target);
            int pageEnd = reader.pageStartDoc() + reader.pageRows();
            for (; target < pageEnd; target++) {
                if (reader.present(target)) {
                    doc = target;
                    return doc;
                }
            }
        }
        doc = DocIdSetIterator.NO_MORE_DOCS;
        return doc;
    }

    @Override
    public long cost() {
        return maxDoc;
    }

    @Override
    public BlockLoader.Block tryRead(
        BlockLoader.BlockFactory factory,
        BlockLoader.Docs docs,
        int offset,
        boolean nullsFiltered,
        BlockDocValuesReader.ToDouble toDouble,
        boolean toInt,
        boolean binaryMultiValuedFormat
    ) throws IOException {
        int end = docs.count();
        // these are the values of a single valued field, which is not the format that a multi valued binary field has
        if (end - offset <= 0 || binaryMultiValuedFormat || toDouble != null || toInt || docs.mayContainDuplicates()) {
            return null;
        }
        if (required || nullsFiltered) {
            return readEveryDocument(factory, docs, offset, end, nullsFiltered);
        }
        return readWithMissingValues(factory, docs, offset, end);
    }

    /**
     * Every document has a value. The block takes one array with all the bytes and the offsets of the values in it, so the values of
     * the documents are copied into one growing array, a page at a time. Returns {@code null}, so that the caller reads one document at
     * a time, for documents that are not in ascending order or outside of the segment.
     */
    private BlockLoader.Block readEveryDocument(
        BlockLoader.BlockFactory factory,
        BlockLoader.Docs docs,
        int offset,
        int end,
        boolean nullsFiltered
    ) throws IOException {
        int count = end - offset;
        int[] offsets = new int[count + 1];
        byte[] bytes = new byte[Math.max(1024, Math.min(count, 1 << 16) * 32)];
        int used = 0;
        int previous = -1;
        int i = offset;
        while (i < end) {
            int doc = docs.get(i);
            if (doc <= previous || doc >= maxDoc) {
                return null;
            }
            reader.load(doc);
            int pageStart = reader.pageStartDoc();
            int pageEnd = pageStart + reader.pageRows();
            byte[] page = decoder.pageBytes();
            int last = doc - 1;
            for (; i < end; i++) {
                int d = docs.get(i);
                if (d >= pageEnd) {
                    break;
                }
                if (d <= last) {
                    return null;
                }
                if (required == false && nullsFiltered && reader.present(d) == false) {
                    return null; // the caller promised a value for every document
                }
                int row = d - pageStart;
                int length = decoder.valueLength(row);
                long needed = (long) used + length;
                if (needed > Integer.MAX_VALUE - 8) {
                    return null;
                }
                if (needed > bytes.length) {
                    bytes = Arrays.copyOf(bytes, (int) Math.max(needed, Math.min((long) bytes.length * 2, Integer.MAX_VALUE - 8)));
                }
                System.arraycopy(page, decoder.valueStart(row), bytes, used, length);
                offsets[i - offset] = used;
                used += length;
                last = d;
            }
            previous = last;
        }
        offsets[count] = used;
        try (BlockLoader.SingletonBytesRefBuilder builder = factory.singletonBytesRefs(count)) {
            // the builder keeps the arrays, so they must not be longer than the bytes
            builder.appendBytesRefs(used == bytes.length ? bytes : Arrays.copyOf(bytes, used), offsets);
            this.doc = previous;
            return builder.build();
        }
    }

    /** A column that may miss values and no promise from the caller: a builder with nulls, filled one value at a time from the pages. */
    private BlockLoader.Block readWithMissingValues(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, int end)
        throws IOException {
        try (BlockLoader.BytesRefBuilder builder = factory.bytesRefsFromDocValues(end - offset)) {
            int previous = -1;
            for (int i = offset; i < end; i++) {
                int doc = docs.get(i);
                if (doc <= previous || doc >= maxDoc) {
                    return null;
                }
                reader.load(doc);
                if (reader.present(doc)) {
                    builder.appendBytesRef(decoder.value(doc - reader.pageStartDoc()));
                } else {
                    builder.appendNull();
                }
                previous = doc;
            }
            this.doc = previous;
            return builder.build();
        }
    }

    @Override
    public DocIdSetIterator tryTermEqualIterator(BytesRef term) {
        return TwoPhaseIterator.asDocIdSetIterator(new PageMatches(new BytesPageMatcher(term, BytesPageMatcher.Mode.EQUALS)));
    }

    @Override
    public DocIdSetIterator tryContainsIterator(BytesRef containsTerm) {
        return TwoPhaseIterator.asDocIdSetIterator(new PageMatches(new BytesPageMatcher(containsTerm, BytesPageMatcher.Mode.CONTAINS)));
    }

    /**
     * PROTOTYPE, only for the proof of concept: the documents whose value satisfies a predicate, which is what a prefix, a range, an
     * in-set, an automaton or a regexp query asks. For a column with a dictionary the predicate runs once for each entry of the
     * dictionary of a chunk and not once for each document.
     */
    @Override
    public DocIdSetIterator tryMatchIterator(Predicate<BytesRef> matcher) {
        return TwoPhaseIterator.asDocIdSetIterator(new PageMatches(new BytesPageMatcher(matcher)));
    }

    /**
     * The documents that a {@link BytesPageMatcher} finds, as a two phase iterator: the approximation is every document, and
     * {@link #matches} looks at the page of the document, once for all of its documents. That keeps the cost of a slice of the segment
     * (a window of documents, as {@code DataPartitioning.DOC} makes) in proportion to the slice, because nothing reads ahead of the
     * document that the caller asks about. A page with no match, and the rows of a page before its next match, are jumped over by the
     * approximation, so documents that cannot match cost no call.
     *
     * <p>It has a reader of its own, so it does not move the position of the doc values that made it.
     */
    private final class PageMatches extends TwoPhaseIterator {

        private final BytesPageMatcher matcher;
        private final ColumnChunkReader pages;
        private final Candidates candidates;
        // the documents of the page that the matcher holds, which is none until the first call of matches
        private int pageStart = 0;
        private int pageEnd = 0;

        PageMatches(BytesPageMatcher matcher) {
            this(matcher, new Candidates());
        }

        private PageMatches(BytesPageMatcher matcher, Candidates candidates) {
            super(candidates);
            this.matcher = matcher;
            this.candidates = candidates;
            this.pages = new ColumnChunkReader(blocks, required, matcher);
        }

        @Override
        public boolean matches() throws IOException {
            int doc = candidates.doc;
            if (doc < pageStart || doc >= pageEnd) {
                pages.load(doc);
                pageStart = pages.pageStartDoc();
                pageEnd = pageStart + pages.pageRows();
            }
            int next = matcher.pageHasMatch() ? matcher.nextMatch(doc - pageStart) : -1;
            if (next == doc - pageStart) {
                return true;
            }
            // the next document that can match: in this page, or in the next one
            candidates.next = next < 0 ? pageEnd : pageStart + next;
            return false;
        }

        @Override
        public float matchCost() {
            return 10f;
        }
    }

    /** All the documents, except the ones that the matcher has said cannot match: those are the ones below {@code next}. */
    private final class Candidates extends DocIdSetIterator {

        int doc = -1;
        int next = 0;

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return doc == NO_MORE_DOCS ? doc : advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            int candidate = Math.max(target, next);
            doc = candidate < maxDoc ? candidate : NO_MORE_DOCS;
            return doc;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }
}
