/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOSupplier;

import java.io.Closeable;
import java.io.IOException;

/**
 * Writes a field's column-iterator structure — an {@link IndexedDISI} for sparse columns — and returns
 * its {@link ColumnIteratorMetadata} from {@link #install}. For empty and dense columns nothing is written
 * and {@link #install} returns the appropriate sentinel immediately.
 *
 * <p>Usage: call {@link #open} to create the writer, then {@link #walk} or {@link #walkPresence} to drive
 * the walk (and, for sparse columns, build the staged bitset), then {@link #install} to append the staged
 * bytes to the data file and obtain the metadata.
 *
 * <p>Two walk variants serve two different use cases:
 * <ul>
 *   <li>{@link #walk}: folds the presence pass into a value pass — one cursor open drives both the bitset
 *       (for sparse) and a caller-supplied {@link Pass}. For dense columns the pass is driven without
 *       staging anything; for empty columns the pass is never called.</li>
 *   <li>{@link #walkPresence}: keeps the presence pass separate — no cursor is opened for dense or empty
 *       columns (nothing to write), and the caller opens a second cursor for values. Use when the value
 *       loop is not yet expressed as a {@link Pass}.</li>
 * </ul>
 *
 * <p>The private {@link PassDecorator} is the sole class that wraps a cursor to feed into
 * {@link IndexedDISI#writeBitSet}. Only {@code nextDoc()}, {@code docID()}, and {@code cost()} are
 * overridden there — <strong>NOT</strong> {@code intoBitSet}, {@code advance}, or {@code docIDRunEnd}.
 * {@code writeBitSet} drives the walk through the default {@code intoBitSet}, which loops on
 * {@code nextDoc()}; delegating {@code intoBitSet} to the cursor would advance it behind the pass's back
 * and silently drop documents from the pass.
 */
public final class ColumnIteratorWriter<C extends DocIdSetIterator> implements Closeable {

    /**
     * What one pass does with each document the cursor is positioned on.
     *
     * @param <C> the cursor type; positioned to the current document before {@code accept} is called
     */
    public interface Pass<C> {
        void accept(C cursor, int doc) throws IOException;
    }

    private final IOSupplier<C> cursors;
    private final int numDocsWithField;
    private final int maxDoc;
    private final Directory directory;
    private final IOContext context;
    private final String prefix;

    // Built during walk for the sparse case; null for dense/empty.
    private StagedBytes staged;
    private short jumpTableEntryCount;

    // Prevents a second walk and enforces that install follows a walk.
    private boolean walked;

    private ColumnIteratorWriter(
        IOSupplier<C> cursors,
        int numDocsWithField,
        int maxDoc,
        Directory directory,
        IOContext context,
        String prefix
    ) {
        this.cursors = cursors;
        this.numDocsWithField = numDocsWithField;
        this.maxDoc = maxDoc;
        this.directory = directory;
        this.context = context;
        this.prefix = prefix;
    }

    /**
     * Creates a writer for a column with {@code numDocsWithField} documents out of {@code maxDoc}.
     * No cursor is opened and nothing is written until {@link #walk} or {@link #walkPresence} is called.
     *
     * @param cursors          supplies a fresh forward cursor over the documents that have a value;
     *                         called at most once (and only when the column is non-empty) by each walk
     * @param numDocsWithField number of documents that have a value (the cardinality)
     * @param maxDoc           number of documents in the segment
     * @param directory        used for the temporary staged bitset (sparse columns only)
     * @param context          IO context for the temporary file
     * @param prefix           name prefix for the temporary file
     */
    public static <C extends DocIdSetIterator> ColumnIteratorWriter<C> open(
        IOSupplier<C> cursors,
        int numDocsWithField,
        int maxDoc,
        Directory directory,
        IOContext context,
        String prefix
    ) {
        return new ColumnIteratorWriter<>(cursors, numDocsWithField, maxDoc, directory, context, prefix);
    }

    /**
     * Drives the pass over every document that has a value, and stages the bitset for sparse columns.
     * A cursor is opened for every non-empty column: for dense columns it drives a plain loop; for sparse
     * columns it drives the bitset write via a private decorator. For empty columns the pass is never
     * called and no cursor is opened. Must be called at most once, before {@link #install}.
     */
    public void walk(Pass<C> pass) throws IOException {
        assert walked == false : "walk or walkPresence called more than once";
        walked = true;
        if (numDocsWithField == 0) {
            return;
        }
        final C cursor = cursors.get();
        if (numDocsWithField == maxDoc) {
            // Dense: plain loop, no bitset to stage.
            for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
                pass.accept(cursor, doc);
            }
        } else {
            // Sparse: stage the bitset and drive the pass through a decorator.
            staged = new StagedBytes(directory, context, prefix, "columnar-presence");
            jumpTableEntryCount = IndexedDISI.writeBitSet(
                new PassDecorator<>(cursor, pass),
                staged.output(),
                IndexedDISI.DEFAULT_DENSE_RANK_POWER
            );
        }
    }

    /**
     * Stages the bitset for sparse columns without running a pass. For dense and empty columns no cursor
     * is opened and nothing is staged. Use when the value loop is driven separately by the caller.
     * Must be called at most once, before {@link #install}.
     */
    public void walkPresence() throws IOException {
        assert walked == false : "walk or walkPresence called more than once";
        walked = true;
        if (numDocsWithField == 0 || numDocsWithField == maxDoc) {
            return;
        }
        staged = new StagedBytes(directory, context, prefix, "columnar-presence");
        jumpTableEntryCount = IndexedDISI.writeBitSet(cursors.get(), staged.output(), IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    /**
     * Appends the staged bitset to {@code data} and returns its {@link ColumnIteratorMetadata}, or returns
     * the empty/dense sentinel when no bytes were staged. Must be called after {@link #walk} or
     * {@link #walkPresence}, and only once.
     */
    public ColumnIteratorMetadata install(IndexOutput data) throws IOException {
        assert walked : "install called before walk or walkPresence";
        if (numDocsWithField == 0) {
            return ColumnIteratorMetadata.empty(maxDoc);
        }
        if (numDocsWithField == maxDoc) {
            return ColumnIteratorMetadata.dense(maxDoc);
        }
        final long stagedLength = staged.length();
        final long offset = staged.copyInto(data);
        return new ColumnIteratorMetadata(
            offset,
            stagedLength,
            jumpTableEntryCount,
            IndexedDISI.DEFAULT_DENSE_RANK_POWER,
            numDocsWithField,
            maxDoc
        );
    }

    @Override
    public void close() throws IOException {
        if (staged != null) {
            staged.close();
        }
    }

    /**
     * Decorates a cursor to call a {@link Pass} on every {@link #nextDoc()} so that writing the
     * {@link IndexedDISI} bitset and running the pass happen in the same walk over the cursor.
     *
     * <p>Only {@code nextDoc()}, {@code docID()}, and {@code cost()} are overridden here — see the
     * class-level Javadoc for why {@code intoBitSet} must not be overridden.
     */
    private static final class PassDecorator<C extends DocIdSetIterator> extends DocIdSetIterator {
        private final C cursor;
        private final Pass<C> pass;

        PassDecorator(C cursor, Pass<C> pass) {
            this.cursor = cursor;
            this.pass = pass;
        }

        @Override
        public int nextDoc() throws IOException {
            final int doc = cursor.nextDoc();
            if (doc != NO_MORE_DOCS) {
                pass.accept(cursor, doc);
            }
            return doc;
        }

        @Override
        public int docID() {
            return cursor.docID();
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException("writeBitSet never calls advance");
        }

        @Override
        public long cost() {
            return cursor.cost();
        }
    }
}
