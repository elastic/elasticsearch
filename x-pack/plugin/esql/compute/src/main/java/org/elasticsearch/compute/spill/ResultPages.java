/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The pages of a query's result: in memory, or in one page run once they did not fit the result budget. A response
 * holds one of these instead of a list, walks it through cursors that decode pages on demand and may be opened again,
 * and releases it when it closes, which is when the run goes. The per-column facts a renderer needs, whether every
 * value of a column is null and whether a column holds multivalues, are captured as the pages are collected, so that
 * no renderer walks the pages for them.
 */
public abstract class ResultPages implements Releasable {

    private static final Logger logger = LogManager.getLogger(ResultPages.class);

    /** Rows over every page. */
    public abstract long rows();

    public abstract int pageCount();

    /** Per column, whether every value is null; a copy. */
    public abstract boolean[] nullColumns();

    /** Per column, whether a value may hold several values; a copy. */
    public abstract boolean[] multivaluedColumns();

    /** Whether the pages are in a run rather than in memory. */
    public abstract boolean spilled();

    /**
     * The pages in memory, which stay this object's. A result in a run has none to give and throws: a caller that
     * needs the pages as a list is one that has not been adapted to walk a cursor.
     */
    public abstract List<Page> resident();

    /**
     * A cursor from the first page; several may be open at once, each with reads of its own.
     */
    public abstract Cursor open();

    /**
     * Walks the pages in order. A page handed out is the caller's to release.
     */
    public interface Cursor extends Releasable {
        boolean hasNext();

        /**
         * The next page, decoded on demand; the listener may complete inline, with a page in memory, or later, once a
         * read has landed. Null at the end.
         */
        void next(ActionListener<Page> listener);

        /**
         * The next page, waiting for a read if one is needed: for callers off the transport threads, tests and the
         * Java client's view of a response.
         */
        @Nullable
        default Page nextBlocking() {
            PlainActionFuture<Page> future = new PlainActionFuture<>();
            next(future);
            return future.actionGet();
        }
    }

    /**
     * Pages in memory, released with the result.
     */
    public static ResultPages resident(List<Page> pages) {
        return new Resident(pages);
    }

    /**
     * The facts a renderer needs of a result, captured page by page as they are collected.
     */
    public static final class ColumnFacts {
        @Nullable
        private boolean[] nullColumns;
        @Nullable
        private boolean[] multivalued;
        private long rows;
        private int pages;

        public void add(Page page) {
            int columns = page.getBlockCount();
            if (nullColumns == null) {
                nullColumns = new boolean[columns];
                multivalued = new boolean[columns];
                Arrays.fill(nullColumns, true);
            }
            for (int c = 0; c < columns; c++) {
                if (nullColumns[c] && page.getBlock(c).areAllValuesNull() == false) {
                    nullColumns[c] = false;
                }
                if (multivalued[c] == false && page.getBlock(c).mayHaveMultivaluedFields()) {
                    multivalued[c] = true;
                }
            }
            rows += page.getPositionCount();
            pages++;
        }

        public long rows() {
            return rows;
        }

        public int pages() {
            return pages;
        }

        public boolean[] nullColumns(int columns) {
            return nullColumns == null ? allNull(columns) : nullColumns.clone();
        }

        public boolean[] multivaluedColumns(int columns) {
            return multivalued == null ? new boolean[columns] : multivalued.clone();
        }

        private static boolean[] allNull(int columns) {
            boolean[] all = new boolean[columns];
            Arrays.fill(all, true);
            return all;
        }
    }

    private static final class Resident extends ResultPages {
        private final List<Page> pages;
        private final ColumnFacts facts = new ColumnFacts();
        private final int columns;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Resident(List<Page> pages) {
            this.pages = pages;
            this.columns = pages.isEmpty() ? 0 : pages.get(0).getBlockCount();
            for (Page page : pages) {
                facts.add(page);
            }
        }

        @Override
        public long rows() {
            return facts.rows();
        }

        @Override
        public int pageCount() {
            return pages.size();
        }

        @Override
        public boolean[] nullColumns() {
            return facts.nullColumns(columns);
        }

        @Override
        public boolean[] multivaluedColumns() {
            return facts.multivaluedColumns(columns);
        }

        @Override
        public boolean spilled() {
            return false;
        }

        @Override
        public List<Page> resident() {
            return pages;
        }

        @Override
        public Cursor open() {
            Iterator<Page> iterator = pages.iterator();
            return new Cursor() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public void next(ActionListener<Page> listener) {
                    if (iterator.hasNext() == false) {
                        listener.onResponse(null);
                        return;
                    }
                    // the result keeps the page; a renderer releases what it is handed, so it gets a reference of its own
                    Page page = iterator.next();
                    page.allowPassingToDifferentDriver();
                    listener.onResponse(page.shallowCopy());
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                Releasables.close(() -> Releasables.close(pages.stream().map(p -> (Releasable) p::releaseBlocks).toList()));
            }
        }

        @Override
        public String toString() {
            return "ResultPages[resident, pages=" + pages.size() + ", rows=" + facts.rows() + "]";
        }
    }

    /**
     * Pages in one run of the query's scope, decoded through the scope's block factory, so that a page handed out is
     * charged to the query until the renderer releases it. Each cursor opens a reader of its own, lazily, at its first
     * page. Closing deletes the run; the scope's close is the backstop for a deletion that fails.
     */
    public static final class Spilled extends ResultPages {
        private final FramedRun run;
        private final QueryScope scope;
        private final int columns;
        private final ColumnFacts facts;
        private final List<Cursor> cursors = new ArrayList<>();
        @Nullable
        private final Releasable onClose;
        private final AtomicBoolean closed = new AtomicBoolean();

        /**
         * @param onClose released after the run, the holder's lease on the scope
         */
        public Spilled(FramedRun run, QueryScope scope, int columns, ColumnFacts facts, @Nullable Releasable onClose) {
            this.run = run;
            this.scope = scope;
            this.columns = columns;
            this.facts = facts;
            this.onClose = onClose;
        }

        public FramedRun run() {
            return run;
        }

        @Override
        public long rows() {
            return facts.rows();
        }

        @Override
        public int pageCount() {
            return facts.pages();
        }

        @Override
        public boolean[] nullColumns() {
            return facts.nullColumns(columns);
        }

        @Override
        public boolean[] multivaluedColumns() {
            return facts.multivaluedColumns(columns);
        }

        @Override
        public boolean spilled() {
            return true;
        }

        @Override
        public List<Page> resident() {
            throw new IllegalStateException("the result's [" + facts.pages() + "] pages are in a run, not in memory");
        }

        @Override
        public Cursor open() {
            if (closed.get()) {
                throw new IllegalStateException("the result is closed");
            }
            RunCursor cursor = new RunCursor();
            synchronized (cursors) {
                cursors.add(cursor);
            }
            return cursor;
        }

        private final class RunCursor implements Cursor {
            @Nullable
            private FramedRunReader reader;
            @Nullable
            private PageRunReader pages;
            private int read;
            private final AtomicBoolean cursorClosed = new AtomicBoolean();

            @Override
            public boolean hasNext() {
                return read < facts.pages();
            }

            @Override
            public void next(ActionListener<Page> listener) {
                if (hasNext() == false) {
                    listener.onResponse(null);
                    return;
                }
                PageRunReader current;
                synchronized (this) {
                    if (cursorClosed.get()) {
                        listener.onFailure(new IllegalStateException("the cursor is closed"));
                        return;
                    }
                    current = pages;
                }
                if (current == null) {
                    FramedRunReader.open(
                        run.name(),
                        run.object(),
                        scope.rootView(),
                        FramedRunMaxima.DEFAULT,
                        ResultCollector.CHUNK_BYTES,
                        listener.delegateFailureAndWrap((l, opened) -> {
                            boolean keep;
                            synchronized (this) {
                                keep = cursorClosed.get() == false;
                                if (keep) {
                                    reader = opened;
                                    pages = new PageRunReader(opened, 0, scope.blockFactory(), scope.rootView());
                                }
                            }
                            if (keep == false) {
                                opened.close();
                                l.onFailure(new IllegalStateException("the cursor is closed"));
                                return;
                            }
                            readNext(l);
                        })
                    );
                    return;
                }
                readNext(listener);
            }

            private void readNext(ActionListener<Page> listener) {
                PageRunReader reader;
                synchronized (this) {
                    reader = pages;
                }
                if (reader == null) {
                    listener.onFailure(new IllegalStateException("the cursor is closed"));
                    return;
                }
                read++;
                reader.next(listener);
            }

            @Override
            public void close() {
                if (cursorClosed.compareAndSet(false, true) == false) {
                    return;
                }
                synchronized (this) {
                    Releasables.close(pages, reader);
                    pages = null;
                    reader = null;
                }
                synchronized (cursors) {
                    cursors.remove(this);
                }
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true) == false) {
                return;
            }
            List<Cursor> open;
            synchronized (cursors) {
                open = new ArrayList<>(cursors);
            }
            Releasables.close(open);
            // the lease after the run: the scope may close once the run is gone
            run.delete(
                ActionListener.releaseAfter(
                    ActionListener.wrap(
                        ignored -> {},
                        e -> logger.debug(
                            () -> Strings.format("result run [%s] could not be deleted; the scope's close removes it", run.name()),
                            e
                        )
                    ),
                    onClose == null ? () -> {} : onClose
                )
            );
        }

        @Override
        public String toString() {
            return "ResultPages[run=" + run.name() + ", pages=" + facts.pages() + ", rows=" + facts.rows() + "]";
        }
    }
}
