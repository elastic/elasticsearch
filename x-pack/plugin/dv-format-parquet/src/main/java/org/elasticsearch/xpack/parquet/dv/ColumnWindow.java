/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetPositionalReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

/**
 * The decoded values of one column around the document that a doc values iterator reads last. A Lucene iterator moves one document at
 * a time, while the ES|QL reader decodes in batches, so the window is the bridge: it loads a batch, copies it into plain arrays and
 * releases the block.
 *
 * <p>There are two ways to load a batch, because the reader has two paths with different costs. A <b>forward scan</b> of a row group
 * decodes each page once, so it is the way to read consecutive documents, and also the way to hop forward several times in one row
 * group. A <b>positional read</b> asks for the rows by position, and costs about one pass over the column chunk per call, because it
 * keeps no decoder state; it is the way to read isolated documents and to step backward. The window picks the path from the access
 * pattern: a move forward that continues the window, that an open scan reaches, or that is the third load in a row from one row
 * group, scans; anything else reads by
 * position, in a window aligned to a multiple of the window size.
 *
 * <p>Not thread-safe: one window belongs to one iterator.
 */
abstract class ColumnWindow {

    private final ParquetPositionalReader reader;
    private final FileLayout layout;
    /** The name of the field, which keys the shared pages: two fields of one column can have other conversions. */
    private final String field;
    private final String column;
    private final long firstRow;
    private final int maxDoc;
    private final int windowSize;
    @Nullable
    private final ParquetDocValuesProducer owner;
    @Nullable
    private final DecodedPageCache cache;
    /** Builds the blocks that the reader returns for a positional read, which the window copies and releases at once. */
    private final BlockFactory blockFactory;

    /** The page that holds the document that the iterator is on, or {@code null} before the first load. */
    private DecodedPageCache.ColumnPage current;
    /** The row group of the last load, and how many loads in a row hit that group, to recognize an iterator that keeps coming back. */
    private int lastGroup = -1;
    private int sameGroupLoads;
    private int loads;

    /** The open forward scan: the row group, its pages, and the file row of the first row of the next page. */
    private int scanGroup = -1;
    private CloseableIterator<Page> pages;
    private long scanNextRow;
    /** The thread that opened the scan, to recognize the scans that a thread leaves behind when it moves to a new iterator. */
    private volatile Thread scanThread;

    ColumnWindow(
        ParquetPositionalReader reader,
        FileLayout layout,
        String field,
        String column,
        long firstRow,
        int maxDoc,
        int windowSize,
        @Nullable ParquetDocValuesProducer owner,
        @Nullable DecodedPageCache cache,
        BlockFactory blockFactory
    ) {
        this.reader = reader;
        this.layout = layout;
        this.field = field;
        this.column = column;
        this.firstRow = firstRow;
        this.maxDoc = maxDoc;
        this.windowSize = windowSize;
        this.owner = owner;
        this.cache = cache;
        this.blockFactory = blockFactory;
    }

    boolean ownedBy(ParquetDocValuesProducer producer) {
        return owner == producer;
    }

    /** Whether the window reads the same column of the same segment as the other one. */
    boolean readsSameColumnAs(ColumnWindow other) {
        return owner == other.owner && column.equals(other.column);
    }

    /**
     * Whether the thread opened the window's open scan. It takes no lock, because the caller is another window's load that holds that
     * window's lock, and a load holds its lock for the whole read: two windows that checked each other under their locks would wait
     * for each other.
     */
    boolean scanOpenedBy(Thread thread) {
        return scanThread == thread;
    }

    final int maxDoc() {
        return maxDoc;
    }

    /**
     * How many loads in a row from one row group an iterator makes by position before it scans the group. A positional read costs about
     * one pass over the column chunk (it decompresses the chunk and decodes only what it needs), and a scan decodes every page up to the
     * target. For a numeric column the chunk is cheap to decode, so a few isolated reads by position are cheaper than a scan. For a
     * column of strings the decompression of the chunk is the cost, so a scan wins from the second read, and a filter on several string
     * columns, which reads the later columns at the sparse documents that the earlier ones let through, must not pay a chunk for each.
     */
    protected abstract int scanAfterLoads();

    /** The page that holds the document that the iterator is on; it is valid after {@link #ensure}. */
    protected final DecodedPageCache.ColumnPage page() {
        return current;
    }

    /** Whether the document, which must be in the loaded page, has a value. */
    final boolean has(int doc) {
        return current.has(doc);
    }

    /** Loads the page that holds the document, unless it is loaded. */
    final void ensure(int doc) throws IOException {
        if (current != null && current.contains(doc)) {
            return;
        }
        if (cache != null) {
            DecodedPageCache.ColumnPage shared = cache.find(field, doc);
            if (shared != null) {
                // another iterator of the segment decoded this page
                current = shared;
                return;
            }
        }
        int end = current == null ? 0 : current.end();
        long row = firstRow + doc;
        int group = layout.groupOf(row);
        boolean forward = doc >= end;
        sameGroupLoads = group == lastGroup ? sameGroupLoads + 1 : 0;
        // A scan is the way for consecutive documents (a load after the first that starts where the page ends), for a document that an
        // open scan reaches without a restart, and for a row group that the iterator keeps coming back to. An isolated document is
        // cheaper by position, which decodes only what it needs. The first load of an iterator is always by position, whatever the
        // document: one document read for each of many columns (a LIMIT 1 over a wide file) would otherwise open a scan per column.
        boolean continues = loads > 0 && doc == end;
        boolean scan = forward && (continues || (group == scanGroup && row >= scanNextRow) || sameGroupLoads >= scanAfterLoads());
        loads++;
        // a failed load leaves the window without a page instead of half filled
        current = null;
        lastGroup = group;
        if (scan) {
            loadByScan(doc, group);
        } else {
            loadByPosition(doc);
        }
    }

    private void loadByPosition(int doc) throws IOException {
        int newStart = doc - doc % windowSize;
        int newEnd = Math.min(maxDoc, newStart + windowSize);
        int count = newEnd - newStart;
        long[] positions = new long[count];
        for (int i = 0; i < count; i++) {
            positions[i] = firstRow + newStart + i;
        }
        Block[] blocks = reader.extract(new String[] { column }, null, positions, blockFactory);
        try {
            current = load(blocks[0], 0, count, newStart);
        } finally {
            blocks[0].close();
        }
    }

    private synchronized void loadByScan(int doc, int group) throws IOException {
        long row = firstRow + doc;
        if (pages == null || scanGroup != group || row < scanNextRow) {
            closeScan();
            // free the budget that abandoned scans hold before asking for more
            ScanRegistry.INSTANCE.beforeOpen(this);
            scanThread = Thread.currentThread();
            // The scan starts at the document that the iterator asks for, not at the start of the row group: a slice of a segment
            // starts in the middle of a group, and the file has no column index, so a scan from the start of the group would decode
            // every page up to the slice. From the row, the reader skips the pages before it without decompressing them.
            pages = reader.scanRowGroup(column, group, row - layout.firstRowOf(group), windowSize);
            if (owner != null) {
                owner.scanOpened();
            }
            scanGroup = group;
            scanNextRow = row;
        }
        ScanRegistry.INSTANCE.touch(this);
        while (true) {
            if (pages.hasNext() == false) {
                closeScan();
                throw new IOException("row group [" + group + "] of the file ended before row [" + row + "]");
            }
            Page page = pages.next();
            long pageFirst = scanNextRow;
            int pageRows = page.getPositionCount();
            scanNextRow += pageRows;
            try {
                if (row < pageFirst + pageRows) {
                    // the documents of the segment in this page: the segment may serve only part of the file
                    long from = Math.max(pageFirst, firstRow);
                    long to = Math.min(pageFirst + pageRows, firstRow + maxDoc);
                    current = load(page.getBlock(0), (int) (from - pageFirst), (int) (to - from), (int) (from - firstRow));
                    if (cache != null) {
                        cache.put(field, current);
                    }
                    if (scanNextRow >= layout.endRowOf(group)) {
                        // the row group is read: hold nothing, because an iterator that reached the end of its group may never be called
                        // again, and an open scan keeps its share of the reader's byte budget
                        closeScan();
                    }
                    return;
                }
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /** Releases the open forward scan, if any. */
    final synchronized void closeScan() {
        if (pages != null) {
            try {
                pages.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                pages = null;
                scanThread = null;
                scanGroup = -1;
                ScanRegistry.INSTANCE.remove(this);
            }
        }
    }

    private DecodedPageCache.ColumnPage load(Block block, int blockOffset, int count, int startDoc) {
        boolean[] present = null;
        if (block.areAllValuesNull()) {
            present = new boolean[count];
        } else if (block.mayHaveNulls()) {
            present = new boolean[count];
            boolean all = true;
            for (int i = 0; i < count; i++) {
                present[i] = block.isNull(blockOffset + i) == false;
                all &= present[i];
            }
            if (all) {
                present = null;
            }
        }
        return decode(block, blockOffset, count, startDoc, present);
    }

    /**
     * Copies {@code count} positions of the block, starting at {@code blockOffset}, into a page that starts at the document
     * {@code startDoc}; {@code present} is {@code null} when every position has a value.
     */
    protected abstract DecodedPageCache.ColumnPage decode(
        Block block,
        int blockOffset,
        int count,
        int startDoc,
        @Nullable boolean[] present
    );

    /** The first document at or after {@code from} that has a value, or {@link DocIdSetIterator#NO_MORE_DOCS}. */
    final int nextPresent(int from) throws IOException {
        int doc = from;
        while (doc < maxDoc) {
            ensure(doc);
            int found = current.nextPresent(doc);
            if (found != DocIdSetIterator.NO_MORE_DOCS) {
                return found;
            }
            doc = current.end();
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    /** The first file row of every row group, which maps a file row to the row group that holds it. */
    static final class FileLayout {

        private final long[] firstRows;
        private final long rowCount;

        FileLayout(ParquetMetadata footer) {
            firstRows = new long[footer.getBlocks().size()];
            long rows = 0;
            int i = 0;
            for (BlockMetaData block : footer.getBlocks()) {
                firstRows[i++] = rows;
                rows += block.getRowCount();
            }
            rowCount = rows;
        }

        long rowCount() {
            return rowCount;
        }

        /** The index of the row group that holds the file row; the row must be in the file. */
        int groupOf(long row) {
            int index = Arrays.binarySearch(firstRows, row);
            // a miss returns -(insertion point) - 1, and the row group is the one before the insertion point; row groups with no rows
            // share their first row with the next group, which the search can return for either of them, so move to the last such group
            int group = index >= 0 ? index : -index - 2;
            while (group + 1 < firstRows.length && firstRows[group + 1] == firstRows[group]) {
                group++;
            }
            return group;
        }

        /** The file row after the last row of the group. */
        long endRowOf(int group) {
            return group + 1 < firstRows.length ? firstRows[group + 1] : rowCount;
        }

        long firstRowOf(int group) {
            return firstRows[group];
        }
    }
}
