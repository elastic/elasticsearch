/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.core.CheckedRunnable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Checks the prefetch contract of a doc-values column on the data files {@code checked} accepts: a stage reads only what
 * an earlier stage prefetched, and reading the documents reads only what some stage prefetched. Nothing is prefetched
 * from any other file, such as metadata a store warms when the segment opens.
 */
public final class PrefetchRecordingDirectory extends FilterDirectory {

    /** Stage {@code stage} of a column's prefetch; whether the next one is needed. */
    @FunctionalInterface
    public interface Stage {
        boolean run(int stage) throws IOException;
    }

    private record Range(String file, long from, long to) {
        boolean covers(String file, long from, long to) {
            return this.file.equals(file) && this.from <= from && to <= this.to;
        }
    }

    private final Predicate<String> checked;
    private final List<Range> prefetched = new ArrayList<>();
    private List<Range> allowed;
    private final List<String> violations = new ArrayList<>();

    public PrefetchRecordingDirectory(Directory in, Predicate<String> checked) {
        super(in);
        this.checked = checked;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return new RecordingInput(name, super.openInput(name, context), 0);
    }

    /** Forgets what was prefetched, as a cold cache would. */
    public synchronized void reset() {
        prefetched.clear();
    }

    /**
     * Runs the stages of a column over a batch, then {@code read}, and fails if either read what no earlier stage asked
     * for since the last {@link #reset}.
     *
     * @return the stages run
     */
    public synchronized int assertReadsArePrefetched(Stage stages, CheckedRunnable<IOException> read) throws IOException {
        violations.clear();
        int stage = 0;
        boolean more = true;
        try {
            while (more) {
                allowed = List.copyOf(prefetched);
                more = stages.run(stage++);
            }
            allowed = List.copyOf(prefetched);
            read.run();
        } finally {
            allowed = null;
        }
        if (violations.isEmpty() == false) {
            fail(
                violations.size()
                    + " reads of bytes no earlier stage prefetched, first "
                    + violations.subList(0, Math.min(10, violations.size()))
                    + "; prefetched "
                    + prefetched.subList(0, Math.min(10, prefetched.size()))
            );
        }
        return stage;
    }

    /** Fails unless a stage prefetched something since the last {@link #reset}, and only from the checked files. */
    public synchronized void assertPrefetched() {
        assertTrue("nothing was prefetched", prefetched.isEmpty() == false);
        for (Range range : prefetched) {
            assertTrue("prefetched from an unchecked file: " + range, checked.test(range.file()));
        }
    }

    private synchronized void onPrefetch(String file, long from, long length) {
        prefetched.add(new Range(file, from, from + length));
    }

    private synchronized void onRead(String file, long from, long length) {
        if (allowed == null || length == 0 || checked.test(file) == false) {
            return;
        }
        long to = from + length;
        for (Range range : allowed) {
            if (range.covers(file, from, to)) {
                return;
            }
        }
        // A read may span neighbouring ranges that were asked for separately.
        long at = from;
        boolean progressed = true;
        while (at < to && progressed) {
            progressed = false;
            for (Range range : allowed) {
                if (range.file.equals(file) && range.from <= at && at < range.to) {
                    at = range.to;
                    progressed = true;
                }
            }
        }
        if (at < to) {
            violations.add(file + "[" + from + ", " + to + ")");
        }
    }

    /** Reports reads and prefetches in file coordinates. */
    private final class RecordingInput extends IndexInput {
        private final String file;
        private final IndexInput in;
        private final long base;

        RecordingInput(String file, IndexInput in, long base) {
            super("recording(" + in + ")");
            this.file = file;
            this.in = in;
            this.base = base;
        }

        @Override
        public byte readByte() throws IOException {
            onRead(file, base + in.getFilePointer(), 1);
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            onRead(file, base + in.getFilePointer(), len);
            in.readBytes(b, offset, len);
        }

        @Override
        public void prefetch(long offset, long length) throws IOException {
            onPrefetch(file, base + offset, length);
            in.prefetch(offset, length);
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new RecordingInput(file, in.slice(sliceDescription, offset, length), base + offset);
        }

        @Override
        public IndexInput clone() {
            return new RecordingInput(file, in.clone(), base);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
